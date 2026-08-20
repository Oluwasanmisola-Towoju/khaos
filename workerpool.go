// workerpool.go
package khaos

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// ConcurrentWorkerPool implements WorkerPool. It runs a fixed number of
// worker goroutines pulling from a single shared dispatch channel, and
// commits every finished node to an injected ReorderBuffer.
//
// Constructor injection note: the WorkerPool interface's Run signature
// (fixed in Phase 1) takes ctx, storage, and initial — it deliberately
// says nothing about a ReorderBuffer, since Phase 1 was written before
// the ROB existed. Rather than break that interface now, the ROB is
// wired in at construction time via NewConcurrentWorkerPool. Run stays
// interface-compliant; the pool simply knows, for its own lifetime, where
// ConcurrentWorkerPool runs a fixed set of workers over a shared queue and
// commits completed nodes to its injected ReorderBuffer.
type ConcurrentWorkerPool struct {
	numWorkers int
	rob        ReorderBuffer
}

// NewConcurrentWorkerPool constructs a pool with numWorkers worker
// goroutines. If numWorkers <= 0, runtime.NumCPU() is used.
// NewConcurrentWorkerPool creates a pool. Non-positive worker counts use the
// number of available CPUs.
func NewConcurrentWorkerPool(rob ReorderBuffer, numWorkers int) *ConcurrentWorkerPool {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	return &ConcurrentWorkerPool{numWorkers: numWorkers, rob: rob}
}

// reachableNodes performs a single-threaded BFS over the DAG starting
// from roots, following Dependents() edges, and returns every distinct
// node reachable — which, given how HazardResolver.Build wires the graph,
// is every node in the batch (every node either has inDegree 0, making it
// a root, or has a predecessor, recursively grounding at some root).
//
// This traversal is what lets Run compute an exact upper bound on how
// many times a node will ever be sent into the dispatch channel over the
// life of the run — see the deadlock-prevention discussion on
// ConcurrentWorkerPool.Run for why that number matters.
// reachableNodes returns every distinct node reachable from the roots using
// a single-threaded BFS. Run uses the count to size its queue exactly.
func reachableNodes(roots []*DAGNode) []*DAGNode {
	visited := make(map[*DAGNode]bool, len(roots))
	queue := make([]*DAGNode, 0, len(roots))
	for _, n := range roots {
		if !visited[n] {
			visited[n] = true
			queue = append(queue, n)
		}
	}

	order := make([]*DAGNode, 0, len(roots))
	for i := 0; i < len(queue); i++ {
		n := queue[i]
		order = append(order, n)
		for _, dep := range n.Dependents() {
			if !visited[dep] {
				visited[dep] = true
				queue = append(queue, dep)
			}
		}
	}
	return order
}

// executeNode performs the actual storage operation for node, writing
// Result and Err exactly once. This happens strictly before Release is
// called on the node (see Run below), which is what gives dependents the
// happens-before guarantee documented on DAGNode.
// executeNode performs one storage operation and writes Result and Err before
// Release makes dependent nodes runnable.
func executeNode(ctx context.Context, storage StorageEngine, node *DAGNode) {
	switch node.Type {
	case OpRead:
		v, found, err := storage.Get(ctx, node.Key)
		if err != nil {
			node.Err = err
			return
		}
		if found {
			node.Result = v
		}
		// not found: Result stays nil, Err stays nil — a miss is not a
		// A missing key is a successful read with a nil result.

	case OpWrite:
		if err := storage.Set(ctx, node.Key, node.Value); err != nil {
			node.Err = err
			return
		}
		node.Result = node.Value

	case OpDelete:
		if err := storage.Delete(ctx, node.Key); err != nil {
			node.Err = err
		}

	default:
		node.Err = errUnknownOperationType
	}
}

var errUnknownOperationType = errUnknown("khaos: unknown OperationType encountered during execution")

type errUnknown string

func (e errUnknown) Error() string { return string(e) }

// Run implements WorkerPool.
//
// Deadlock-prevention strategy for the dynamic dispatch channel:
//
// The classic failure mode for a work-stealing pool where workers push
// new work back into their own input channel is a bounded-buffer
// deadlock: if the channel is undersized, every worker can end up
// blocked trying to push a newly-runnable dependent while every other
// worker is equally blocked, and nobody is left to drain the channel —
// total deadlock, or worse, a silent goroutine leak if ctx is never
// cancelled.
//
// Run avoids this entirely by sizing the channel exactly to the number
// of distinct nodes reachable in this batch (computed once, up front, via
// reachableNodes). This number is an exact bound — not a heuristic —
// because of an invariant guaranteed by construction: every node in the
// batch is sent into workCh exactly once over the life of the run. A node
// enters the channel either as part of `initial` (inDegree 0 at
// construction) or exactly once via Release() (the instant its inDegree
// transitions to 0). It is never re-queued, never duplicated — Release
// only returns a dependent in the slice for the single Add(-1) call that
// drives its counter to exactly zero. With capacity equal to the total
// node count, every send — whether the initial seeding or a worker's
// post-Release dispatch — is guaranteed to find free buffer space
// immediately. No send can ever block, so no worker can ever be stuck
// pushing while also being needed to pop. The structural precondition for
// this class of deadlock is removed, not merely made unlikely.
//
// Graceful shutdown: an atomic counter starts at the total node count and
// is decremented by exactly one each time a node is fully processed
// (executed, released, committed). The worker that drives it to zero is,
// by construction, the only one that can ever observe that exact
// transition — so it alone closes workCh, exactly once, with no
// coordination needed beyond the atomic decrement itself. Every worker's
// main loop selects between receiving from workCh and ctx.Done(), so:
//   - normal completion: workCh closes, every worker's receive returns
//     ok=false, every worker returns — no leaks.
//   - cancellation: any worker blocked in the select wakes on ctx.Done()
//     and returns; workers mid-execution finish their current
//     executeNode/Release/Commit and then observe ctx.Done() the next
//     time they hit the select. Either way, every goroutine has a
//     deterministic exit, satisfying the no-leak requirement.
//
// Run processes the initial DAG nodes with worker goroutines. The queue is
// sized to the exact reachable-node count: each node enters it once, either
// initially or when Release makes it runnable, so dispatch cannot deadlock.
// An atomic remaining count lets the last completed worker close the queue.
// Workers also select on ctx.Done(), giving both normal completion and
// cancellation a deterministic exit.
// The queue has capacity for every reachable node.
func (p *ConcurrentWorkerPool) Run(ctx context.Context, storage StorageEngine, initial []*DAGNode) error {
	all := reachableNodes(initial)
	total := len(all)
	if total == 0 {
		return nil
	}

	workCh := make(chan *DAGNode, total)
	for _, n := range initial {
		workCh <- n
	}

	var remaining int64 = int64(total)

	var wg sync.WaitGroup
	wg.Add(p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case node, ok := <-workCh:
					if !ok {
						return
					}

					executeNode(ctx, storage, node)
					newlyRunnable := node.Release()
					if p.rob != nil {
						p.rob.Commit(node)
					}

					for _, next := range newlyRunnable {
						// Never blocks: see capacity argument above.
						workCh <- next
					}

					if atomic.AddInt64(&remaining, -1) == 0 {
						close(workCh)
					}

				case <-ctx.Done():
					return
				}
			}
		}()
	}

	wg.Wait()
	return ctx.Err()
}

// compile-time interface satisfaction check.
// Compile-time interface check.
var _ WorkerPool = (*ConcurrentWorkerPool)(nil)
