package khaos

import "context"

// StorageEngine is the thread-safe key-value backend underlying the
// engine. Phase 2 will implement this as a sharded hash map
// (hash(key) -> shard, one sync.RWMutex per shard). Callers of
// StorageEngine must be able to invoke Get/Set/Delete concurrently from
// multiple goroutines without holding any external lock — all
// synchronization is internal to the implementation.
type StorageEngine interface {
	Get(ctx context.Context, key string) (value any, found bool, err error)
	Set(ctx context.Context, key string, value any) error
	Delete(ctx context.Context, key string) error
}

// HazardResolver builds the dependency DAG for a batch of operations. It
// detects the three classical data hazards between operations that target
// the same Key:
//
//   - RAW (Read-After-Write):  a read must wait for an earlier write to
//     the same key to commit.
//   - WAR (Write-After-Read):  a write must wait for earlier reads of the
//     same key to commit, so those reads observe the pre-write value.
//   - WAW (Write-After-Write): a write must wait for an earlier write to
//     the same key to commit, so the later write wins deterministically.
//
// Build is single-threaded by contract: implementations must not spawn
// worker goroutines, and must fully finish wiring every DAGNode's
// dependents/inDegree before returning. This is what lets DAGNode.dependents
// be read without synchronization once execution starts (see DAGNode
// doc comment) — Build's return is the happens-before boundary.
//
// Build returns the full node set (ordered by SeqID, for the
// ReorderBuffer to size and index itself) and the subset with inDegree 0
// at construction time (immediately dispatchable to the WorkerPool).
type HazardResolver interface {
	Build(batch []Operation) (all []*DAGNode, runnable []*DAGNode, err error)
}

// WorkerPool executes runnable DAGNodes concurrently and drives the
// dependency graph forward: as each node finishes, the pool writes its
// Result/Err, calls Release on it, and dispatches any newly-runnable
// dependents to the next available worker.
//
// Run blocks until every node reachable from initial has committed, or
// until ctx is cancelled. On cancellation, Run must return promptly
// (bounded by the granularity of the work being executed) and must leave
// no goroutines running behind it — every goroutine started inside Run
// must have a deterministic exit via ctx.Done() or a closed channel.
type WorkerPool interface {
	Run(ctx context.Context, storage StorageEngine, initial []*DAGNode) error
}

// ReorderBuffer accepts completed DAGNodes in arbitrary execution order
// and releases their results to the caller strictly in SeqID order — a
// result for SeqID N is only released once SeqIDs 0..N-1 have all been
// released. This is the software equivalent of a CPU's reorder buffer:
// it hides the out-of-order execution entirely, so the embedding
// application always observes results in the order it submitted them.
type ReorderBuffer interface {
	// Commit is called by the WorkerPool once a node's Result/Err are
	// final (i.e. after Release has fired for that node). Commit may be
	// called concurrently by multiple workers for different nodes and
	// must be safe for that.
	Commit(node *DAGNode)

	// Drain returns a channel that yields committed Operations strictly
	// in SeqID order, starting from SeqID 0. The channel is closed once
	// `total` operations have been drained, or immediately if ctx is
	// cancelled first.
	Drain(ctx context.Context, total int) <-chan Operation
}
