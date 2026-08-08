package khaos

import "sync/atomic"

// OperationType identifies the class of data operation being requested.
type OperationType uint8

const (
	OpRead OperationType = iota
	OpWrite
	OpDelete
)

func (t OperationType) String() string {
	switch t {
	case OpRead:
		return "READ"
	case OpWrite:
		return "WRITE"
	case OpDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// ExecutionState tracks the lifecycle of an Operation as it moves through
// the engine. Transitions are monotonic:
//
//	Pending -> Blocked -> Runnable -> Executing -> Committed
//	                                            \-> Failed
//
// State is stored on DAGNode as an atomic value for observability
// (metrics, tracing, debugging) ONLY. It is never read by scheduling
// logic — the authoritative scheduling signals are DAGNode.inDegree
// reaching zero and DAGNode.done closing. Treating state as anything more
// than a label would reintroduce exactly the kind of shared mutable state
// this engine is designed to avoid.
type ExecutionState uint32

const (
	StatePending ExecutionState = iota
	StateBlocked
	StateRunnable
	StateExecuting
	StateCommitted
	StateFailed
)

func (s ExecutionState) String() string {
	switch s {
	case StatePending:
		return "PENDING"
	case StateBlocked:
		return "BLOCKED"
	case StateRunnable:
		return "RUNNABLE"
	case StateExecuting:
		return "EXECUTING"
	case StateCommitted:
		return "COMMITTED"
	case StateFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// Operation represents a single unit of work submitted to the engine as
// part of a batch. It is a native Go value — the engine accepts and
// returns structs directly, never serialized strings, so there is no
// parsing layer anywhere in this design.
//
// Operation is logically immutable from the caller's perspective after
// submission, with two narrow, well-defined exceptions: Result and Err,
// each written exactly once by the worker that executes this operation.
// See DAGNode.Release for the happens-before guarantee that makes reading
// Result/Err from other goroutines race-free without a mutex.
type Operation struct {
	// SeqID is the caller-assigned position of this operation within its
	// batch. SeqIDs must be dense and zero-indexed (0..N-1) within a
	// single batch. The ReorderBuffer uses SeqID — never completion order
	// — to decide when a result may be released to the caller.
	SeqID uint64

	// Key is the storage key this operation touches. Hazard detection is
	// performed per Key: two operations receive a dependency edge only if
	// they touch the same Key and at least one of them is a write
	// (OpWrite or OpDelete).
	Key string

	// Type identifies whether this is a Read, Write, or Delete.
	Type OperationType

	// Value is the payload for Write operations. It is nil for Read and
	// Delete. It is a native Go value (typically a struct, pointer, or
	// primitive) supplied directly by the caller — the engine performs no
	// serialization or parsing on it.
	Value any

	// Result is populated by the worker that executes this operation. It
	// holds the read value for OpRead, or a confirmation payload for
	// OpWrite/OpDelete, depending on the StorageEngine implementation.
	// Written exactly once. Must not be read until DAGNode.Done() has
	// fired for this operation's node.
	Result any

	// Err is populated exactly once, under the same happens-before
	// guarantee as Result. A non-nil Err marks the operation (and, per
	// the DAG hazard rules, its dependents) as failed.
	Err error
}

// DAGNode wraps an Operation with the bookkeeping required to schedule it
// inside the batch's dependency graph. Exactly one DAGNode is created per
// Operation for the lifetime of a single batch execution. DAGNodes are
// never reused or shared across batches — a fresh DAG is built per batch.
//
// Memory layout note: DAGNode embeds Operation by value (not pointer).
// DAGNodes themselves are always handled by pointer (*DAGNode) once
// constructed, so this embedding avoids a second allocation and keeps the
// Operation's fields co-located in memory with the scheduling metadata
// that governs their execution — one cache-friendly block per unit of
// work.
type DAGNode struct {
	Operation

	// inDegree is the number of unresolved hazard edges pointing INTO
	// this node — i.e. the count of predecessor operations that must
	// commit before this node may run. It is manipulated exclusively via
	// sync/atomic. During execution the only operation performed on it is
	// "decrement by one and check whether the result is exactly zero",
	// which maps directly onto atomic.Int64.Add and requires no mutex.
	// See the Phase 1 concurrency analysis for why concurrent decrements
	// from multiple worker goroutines cannot double-fire or drop a
	// transition to runnable.
	inDegree atomic.Int64

	// dependents is the fan-out list: nodes that depend on THIS node
	// committing. It is built once, single-threaded, during
	// HazardResolver.Build — before any worker goroutine starts touching
	// the DAG. From that point until the batch finishes, dependents is
	// read-only. Because DAG construction fully happens-before execution
	// (the resolver returns the built graph to its caller before any
	// worker is spawned), no synchronization is required to read
	// dependents concurrently during execution — there is no writer to
	// race against.
	dependents []*DAGNode

	// done is closed exactly once, by the worker that executes this node,
	// strictly after Operation.Result/Err have been written. Closing a
	// channel is the synchronization primitive that establishes the
	// happens-before edge between "this node's result is ready" and "a
	// dependent may safely read it". Goroutines that need to wait on a
	// predecessor select on Done() rather than polling State().
	done chan struct{}

	// state is an atomic observability label only. See ExecutionState.
	state atomic.Uint32
}

// NewDAGNode constructs a DAGNode wrapping op. The returned node has
// inDegree 0 and no dependents; HazardResolver.Build is responsible for
// wiring edges via AddDependency during graph construction.
func NewDAGNode(op Operation) *DAGNode {
	n := &DAGNode{
		Operation: op,
		done:      make(chan struct{}),
	}
	n.state.Store(uint32(StatePending))
	return n
}

// AddDependency records that `dependent` must wait for `n` to commit
// before it may run: it appends `dependent` to n's fan-out list and
// increments dependent's in-degree by one.
//
// AddDependency is called only by HazardResolver.Build during the
// single-threaded graph-construction phase — never concurrently with
// itself and never after execution has started. The in-degree increment
// still goes through atomic.Int64.Add (rather than a plain field write)
// so the type remains safe even if a future revision parallelizes graph
// construction; it costs nothing today and removes a footgun later.
func (n *DAGNode) AddDependency(dependent *DAGNode) {
	n.dependents = append(n.dependents, dependent)
	dependent.inDegree.Add(1)
}

// Dependents returns n's fan-out list. Safe to call concurrently once
// construction has finished, per the happens-before argument on the
// dependents field above.
func (n *DAGNode) Dependents() []*DAGNode {
	return n.dependents
}

// InDegree returns the current number of unresolved predecessors.
func (n *DAGNode) InDegree() int64 {
	return n.inDegree.Load()
}

// Release is called exactly once by the worker that finishes executing n,
// strictly after that worker has written n.Result and n.Err. It decrements
// the in-degree of every dependent and returns the subset that became
// runnable (in-degree transitioned to exactly zero) as a result of this
// call, then closes n.done to publish n's Result/Err.
//
// Ordering within this method is deliberate: dependents are only
// released for consumption by the caller after Result/Err are already
// final (enforced by the WorkerPool's call sequence in Phase 2 — Release
// is invoked only after the write completes), and done is closed last so
// that "<-n.done has fired" reliably implies "Result and Err are safe to
// read from any goroutine".
func (n *DAGNode) Release() []*DAGNode {
	var runnable []*DAGNode
	for _, dep := range n.dependents {
		if dep.inDegree.Add(-1) == 0 {
			runnable = append(runnable, dep)
		}
	}
	close(n.done)
	return runnable
}

// Done returns a channel that is closed once this node has committed its
// Result/Err. Safe to call and select on from any number of goroutines,
// any number of times.
func (n *DAGNode) Done() <-chan struct{} {
	return n.done
}

// State returns the current observability state of the node. Not
// authoritative for scheduling decisions — see ExecutionState.
func (n *DAGNode) State() ExecutionState {
	return ExecutionState(n.state.Load())
}

// SetState atomically updates the observability label.
func (n *DAGNode) SetState(s ExecutionState) {
	n.state.Store(uint32(s))
}
