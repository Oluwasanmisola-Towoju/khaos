package khaos

import "fmt"

// SequentialHazardResolver builds the dependency DAG for a batch in a
// single O(N) pass, using per-key running state instead of comparing every
// operation pair (which would be O(N^2)).
//
// Algorithm:
//
// Walk the batch in SeqID order, maintaining two maps:
//
//	lastWrite map[key]  -> the most recent node that wrote (or deleted) key
//	lastReads map[key]  -> every node that has read key since lastWrite[key]
//	                        was last updated
//
// For each operation, before updating that state, wire edges against
// whatever is currently in it:
//
//	Read(key):
//	  if lastWrite[key] exists -> lastWrite[key].AddDependency(this)   // RAW
//	  append this to lastReads[key]
//
//	Write(key) / Delete(key):
//	  if lastWrite[key] exists -> lastWrite[key].AddDependency(this)   // WAW
//	  for each r in lastReads[key] -> r.AddDependency(this)            // WAR
//	  lastWrite[key] = this
//	  lastReads[key] = nil   // reset: WAR only cares about reads since
//	                         // the most recent write, and this write is
//	                         // now the most recent one
//
// Every operation does O(1) map lookups/updates plus work proportional to
// the number of edges it actually creates. Summed across the whole batch,
// total edge count is bounded by the batch itself (each node can be
// pointed at by at most the write/read history that precedes it), so the
// whole Build call is O(N) in the number of operations plus O(E) in the
// number of hazard edges produced — never O(N^2).
//
// Acyclicity: every edge added goes from a node with a strictly smaller
// SeqID (already seen) to the node currently being processed (the largest
// SeqID seen so far). An edge can therefore never point from a later
// SeqID to an earlier one, which makes a cycle structurally impossible.
// No runtime cycle detection is performed because none is needed.
type SequentialHazardResolver struct{}

// NewSequentialHazardResolver constructs a stateless HazardResolver. It
// holds no fields because all working state (lastWrite/lastReads) is
// local to a single Build call — the resolver itself is safe to reuse
// across batches and safe to share across goroutines building unrelated
// batches concurrently, since Build never mutates the resolver.
func NewSequentialHazardResolver() *SequentialHazardResolver {
	return &SequentialHazardResolver{}
}

// Build implements HazardResolver.
func (r *SequentialHazardResolver) Build(batch []Operation) ([]*DAGNode, []*DAGNode, error) {
	all := make([]*DAGNode, len(batch))

	lastWrite := make(map[string]*DAGNode, len(batch))
	lastReads := make(map[string][]*DAGNode, len(batch))

	for i, op := range batch {
		if int(op.SeqID) != i {
			return nil, nil, fmt.Errorf(
				"khaos: batch must be dense and zero-indexed by SeqID: "+
					"operation at position %d has SeqID %d", i, op.SeqID)
		}

		node := NewDAGNode(op)
		all[i] = node

		switch op.Type {
		case OpRead:
			if w, ok := lastWrite[op.Key]; ok {
				w.AddDependency(node) // RAW
			}
			lastReads[op.Key] = append(lastReads[op.Key], node)

		case OpWrite, OpDelete:
			if w, ok := lastWrite[op.Key]; ok {
				w.AddDependency(node) // WAW
			}
			for _, reader := range lastReads[op.Key] {
				reader.AddDependency(node) // WAR
			}
			lastWrite[op.Key] = node
			// Reset: WAR hazards only concern reads since the most
			// recent write, and this write just became the most recent
			// one. Setting to nil (rather than a fresh empty slice)
			// avoids an allocation for keys that aren't read again.
			lastReads[op.Key] = nil

		default:
			return nil, nil, fmt.Errorf(
				"khaos: unknown OperationType %d at SeqID %d", op.Type, op.SeqID)
		}
	}

	runnable := make([]*DAGNode, 0, len(all))
	for _, n := range all {
		if n.InDegree() == 0 {
			runnable = append(runnable, n)
		}
	}

	return all, runnable, nil
}

// compile-time interface satisfaction check.
var _ HazardResolver = (*SequentialHazardResolver)(nil)
