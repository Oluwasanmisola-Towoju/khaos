package khaos

import "testing"

// findNode is a small test helper to locate a node by SeqID in an `all`
// slice, since Build guarantees `all[i].SeqID == i` but tests read better
// referencing SeqID explicitly.
func findNode(all []*DAGNode, seqID uint64) *DAGNode {
	for _, n := range all {
		if n.SeqID == seqID {
			return n
		}
	}
	return nil
}

// containsNode reports whether target appears in nodes.
func containsNode(nodes []*DAGNode, target *DAGNode) bool {
	for _, n := range nodes {
		if n == target {
			return true
		}
	}
	return false
}

func TestHazardResolver_RAW(t *testing.T) {
	// SeqID 0: Write "a"
	// SeqID 1: Read  "a"  -> must wait on SeqID 0 (RAW)
	batch := []Operation{
		{SeqID: 0, Key: "a", Type: OpWrite, Value: 1},
		{SeqID: 1, Key: "a", Type: OpRead},
	}

	r := NewSequentialHazardResolver()
	all, runnable, err := r.Build(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	n0, n1 := findNode(all, 0), findNode(all, 1)

	if got := n1.InDegree(); got != 1 {
		t.Fatalf("expected read (SeqID 1) to have inDegree 1, got %d", got)
	}
	if !containsNode(n0.Dependents(), n1) {
		t.Fatalf("expected write (SeqID 0) to list read (SeqID 1) as a dependent")
	}
	if len(runnable) != 1 || runnable[0] != n0 {
		t.Fatalf("expected only the write (SeqID 0) to be immediately runnable")
	}
}

func TestHazardResolver_WAR(t *testing.T) {
	// SeqID 0: Read  "a"
	// SeqID 1: Write "a"  -> must wait on SeqID 0 (WAR)
	batch := []Operation{
		{SeqID: 0, Key: "a", Type: OpRead},
		{SeqID: 1, Key: "a", Type: OpWrite, Value: 1},
	}

	r := NewSequentialHazardResolver()
	all, runnable, err := r.Build(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	n0, n1 := findNode(all, 0), findNode(all, 1)

	if got := n1.InDegree(); got != 1 {
		t.Fatalf("expected write (SeqID 1) to have inDegree 1, got %d", got)
	}
	if !containsNode(n0.Dependents(), n1) {
		t.Fatalf("expected read (SeqID 0) to list write (SeqID 1) as a dependent")
	}
	if len(runnable) != 1 || runnable[0] != n0 {
		t.Fatalf("expected only the read (SeqID 0) to be immediately runnable")
	}
}

func TestHazardResolver_WAW(t *testing.T) {
	// SeqID 0: Write "a"
	// SeqID 1: Write "a"  -> must wait on SeqID 0 (WAW)
	batch := []Operation{
		{SeqID: 0, Key: "a", Type: OpWrite, Value: 1},
		{SeqID: 1, Key: "a", Type: OpWrite, Value: 2},
	}

	r := NewSequentialHazardResolver()
	all, runnable, err := r.Build(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	n0, n1 := findNode(all, 0), findNode(all, 1)

	if got := n1.InDegree(); got != 1 {
		t.Fatalf("expected second write (SeqID 1) to have inDegree 1, got %d", got)
	}
	if !containsNode(n0.Dependents(), n1) {
		t.Fatalf("expected first write (SeqID 0) to list second write (SeqID 1) as a dependent")
	}
	if len(runnable) != 1 || runnable[0] != n0 {
		t.Fatalf("expected only the first write (SeqID 0) to be immediately runnable")
	}
}

func TestHazardResolver_DeleteBehavesAsWriteForHazards(t *testing.T) {
	// SeqID 0: Read   "a"
	// SeqID 1: Delete "a"  -> must wait on SeqID 0 (WAR against a delete)
	// SeqID 2: Write  "a"  -> must wait on SeqID 1 (WAW-equivalent)
	batch := []Operation{
		{SeqID: 0, Key: "a", Type: OpRead},
		{SeqID: 1, Key: "a", Type: OpDelete},
		{SeqID: 2, Key: "a", Type: OpWrite, Value: 1},
	}

	r := NewSequentialHazardResolver()
	all, runnable, err := r.Build(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	n0, n1, n2 := findNode(all, 0), findNode(all, 1), findNode(all, 2)

	if got := n1.InDegree(); got != 1 {
		t.Fatalf("expected delete (SeqID 1) to have inDegree 1, got %d", got)
	}
	if got := n2.InDegree(); got != 1 {
		t.Fatalf("expected write (SeqID 2) to have inDegree 1, got %d", got)
	}
	if !containsNode(n0.Dependents(), n1) {
		t.Fatalf("expected read (SeqID 0) to list delete (SeqID 1) as a dependent")
	}
	if !containsNode(n1.Dependents(), n2) {
		t.Fatalf("expected delete (SeqID 1) to list write (SeqID 2) as a dependent")
	}
	if len(runnable) != 1 || runnable[0] != n0 {
		t.Fatalf("expected only the read (SeqID 0) to be immediately runnable")
	}
}

func TestHazardResolver_IndependentKeysAreFullyParallel(t *testing.T) {
	// Three totally unrelated keys, mixed op types. None should depend on
	// any other — all should be immediately runnable.
	batch := []Operation{
		{SeqID: 0, Key: "a", Type: OpWrite, Value: 1},
		{SeqID: 1, Key: "b", Type: OpRead},
		{SeqID: 2, Key: "c", Type: OpDelete},
		{SeqID: 3, Key: "b", Type: OpWrite, Value: 2}, // depends on SeqID 1 (WAR)
	}

	r := NewSequentialHazardResolver()
	all, runnable, err := r.Build(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	n3 := findNode(all, 3)
	if got := n3.InDegree(); got != 1 {
		t.Fatalf("expected SeqID 3 to depend only on SeqID 1 (WAR), inDegree=%d", got)
	}

	if len(runnable) != 3 {
		t.Fatalf("expected exactly 3 immediately-runnable nodes (SeqIDs 0,1,2), got %d", len(runnable))
	}
	for _, seq := range []uint64{0, 1, 2} {
		if !containsNode(runnable, findNode(all, seq)) {
			t.Fatalf("expected SeqID %d to be immediately runnable", seq)
		}
	}
}

func TestHazardResolver_ReadFanOutFromSingleWrite(t *testing.T) {
	// SeqID 0: Write "a"
	// SeqID 1,2,3: Read "a" -- all three depend on SeqID 0, but not on
	// each other (concurrent reads are always safe).
	batch := []Operation{
		{SeqID: 0, Key: "a", Type: OpWrite, Value: 1},
		{SeqID: 1, Key: "a", Type: OpRead},
		{SeqID: 2, Key: "a", Type: OpRead},
		{SeqID: 3, Key: "a", Type: OpRead},
	}

	r := NewSequentialHazardResolver()
	all, runnable, err := r.Build(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	n0 := findNode(all, 0)
	if len(n0.Dependents()) != 3 {
		t.Fatalf("expected write to fan out to all 3 readers, got %d dependents", len(n0.Dependents()))
	}
	for _, seq := range []uint64{1, 2, 3} {
		n := findNode(all, seq)
		if got := n.InDegree(); got != 1 {
			t.Fatalf("expected reader SeqID %d to have inDegree 1, got %d", seq, got)
		}
	}
	if len(runnable) != 1 || runnable[0] != n0 {
		t.Fatalf("expected only the write to be immediately runnable")
	}
}

func TestHazardResolver_WriteFanInThenFanOut(t *testing.T) {
	// SeqID 0,1: Read "a"        (both runnable immediately)
	// SeqID 2:   Write "a"       (depends on both 0 and 1 -- WAR fan-in)
	// SeqID 3,4: Read "a"        (both depend on 2 only -- RAW fan-out)
	batch := []Operation{
		{SeqID: 0, Key: "a", Type: OpRead},
		{SeqID: 1, Key: "a", Type: OpRead},
		{SeqID: 2, Key: "a", Type: OpWrite, Value: 1},
		{SeqID: 3, Key: "a", Type: OpRead},
		{SeqID: 4, Key: "a", Type: OpRead},
	}

	r := NewSequentialHazardResolver()
	all, runnable, err := r.Build(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	n2 := findNode(all, 2)
	if got := n2.InDegree(); got != 2 {
		t.Fatalf("expected write (SeqID 2) to have inDegree 2 (fan-in from both reads), got %d", got)
	}
	for _, seq := range []uint64{3, 4} {
		n := findNode(all, seq)
		if got := n.InDegree(); got != 1 {
			t.Fatalf("expected reader SeqID %d to depend only on the write, inDegree=%d", seq, got)
		}
		if containsNode(findNode(all, 0).Dependents(), n) || containsNode(findNode(all, 1).Dependents(), n) {
			t.Fatalf("reader SeqID %d must NOT depend directly on the earlier reads (only on the intervening write)", seq)
		}
	}
	if len(runnable) != 2 {
		t.Fatalf("expected exactly 2 immediately-runnable nodes (the two initial reads), got %d", len(runnable))
	}
}

func TestHazardResolver_RejectsNonDenseSeqIDs(t *testing.T) {
	batch := []Operation{
		{SeqID: 0, Key: "a", Type: OpWrite},
		{SeqID: 5, Key: "b", Type: OpWrite}, // gap -- invalid
	}
	r := NewSequentialHazardResolver()
	if _, _, err := r.Build(batch); err == nil {
		t.Fatalf("expected an error for non-dense SeqIDs, got nil")
	}
}
