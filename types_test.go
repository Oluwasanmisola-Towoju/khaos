package khaos

import (
	"sync"
	"testing"
)

// TestDAGNode_ReleaseFanOut verifies that a dependent with multiple
// predecessors becomes runnable exactly once — precisely when the LAST
// predecessor releases — even when all predecessors call Release
// concurrently from different goroutines. This is the core correctness
// property the whole scheduler depends on: no double-dispatch, no
// dropped dispatch.
func TestDAGNode_ReleaseFanOut(t *testing.T) {
	const numPredecessors = 64

	dependent := NewDAGNode(Operation{SeqID: 999, Key: "k"})
	preds := make([]*DAGNode, numPredecessors)
	for i := range preds {
		preds[i] = NewDAGNode(Operation{SeqID: uint64(i), Key: "k"})
		preds[i].AddDependency(dependent)
	}

	if got := dependent.InDegree(); got != numPredecessors {
		t.Fatalf("expected inDegree %d, got %d", numPredecessors, got)
	}

	var runnableCount atomicCounter
	var wg sync.WaitGroup
	wg.Add(numPredecessors)

	for _, p := range preds {
		p := p
		go func() {
			defer wg.Done()
			p.Result = "ok"
			runnable := p.Release()
			for range runnable {
				runnableCount.inc()
			}
		}()
	}
	wg.Wait()

	if n := runnableCount.get(); n != 1 {
		t.Fatalf("expected dependent to become runnable exactly once, got %d releases", n)
	}
	if got := dependent.InDegree(); got != 0 {
		t.Fatalf("expected final inDegree 0, got %d", got)
	}
}

// TestDAGNode_DoneHappensBeforeResult verifies the happens-before
// guarantee: by the time Done() fires, Result written before Release is
// visible to the goroutine that observed the close. go test -race is what
// actually proves this — this test just exercises the pattern under
// concurrent readers.
func TestDAGNode_DoneHappensBeforeResult(t *testing.T) {
	n := NewDAGNode(Operation{SeqID: 1, Key: "k"})

	var wg sync.WaitGroup
	const numReaders = 32
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			<-n.Done()
			if n.Result != "final-value" {
				t.Errorf("reader observed incomplete write after Done(): got %v", n.Result)
			}
		}()
	}

	n.Result = "final-value"
	n.Release()

	wg.Wait()
}

// TestDAGNode_NoDependencyRunsImmediately verifies that a node with zero
// predecessors reports inDegree 0 without any Release call, matching the
// HazardResolver contract that such nodes are returned in `runnable`
// directly from Build.
func TestDAGNode_NoDependencyRunsImmediately(t *testing.T) {
	n := NewDAGNode(Operation{SeqID: 0, Key: "k"})
	if got := n.InDegree(); got != 0 {
		t.Fatalf("expected fresh node to have inDegree 0, got %d", got)
	}
}

// atomicCounter is a tiny race-free counter used only by tests in this
// file, kept local so Phase 1 introduces no new exported surface beyond
// what the spec asked for.
type atomicCounter struct {
	mu sync.Mutex
	n  int
}

func (c *atomicCounter) inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *atomicCounter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}
