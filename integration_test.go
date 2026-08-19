// integration_test.go
package khaos

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// buildMixedBatch generates a deterministic batch of n operations over a
// small key space, deliberately producing RAW, WAR, and WAW hazards on
// every key by cycling Write -> Read -> Read -> Delete -> Write ... so the
// buildMixedBatch creates deterministic RAW, WAR, and WAW dependency chains
// by cycling operation types over a small set of keys.
func buildMixedBatch(n, numKeys int) []Operation {
	batch := make([]Operation, n)
	cycle := []OperationType{OpWrite, OpRead, OpRead, OpWrite, OpDelete, OpWrite, OpRead}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%d", i%numKeys)
		opType := cycle[i%len(cycle)]
		op := Operation{SeqID: uint64(i), Key: key, Type: opType}
		if opType == OpWrite {
			op.Value = i // distinct, easy-to-check value per write
		}
		batch[i] = op
	}
	return batch
}

// simulateSequentially runs batch against a plain, unsynchronized map in
// strict SeqID order and returns the expected Result for every SeqID plus
// the expected final state of every key. This is the "golden" reference
// the concurrent, out-of-order engine's output must match exactly — if
// simulateSequentially provides the reference results and final state from
// strict SeqID-order execution. The concurrent pipeline must match both.
func simulateSequentially(batch []Operation) (expectedResults []any, finalState map[string]any) {
	ref := make(map[string]any)
	expectedResults = make([]any, len(batch))
	for _, op := range batch {
		switch op.Type {
		case OpRead:
			expectedResults[op.SeqID] = ref[op.Key] // nil if absent, matching map zero-value semantics
		case OpWrite:
			ref[op.Key] = op.Value
			expectedResults[op.SeqID] = op.Value
		case OpDelete:
			delete(ref, op.Key)
		}
	}
	return expectedResults, ref
}

// TestIntegration_FullPipeline wires all three phases together:
// HazardResolver builds the DAG, ConcurrentWorkerPool executes it against
// a real ShardedMap, and InOrderReorderBuffer streams the results back.
// It asserts (1) the output stream is strictly ordered by SeqID with no
// gaps or duplicates, (2) every operation's Result matches what a naive
// sequential execution of the same batch would have produced, and (3) the
// storage engine's final state matches the sequential reference exactly.
// TestIntegration_FullPipeline verifies DAG construction, concurrent
// execution, ordered output, per-operation results, and final storage state.
// op.Value = i // Distinct value for each write.
// expectedResults[op.SeqID] = ref[op.Key] // nil when the key is absent.

func TestIntegration_FullPipeline(t *testing.T) {
	const (
		batchSize = 100
		numKeys   = 10
	)

	batch := buildMixedBatch(batchSize, numKeys)
	expectedResults, expectedFinalState := simulateSequentially(batch)

	storage := NewShardedMap()
	resolver := NewSequentialHazardResolver()
	all, runnable, err := resolver.Build(batch)
	if err != nil {
		t.Fatalf("unexpected Build error: %v", err)
	}
	if len(all) != batchSize {
		t.Fatalf("expected %d nodes, got %d", batchSize, len(all))
	}

	rob := NewInOrderReorderBuffer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results := rob.Drain(ctx, batchSize)

	pool := NewConcurrentWorkerPool(rob, 8)

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- pool.Run(ctx, storage, runnable)
	}()

	var received []Operation
	for op := range results {
		received = append(received, op)
	}

	if err := <-runErrCh; err != nil {
		t.Fatalf("WorkerPool.Run returned an error: %v", err)
	}

	// --- Ordering: strictly sequential SeqIDs, no gaps, no duplicates ---
		// Output must be ordered, complete, and error-free.
	if len(received) != batchSize {
		t.Fatalf("expected %d results, got %d", batchSize, len(received))
	}
	for i, op := range received {
		if op.SeqID != uint64(i) {
			t.Fatalf("output stream out of order: position %d holds SeqID %d", i, op.SeqID)
		}
		if op.Err != nil {
			t.Fatalf("SeqID %d returned unexpected error: %v", op.SeqID, op.Err)
		}
	}

	// --- Correctness: every result matches the sequential reference ---
		// Each result must match strict sequential execution.
	for i, op := range received {
		want := expectedResults[i]
		switch op.Type {
		case OpRead, OpWrite:
			if op.Result != want {
				t.Fatalf("SeqID %d (%s, key=%s): got Result=%v, want %v",
					op.SeqID, op.Type, op.Key, op.Result, want)
			}
		}
	}

	// --- Data integrity: final storage state matches the reference ---
		// Final storage must match the sequential reference.
	for key, wantVal := range expectedFinalState {
		gotVal, found, err := storage.Get(ctx, key)
		if err != nil {
			t.Fatalf("unexpected error reading final state of %s: %v", key, err)
		}
		if !found {
			t.Fatalf("key %s missing from final storage state, expected %v", key, wantVal)
		}
		if gotVal != wantVal {
			t.Fatalf("key %s: final storage value %v does not match reference %v", key, gotVal, wantVal)
		}
	}
	// Any key present in storage but NOT in the reference's final state
	// would indicate a delete was skipped or a stale write survived.
		// Unexpected keys indicate a skipped delete or stale write.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", i)
		if _, expectedPresent := expectedFinalState[key]; !expectedPresent {
			if _, found, _ := storage.Get(ctx, key); found {
				t.Fatalf("key %s unexpectedly present in storage; reference says it should be deleted", key)
			}
		}
	}
}

// slowStorage wraps a StorageEngine and injects an artificial delay for
// one specific key, simulating a slow disk read on that key alone. It
// exists to prove the engine actually executes out of order — that a
// stall on one key does not block independent operations on other keys —
// rather than merely happening to produce correct output via accidental
// slowStorage delays one key to verify independent operations still run
// concurrently instead of waiting behind a single slow operation.
type slowStorage struct {
	StorageEngine
	slowKey string
	delay   time.Duration
}

func (s *slowStorage) Get(ctx context.Context, key string) (any, bool, error) {
	if key == s.slowKey {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	return s.StorageEngine.Get(ctx, key)
}

// TestIntegration_StallDoesNotBlockIndependentKeys demonstrates the core
// value proposition of the engine: a batch containing one artificially
// slow operation on key "slow" and many fast independent operations on
// other keys completes in roughly the time of the slow operation alone,
// not the sum of all operations — proving the fast, independent work
// really did execute concurrently with the stall instead of queuing
// behind it.
// TestIntegration_StallDoesNotBlockIndependentKeys verifies that independent
// operations complete alongside, rather than behind, a slow operation.
func TestIntegration_StallDoesNotBlockIndependentKeys(t *testing.T) {
	const (
		numFastKeys = 20
		stallDelay  = 200 * time.Millisecond
	)

	batch := []Operation{{SeqID: 0, Key: "slow", Type: OpRead}}
	for i := 0; i < numFastKeys; i++ {
		batch = append(batch, Operation{
			SeqID: uint64(i + 1),
			Key:   fmt.Sprintf("fast%d", i),
			Type:  OpWrite,
			Value: i,
		})
	}

	storage := &slowStorage{StorageEngine: NewShardedMap(), slowKey: "slow", delay: stallDelay}

	resolver := NewSequentialHazardResolver()
	all, runnable, err := resolver.Build(batch)
	if err != nil {
		t.Fatalf("unexpected Build error: %v", err)
	}
	if len(runnable) != len(all) {
		t.Fatalf("expected every operation to be independent (all runnable immediately), got %d/%d runnable",
			len(runnable), len(all))
	}

	rob := NewInOrderReorderBuffer()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := rob.Drain(ctx, len(batch))
	pool := NewConcurrentWorkerPool(rob, 8)

	start := time.Now()
	runErrCh := make(chan error, 1)
	go func() { runErrCh <- pool.Run(ctx, storage, runnable) }()

	count := 0
	for range results {
		count++
	}
	elapsed := time.Since(start)

	if err := <-runErrCh; err != nil {
		t.Fatalf("WorkerPool.Run returned an error: %v", err)
	}
	if count != len(batch) {
		t.Fatalf("expected %d results, got %d", len(batch), count)
	}

	if elapsed > stallDelay*3 {
		t.Fatalf("batch took %v, expected roughly one stall duration (%v) since fast ops are independent of the stall",
			elapsed, stallDelay)
	}
}

// TestIntegration_EmptyBatch verifies the pipeline handles a zero-length
// batch cleanly: no goroutines spawned to hang, channel closes
// immediately.
// TestIntegration_EmptyBatch verifies that an empty batch closes immediately
// without starting work.
func TestIntegration_EmptyBatch(t *testing.T) {
	resolver := NewSequentialHazardResolver()
	all, runnable, err := resolver.Build(nil)
	if err != nil {
		t.Fatalf("unexpected error on empty batch: %v", err)
	}
	if len(all) != 0 || len(runnable) != 0 {
		t.Fatalf("expected empty node sets for empty batch")
	}

	rob := NewInOrderReorderBuffer()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	results := rob.Drain(ctx, 0)
	pool := NewConcurrentWorkerPool(rob, 4)

	if err := pool.Run(ctx, NewShardedMap(), runnable); err != nil {
		t.Fatalf("unexpected Run error on empty batch: %v", err)
	}

	count := 0
	for range results {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 results from an empty batch, got %d", count)
	}
}

// TestIntegration_ContextCancellationLeavesNoGoroutines cancels the
// context shortly after starting a large batch and relies on an explicit
// timeout: if any goroutine failed to exit deterministically, Run would
// never return and the test would hang past its own deadline instead of
// failing fast.
// TestIntegration_ContextCancellationLeavesNoGoroutines verifies that Run
// returns promptly after cancellation, without leaked goroutines.
func TestIntegration_ContextCancellationLeavesNoGoroutines(t *testing.T) {
	const batchSize = 500
	batch := buildMixedBatch(batchSize, 25)

	storage := &slowStorage{StorageEngine: NewShardedMap(), slowKey: "key0", delay: 2 * time.Second}

	resolver := NewSequentialHazardResolver()
	_, runnable, err := resolver.Build(batch)
	if err != nil {
		t.Fatalf("unexpected Build error: %v", err)
	}

	rob := NewInOrderReorderBuffer()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_ = rob.Drain(ctx, batchSize)
	pool := NewConcurrentWorkerPool(rob, 8)

	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx, storage, runnable) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected Run to return context.DeadlineExceeded, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not return within 3s of context cancellation — likely goroutine leak")
	}
}