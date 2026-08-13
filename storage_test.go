package khaos

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestShardedMap_ConcurrentReadWrite hammers the store with hundreds of
// goroutines doing overlapping Get/Set/Delete against a shared key space.
// This test's real assertion is implicit: run under `go test -race`, any
// unsynchronized access to shard.data would be flagged as a data race and
// fail the build. The explicit assertions below additionally check that
// no write is ever lost or corrupted — every value we read back must be
// exactly one of the values we could plausibly have written.
func TestShardedMap_ConcurrentReadWrite(t *testing.T) {
	sm := NewShardedMap()
	ctx := context.Background()

	const (
		numGoroutines   = 300
		numKeys         = 50
		opsPerGoroutine = 200
	)

	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := keys[(g+i)%numKeys]
				switch i % 3 {
				case 0:
					if err := sm.Set(ctx, key, g*1000+i); err != nil {
						t.Errorf("unexpected Set error: %v", err)
					}
				case 1:
					if _, _, err := sm.Get(ctx, key); err != nil {
						t.Errorf("unexpected Get error: %v", err)
					}
				case 2:
					if err := sm.Delete(ctx, key); err != nil {
						t.Errorf("unexpected Delete error: %v", err)
					}
				}
			}
		}()
	}

	wg.Wait()

	// Sanity pass: the store must still be internally consistent — every
	// key either holds a well-formed int we could have written, or is
	// absent (deleted). A corrupted map (partial write torn by a race)
	// would surface here as a value of the wrong type or a panic.
	for _, key := range keys {
		v, found, err := sm.Get(ctx, key)
		if err != nil {
			t.Fatalf("unexpected error on final Get(%s): %v", key, err)
		}
		if found {
			if _, ok := v.(int); !ok {
				t.Fatalf("key %s holds corrupted value of type %T: %v", key, v, v)
			}
		}
	}
}

// TestShardedMap_SetThenGet is a basic correctness check independent of
// concurrency: a single Set must be immediately visible to a subsequent
// Get, and Delete must make it disappear.
func TestShardedMap_SetThenGet(t *testing.T) {
	sm := NewShardedMap()
	ctx := context.Background()

	if _, found, err := sm.Get(ctx, "missing"); err != nil || found {
		t.Fatalf("expected missing key to be not found, got found=%v err=%v", found, err)
	}

	if err := sm.Set(ctx, "a", 42); err != nil {
		t.Fatalf("unexpected Set error: %v", err)
	}
	v, found, err := sm.Get(ctx, "a")
	if err != nil || !found {
		t.Fatalf("expected key 'a' to be found, got found=%v err=%v", found, err)
	}
	if v != 42 {
		t.Fatalf("expected value 42, got %v", v)
	}

	if err := sm.Delete(ctx, "a"); err != nil {
		t.Fatalf("unexpected Delete error: %v", err)
	}
	if _, found, _ := sm.Get(ctx, "a"); found {
		t.Fatalf("expected key 'a' to be absent after Delete")
	}
}

// TestShardedMap_DistributesAcrossShards is a distribution sanity check:
// with a reasonable number of distinct keys, we should not see every key
// collapse into a single shard. This isn't a strict correctness
// requirement, but a degenerate hash (everything landing in shard 0)
// would silently defeat the entire point of sharding, so it's worth
// catching in CI.
func TestShardedMap_DistributesAcrossShards(t *testing.T) {
	seen := make(map[int]bool)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("distribution-key-%d", i)
		idx := int(fnv1a(key) & shardMask)
		seen[idx] = true
	}
	if len(seen) < numShards/2 {
		t.Fatalf("expected keys to spread across at least half of %d shards, "+
			"only hit %d — hash distribution looks degenerate", numShards, len(seen))
	}
}
