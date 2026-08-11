package khaos

import (
	"context"
	"sync"
)

// numShards is the fixed shard count for ShardedMap. 256 is chosen as a
// power of two so shard selection is a single AND (shardMask), not a
// division/modulo, and is large enough that on typical multi-core hosts
// (8-64 cores) the odds of two concurrently-hot keys landing in the same
// shard stay low without wasting memory on per-shard overhead.
const numShards = 256

// shardMask relies on numShards being a power of two: hash & shardMask is
// equivalent to hash % numShards but avoids integer division on the hot
// path of every Get/Set/Delete call.
const shardMask = numShards - 1

// fnv1aOffset32 and fnv1aPrime32 are the standard FNV-1a 32-bit constants.
// We hand-roll the hash instead of using hash/fnv from the standard
// library because hash/fnv's Write-based API allocates a hasher and
// forces an interface call per byte; a tight inline loop over the key's
// bytes is both allocation-free and inlinable by the compiler.
const (
	fnv1aOffset32 uint32 = 2166136261
	fnv1aPrime32  uint32 = 16777619
)

// fnv1a computes the 32-bit FNV-1a hash of key. It is not
// cryptographically secure and is not meant to be — it exists purely to
// spread keys evenly across shards as cheaply as possible.
func fnv1a(key string) uint32 {
	h := fnv1aOffset32
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= fnv1aPrime32
	}
	return h
}

// shard is one partition of the ShardedMap: an independent map guarded by
// its own RWMutex. Because each shard has its own lock, a write to a key
// in shard 3 never blocks a read of a key in shard 200 — this is what
// gives ShardedMap its scalability over a single global-lock map.
type shard struct {
	mu   sync.RWMutex
	data map[string]any
}

// ShardedMap is a concurrent, in-memory key-value store partitioned into
// numShards independent shards, each protected by its own sync.RWMutex.
// It implements StorageEngine.
//
// Concurrency model: every method locks exactly one shard for the
// duration of its critical section (a single map read or write) and
// nothing else. RWMutex is chosen over a plain Mutex because reads
// (OpRead, the dominant traffic in most workloads) can proceed
// concurrently with each other within a shard; only writes need exclusive
// access. Critical sections are kept to the single map operation —
// hashing happens before the lock is taken, so lock hold time is minimal.
type ShardedMap struct {
	shards [numShards]*shard
}

// NewShardedMap constructs a ShardedMap with all shards pre-allocated and
// ready for concurrent use.
func NewShardedMap() *ShardedMap {
	sm := &ShardedMap{}
	for i := range sm.shards {
		sm.shards[i] = &shard{data: make(map[string]any)}
	}
	return sm
}

// shardFor returns the shard responsible for key. Pure function of the
// key's hash — no locking involved in the routing decision itself.
func (sm *ShardedMap) shardFor(key string) *shard {
	return sm.shards[fnv1a(key)&shardMask]
}

// Get returns the value stored at key, and whether it was found. Safe to
// call concurrently with any number of other Get calls and with Set/Delete
// calls on different keys without blocking; Set/Delete calls on the same
// key are serialized by that key's shard lock.
func (sm *ShardedMap) Get(ctx context.Context, key string) (any, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s := sm.shardFor(key)
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok, nil
}

// Set stores value at key, overwriting any existing value.
func (sm *ShardedMap) Set(ctx context.Context, key string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := sm.shardFor(key)
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
	return nil
}

// Delete removes key from the store. Deleting a key that does not exist
// is not an error — it is a no-op, matching the semantics of Go's builtin
// delete on maps.
func (sm *ShardedMap) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := sm.shardFor(key)
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
	return nil
}

// compile-time interface satisfaction check.
var _ StorageEngine = (*ShardedMap)(nil)
