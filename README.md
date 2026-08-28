# README.md

# Khaos

**Khaos** is an embedded, out-of-order (OoO) execution engine for key-value workloads, written in Go with zero external dependencies. It applies the same architectural idea that lets modern CPUs run faster than the raw speed of a single instruction — pipelining, hazard detection, and reordered-but-safe execution to a batch of data operations, so that slow, blocking work never stalls the operations around it.

It is a library, not a server. You import it directly into your application, the same way you'd import SQLite or an in-process cache. There is no network hop, no serialization protocol, and no query language to parse — Khaos accepts native Go structs and executes them.

---

## The Problem

A traditional sequential processor — whether that's a CPU executing instructions one at a time, or an application executing a batch of database operations one at a time — pays the full cost of every stall, even when nothing downstream actually depends on the thing it's stalled on.

Concretely: if you hand a batch of 50 operations to a naive in-order executor, and operation #12 has to wait 200ms for a slow disk read, operations #13 through #50 sit idle behind it — even if every one of them touches a completely different piece of data and has no logical reason to wait. The CPU or the goroutine driving that batch is burning wall-clock time on nothing.

This is exactly the problem hardware engineers solved decades ago with out-of-order execution: look ahead at the instruction stream, find work that doesn't depend on the thing that's stalled, and run it now instead of leaving the pipeline idle. Khaos brings that same idea to software batch processing.

## The Solution

Khaos is built from four components that work together as a pipeline:

**1. Sharded Storage (`ShardedMap`)**
The underlying key-value store, partitioned into 256 independent shards, each with its own lock. Two operations touching different shards never contend with each other at all — this is what makes true concurrent execution possible in the first place. Keys are routed to shards with a fast, non-cryptographic FNV-1a hash.

**2. DAG Hazard Resolver (`SequentialHazardResolver`)**
Before anything executes, every operation in the batch is analyzed for data hazards against every other operation on the same key:
- **RAW** (Read-After-Write): a read must wait for an earlier write to the same key.
- **WAR** (Write-After-Read): a write must wait for earlier reads of the same key, so those reads see the old value first.
- **WAW** (Write-After-Write): a write must wait for an earlier write to the same key, so writes apply in the right order.

The resolver builds this dependency graph in a single O(N) pass — no comparing every operation against every other operation — and produces a Directed Acyclic Graph (DAG) where an operation with zero unresolved dependencies is immediately safe to run.

**3. Async Worker Pool (`ConcurrentWorkerPool`)**
A fixed pool of goroutines pulls runnable operations off a shared dispatch channel, executes them against storage, and — critically — pushes newly-unblocked operations back onto that same channel the instant their last dependency clears. This is the mechanism that keeps the CPU busy: a stall on one key never blocks independent work on other keys.

**4. Reorder Buffer (`InOrderReorderBuffer`)**
Operations finish in whatever order the scheduler happens to complete them — not the order they were submitted in. The Reorder Buffer holds finished results and releases them to the caller strictly in original submission order, so from the outside, the chaos of out-of-order execution is completely invisible. You get exactly the sequence you asked for, just faster.

[ Batch of Operations ]
│
▼
┌───────────────────────┐
│ Hazard Resolver (DAG) │ → detects RAW / WAR / WAW hazards, O(N)
└───────────────────────┘
│
▼
┌───────────────────────┐
│ Worker Pool │ → executes unblocked ops concurrently
│ (Sharded Storage) │ against the sharded store
└───────────────────────┘
│
▼
┌───────────────────────┐
│ Reorder Buffer │ → re-sequences results by SeqID
└───────────────────────┘
│
▼
[ Ordered Results Stream ]


## Concurrency Highlights

- **Lock-free dependency tracking.** Each DAG node's in-degree counter is a `sync/atomic` integer, not a mutex-protected field. Decrementing it and checking whether it hit zero is a single atomic operation — with dozens of workers finishing predecessors simultaneously, exactly one worker will ever observe a dependent transition to runnable, with no lock required.
- **O(N) DAG construction.** The hazard resolver never compares operation pairs. It walks the batch once, keeping a running "last writer" and "readers since last write" per key, wiring edges as it goes. Total work is proportional to the batch size plus the number of real hazards found — never quadratic.
- **Deadlock-free dynamic dispatch.** Workers push newly-runnable operations back into the same channel they read from — normally a classic deadlock risk if every worker ends up blocked pushing while none are left to pop. Khaos avoids this by sizing the dispatch channel to the *exact* number of operations in the batch before starting any worker, which is a proven upper bound on total sends over the run's lifetime. No send can ever block, so the deadlock precondition simply doesn't exist.
- **Deterministic shutdown.** Every goroutine's main loop selects between the dispatch channel and `ctx.Done()`. Cancel the context and every worker exits within one loop iteration — no leaked goroutines, verified under `go test -race`.

## Usage Example

```go
package main

import (
	"context"
	"fmt"

	"khaos"
)

func main() {
	// A native Go batch — no string parsing, no query language.
	batch := []khaos.Operation{
		{SeqID: 0, Key: "user:42:balance", Type: khaos.OpWrite, Value: 100},
		{SeqID: 1, Key: "user:42:balance", Type: khaos.OpRead},  // depends on SeqID 0 (RAW)
		{SeqID: 2, Key: "user:7:balance", Type: khaos.OpWrite, Value: 50}, // independent key
	}

	storage := khaos.NewShardedMap()

	// Build the dependency graph.
	resolver := khaos.NewSequentialHazardResolver()
	all, runnable, err := resolver.Build(batch)
	if err != nil {
		panic(err)
	}

	// Wire up the output stream before starting execution.
	rob := khaos.NewInOrderReorderBuffer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := rob.Drain(ctx, len(all))

	// Run the batch. 0 workers -> defaults to runtime.NumCPU().
	pool := khaos.NewConcurrentWorkerPool(rob, 0)
	go func() {
		if err := pool.Run(ctx, storage, runnable); err != nil {
			fmt.Println("pool error:", err)
		}
	}()

	// Results arrive strictly in SeqID order, regardless of execution order.
	for op := range results {
		fmt.Printf("SeqID %d (%s %s) -> result=%v err=%v\n",
			op.SeqID, op.Type, op.Key, op.Result, op.Err)
	}
}
```