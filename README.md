# Khaos: Out-of-Order Execution Engine & API Gateway

**A production-grade, hardware-inspired batch execution engine for Go — built to shield PostgreSQL from concurrent I/O contention in high-throughput, latency-sensitive systems.**

Khaos applies the same architectural principles that let modern CPUs outperform naive sequential execution — pipelining, hazard detection, out-of-order scheduling, and in-order commit — to batches of key-value operations. It ships both as an embeddable Go library and as a standalone microservice fronting PostgreSQL, purpose-built for the punishing write patterns of real-time logistics: dozens to thousands of location pings per second, from independent riders, that must never corrupt each other's state and must never make a customer wait on a stranger's slow write.

---

## Table of Contents

- [The Problem](#the-problem)
- [The Shield Pattern](#the-shield-pattern)
- [Architecture](#architecture)
- [The PHP Dispatcher](#the-php-dispatcher)
- [Benchmarks](#benchmarks)
- [Quickstart](#quickstart)
- [Project Structure](#project-structure)
- [Testing](#testing)

---

## The Problem

A hyper-local food delivery platform — Khaos was built against a Chowdeck/UberEats-style use case — generates a constant stream of rider location pings. Every active rider's device reports its position every few seconds. At even modest scale (a few hundred concurrent riders), that's a sustained burst of small, independent writes hitting the same table.

Send that traffic straight at PostgreSQL with one connection per request and no coordination, and two things go wrong:

1. **Connection exhaustion.** Postgres has a hard, finite connection limit. A naive "one goroutine, one connection, one query" model saturates it under real concurrent load.
2. **Unnecessary serialization.** A slow write to rider A's row has no business making rider B's ping wait — but a purely sequential processing model (or an under-tuned connection pool) makes exactly that happen, turning independent work into a queue.

Khaos exists to make the fast path fast and the contended path safe, automatically — without asking the caller to reason about locking, connection limits, or write ordering.

## The Shield Pattern

Khaos sits between the client-facing API and PostgreSQL as an **out-of-order execution shield**. The core idea: analyze a whole batch of incoming operations up front, execute everything that's provably independent concurrently, and only serialize the operations that actually touch the same data.

**1. The DAG Hazard Resolver** walks each incoming batch once (O(N), never a pairwise comparison) and builds a dependency graph keyed on real data hazards — RAW (read-after-write), WAR (write-after-read), and WAW (write-after-write) — scoped to each individual `rider_id`. Two pings for two different riders never generate a dependency edge, no matter how large the batch. This is what turns "5,000 updates" into "5,000 mostly-independent updates" instead of "5,000 things that must happen in order."

**2. The Worker Pool** executes every hazard-free operation concurrently across a fixed, bounded pool of goroutines — sized to match the database's actual connection budget, not the batch size. A slow write never blocks an unrelated one: the pool dynamically dispatches newly-unblocked operations the instant their last dependency clears, so the database's connection pool stays saturated with useful work instead of idling behind a single stalled query.

**3. The Reorder Buffer** re-sequences results back into original submission order before they ever reach the caller — the out-of-order execution happening underneath is completely invisible from the outside.

**4. PostgreSQL UPSERT semantics** close the loop. Every rider ping is written with `INSERT ... ON CONFLICT (rider_id) DO UPDATE`, keyed on a unique constraint on `rider_id`. This means:
   - The `active_rider_tracking` table stays bounded at *one row per active rider*, regardless of ping frequency — no runaway table growth from riders pinging every few seconds for a multi-hour shift.
   - Retried or duplicate pings are naturally idempotent: re-applying the same location update twice produces the same end state, which is exactly what makes the at-least-once delivery model below safe.

The net effect: Postgres only ever sees a bounded, pool-tuned stream of upserts, ordered exactly where ordering matters and parallelized everywhere it doesn't — shielded from both the volume and the raw concurrency of the traffic hitting the API in front of it.

## Architecture

```
                    ┌─────────────────────────┐
                    │   KhaosDispatcher (PHP)  │
                    │   buffers rider pings    │
                    └────────────┬────────────┘
                                 │ POST /api/v1/riders/batch
                                 ▼
┌───────────────────────────────────────────────────────────┐
│                    Go API Gateway (net/http)                │
│                                                               │
│   ┌───────────────┐   ┌────────────────┐   ┌──────────────┐│
│   │  DAG Hazard    │──▶│  Worker Pool   │──▶│   Reorder    ││
│   │  Resolver O(N) │   │ (bounded conc.)│   │   Buffer     ││
│   └───────────────┘   └────────┬───────┘   └───────┬──────┘│
│                                 │                    │       │
└─────────────────────────────────┼────────────────────┼───────┘
                                 │                    │
                                 ▼                    ▼
                    ┌─────────────────────┐   200 OK, results
                    │   PostgreSQL         │   in original
                    │   active_rider_      │   seq_id order
                    │   tracking (UPSERT)  │
                    └─────────────────────┘
```

| Component | Responsibility |
|---|---|
| `SequentialHazardResolver` | Builds the per-batch dependency DAG in O(N), detecting RAW/WAR/WAW hazards per `rider_id` |
| `ConcurrentWorkerPool` | Executes runnable operations concurrently; dynamically dispatches newly-unblocked work; deadlock-free by construction |
| `InOrderReorderBuffer` | Re-sequences out-of-order results into strict `seq_id` order before they reach the caller |
| `PostgresStorageEngine` | Prepared-statement UPSERT/SELECT/DELETE against `active_rider_tracking`, safe for concurrent use |
| `Server` (`net/http`) | Translates JSON batches into engine operations; enforces per-batch timeouts; graceful shutdown |

## The PHP Dispatcher

`KhaosDispatcher` is the PHP-side client that collects rider pings and ships them to the gateway as a single batch. It follows an **at-least-once delivery** model:

- `addRiderUpdate()` buffers a ping in memory — no network I/O yet.
- `dispatchBatch()` sends the entire buffer as one HTTP POST and **only clears the buffer on a confirmed successful round trip.**
- Any failure — connection refused, timeout, HTTP 5xx, an unparseable response — throws `KhaosDispatchException` and leaves the buffer untouched, so the caller can safely call `dispatchBatch()` again.

This is deliberately *not* an exactly-once guarantee — a response lost in transit after the server already committed the write is possible, and a naive retry would resend that batch. It's safe anyway, specifically **because** of the Shield Pattern's UPSERT semantics: every operation in a rider-ping batch is a `WRITE` keyed on `rider_id`, so resending the same ping twice just overwrites the row with the same values. At-least-once delivery plus idempotent writes composes into effectively-once behavior, without needing deduplication logic anywhere in the stack.

## Benchmarks

Measured with the included load generator (`cmd/simulator`) against a connection-pool-tuned Gateway instance (`KHAOS_DB_MAX_OPEN_CONNS=25`), simulating the target production workload: 100 concurrent clients, each submitting an independent batch of 50 rider pings.

```
Khaos Chaos Simulator
  target:             http://localhost:8080/api/v1/riders/batch
  concurrent clients: 100
  batch size:         50 updates/request
  total updates:      5000

========================================
        SIMULATION SUMMARY REPORT
========================================
  Total Time:          1.852s
  Requests Sent:       100
  Rider Updates Sent:  5000 (100 clients x 50 riders)
  Throughput:          54.0 requests/sec, 2700.0 updates/sec
----------------------------------------
  HTTP 200 (Success):  100
  HTTP 5xx (Server):   0
  Timeouts:            0
  Network Errors:      0
  Success Rate:        100.0%
========================================

✓  Clean sweep: every concurrent batch completed with HTTP 200.
```

**~2,700 rider updates/sec sustained, 100% success rate, zero connection-pool exhaustion** — with 100 concurrent clients hammering the gateway simultaneously. This is the direct, measurable payoff of the Shield Pattern: the DAG resolver keeps independent riders' writes from serializing behind each other, and the tuned connection pool (`SetMaxOpenConns`/`SetMaxIdleConns` in `api/main.go`) keeps the worker pool's concurrency matched to what Postgres can actually sustain, rather than either starving the database of connections or overwhelming it.

Reproduce this yourself:

```bash
go build -o simulator ./cmd/simulator
./simulator -url http://localhost:8080/api/v1/riders/batch -concurrency 100 -batch-size 50
```

## Quickstart

### Prerequisites

- Go 1.22+
- PostgreSQL 14+
- PHP 8.3+ with the `curl` extension (only needed for the PHP dispatcher)

### 1. Set up the database

```sql
CREATE TABLE IF NOT EXISTS active_rider_tracking (
    rider_id        UUID PRIMARY KEY,
    order_id        UUID NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude       DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    current_status  VARCHAR(20) NOT NULL CHECK (current_status IN ('AT_VENDOR', 'EN_ROUTE', 'ARRIVED', 'DELIVERED')),
    eta_minutes     INTEGER NOT NULL CHECK (eta_minutes >= 0),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_active_rider_tracking_order_id
    ON active_rider_tracking (order_id);
```

### 2. Configure and run the gateway

Khaos reads configuration entirely from environment variables — no credentials live in source.

```bash
export KHAOS_DB_HOST=localhost
export KHAOS_DB_PORT=5432
export KHAOS_DB_USER=postgres
export KHAOS_DB_PASSWORD=your-password-here   # required, no default
export KHAOS_DB_NAME=khaos
export KHAOS_DB_SSLMODE=disable               # use require/verify-full outside local dev

# Optional tuning (defaults shown)
export KHAOS_DB_MAX_OPEN_CONNS=25
export KHAOS_DB_MAX_IDLE_CONNS=25
export KHAOS_WORKER_COUNT=8
export KHAOS_BATCH_TIMEOUT=5s
export KHAOS_LISTEN_ADDR=:8080

go run ./api
```

You should see:

```
2026/09/02 09:37:45 connected to PostgreSQL successfully
2026/09/02 09:37:45 khaos API gateway listening on :8080
```

### 3. Send a batch

```bash
curl -X POST http://localhost:8080/api/v1/riders/batch \
  -H "Content-Type: application/json" \
  -d '[
    {
      "seq_id": 0,
      "operation_type": "WRITE",
      "rider_id": "11111111-1111-1111-1111-111111111111",
      "payload": {
        "order_id": "22222222-2222-2222-2222-222222222222",
        "latitude": 6.5244,
        "longitude": 3.3792,
        "current_status": "EN_ROUTE",
        "eta_minutes": 12
      }
    },
    {
      "seq_id": 1,
      "operation_type": "READ",
      "rider_id": "11111111-1111-1111-1111-111111111111"
    }
  ]'
```

### 4. Dispatch from PHP

```php
require 'php/KhaosDispatcher.php';

$dispatcher = new KhaosDispatcher('http://localhost:8080');

$dispatcher->addRiderUpdate(
    riderId: '11111111-1111-1111-1111-111111111111',
    orderId: '22222222-2222-2222-2222-222222222222',
    lat: 6.5244,
    lng: 3.3792,
    status: 'EN_ROUTE',
    eta: 12,
);

try {
    $results = $dispatcher->dispatchBatch();
} catch (KhaosDispatchException $e) {
    // buffer is untouched — safe to retry
}
```

### 5. Load test it

```bash
go build -o simulator ./cmd/simulator
./simulator -url http://localhost:8080/api/v1/riders/batch
```

## Project Structure

```
khaos/
├── api/                    # Gateway process entry point (package main)
│   └── main.go
├── cmd/
│   └── simulator/          # Standalone load generator (package main)
│       └── main.go
├── php/
│   └── KhaosDispatcher.php # PHP client bridge
├── types.go                # Operation, DAGNode, ExecutionState
├── interfaces.go           # StorageEngine, HazardResolver, WorkerPool, ReorderBuffer
├── storage.go               # ShardedMap (in-memory, embedded use)
├── postgres_storage.go       # PostgresStorageEngine (UPSERT-backed)
├── hazard.go                # SequentialHazardResolver (O(N) DAG builder)
├── workerpool.go             # ConcurrentWorkerPool
├── reorder.go                # InOrderReorderBuffer
├── server.go                  # net/http API Gateway
└── go.mod
```

## Testing

```bash
go test -v -race ./...
```

The core engine (`types.go` through `workerpool.go`) is dependency-free standard library Go. `postgres_storage.go` and `api/main.go` are the only files depending on an external package (`github.com/lib/pq`), required to actually speak PostgreSQL's wire protocol.

---

*Khaos: because your database shouldn't have to wait for one slow rider to serve the other four thousand.*