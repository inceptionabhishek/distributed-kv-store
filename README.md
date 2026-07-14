# distributed-kv-store
distributed-kv-store


![Build and Test](https://github.com/inceptionabhishek/distributed-kv-store/actions/workflows/build-and-test.yml/badge.svg)




# Distributed Key-Value Store

A distributed, replicated, eventually-consistent key-value store built from scratch in C++, using gRPC for networking. Built incrementally in stages, each one solving a specific, real failure mode of the previous stage rather than being designed upfront.

## Why this exists

Most "distributed KV store" portfolio projects stop at consistent hashing. This one goes further: it implements and *measures* the actual tradeoffs distributed systems make — consistency vs. availability, replication cost vs. durability, and the real latency/throughput impact of tolerating a node failure — rather than just asserting them.

## Architecture

```
                    ┌─────────────┐
                    │   Router    │  <- client-side coordinator
                    │ (this repo) │     (consistent hashing, quorum,
                    └──────┬──────┘      failure detection, hinted handoff)
                           │
        ┌──────────────────┼──────────────────┬──────────────────┐
        │                  │                  │                  │
   ┌────▼────┐        ┌────▼────┐        ┌────▼────┐        ┌────▼────┐
   │ Node 0  │        │ Node 1  │        │ Node 2  │        │ Node 3  │
   │ (gRPC   │        │ (gRPC   │        │ (gRPC   │        │ (gRPC   │
   │ server) │        │ server) │        │ server) │        │ server) │
   └─────────┘        └─────────┘        └─────────┘        └─────────┘
```

Each **node** is an independent process running a thread-safe, in-memory key-value store behind a gRPC service. The **router** is a client-side coordinator: it never stores data itself, only decides which nodes to talk to and reconciles their answers.

## What it does, stage by stage

| Stage | What was added | The problem it solves |
|---|---|---|
| 1 | Thread-safe single-node store (`std::unordered_map` + mutex) | Baseline: correct concurrent access to a shared map |
| 2 | gRPC server/client | Turns the in-process store into something reachable over a network |
| 3 | Multi-node cluster, `hash(key) % N` routing | Spreads keys across multiple nodes |
| 4 | Consistent hashing with virtual nodes | `% N` remaps ~75% of keys when the cluster resizes; consistent hashing remaps only ~1/(N+1) |
| 5 | Synchronous full replication | A single node dying shouldn't lose data — but "wait for all N replicas" means one dead replica blocks every write to its keys |
| 6 | Quorum reads/writes (W=2, R=2, N=3) + last-write-wins via timestamps | Fixes Stage 5: tolerates any single node being down for both reads and writes, because W + R > N guarantees overlap |
| 7 | Heartbeat failure detection + hinted handoff | Fast failover instead of waiting on RPC timeouts; recovers the 3rd replica copy automatically once a dead node comes back |

Every stage above was verified working before moving to the next — including deliberately breaking things (killing nodes mid-run, resizing the cluster) to confirm the failure modes and fixes behave as claimed, not just in theory.

## Key design decisions

**Deterministic hashing (FNV-1a + MurmurHash3 finalizer), not `std::hash`.**
`std::hash<std::string>` is randomized per-process in some standard library implementations (a hash-flood DoS mitigation) — fine for an in-memory map, fatal for a router that needs the same key to route to the same node across restarts. A custom FNV-1a hash fixes determinism, but its raw output has weak avalanche (similar inputs like `"node0#0"` and `"node0#1"` hash to nearby values) — bad for scattering virtual nodes around a ring. A MurmurHash3-style bit-mixing finalizer fixes that.

**Consistent hashing with virtual nodes (10 per physical node).**
A single ring point per node means load balance is at the mercy of where that one hash lands. Multiple virtual points per node average out the imbalance — verified: adding a 4th node to a 3-node cluster with naive `% N` moved ~75% of keys; with the ring, ~13-25%.

**Quorum (W=2, R=2, N=3), not wait-for-all.**
Verified directly: with wait-for-all replication, killing 1 of 4 nodes failed 5 of 8 writes and produced 2 read mismatches, even though only one node was down. Switching to quorum (W+R > N) fixed both failure modes while keeping the same guarantee — any read is mathematically guaranteed to overlap with the most recent acknowledged write.

**Last-write-wins via client-assigned timestamps, not vector clocks.**
When replicas disagree (because one missed a write), the router needs a deterministic way to pick a winner. LWW is simple and works well when clock skew across nodes is small and true concurrent writes to the same key are rare. The tradeoff: LWW can silently drop a legitimately concurrent write if clocks disagree or two writes race within the same millisecond. Vector clocks (as in the original Dynamo paper) solve this properly by tracking causality instead of wall-clock time, at the cost of needing explicit application-level conflict resolution (e.g. "sibling" values) when true concurrent writes are detected. LWW was chosen here for simplicity; this is a conscious, documented tradeoff, not an oversight.

**Router-side hinted handoff, not replica-side.**
When a write's target replica is down, the router holds the write in memory and replays it once the target recovers. Real systems (Cassandra) store hints on *another* live replica instead of the coordinator, so the hint survives a coordinator crash. This implementation's hints live only in the router's memory and are lost if the router process dies — a known, deliberate simplification.

## Known limitations (things I'd build next)

- **Sequential replica fan-out.** `Put()` waits on all N replica RPCs sequentially, even after write quorum is already satisfied. Benchmarking surfaced this concretely: a healthy 4-node cluster was measurably *slower* than one with a node down, because the down node's RPC was skipped instantly while the healthy path paid for 3 full sequential round-trips. Firing replica RPCs concurrently and returning after `write_quorum_` successes would fix this — see the Benchmarks section below for the full analysis.
- **No persistence.** Everything is in-memory; a node restart loses that node's data until it's re-synced from replicas via reads/hinted handoff. A write-ahead log would fix this.
- **No real data rebalancing on membership change.** Consistent hashing changes *routing* correctly when a node joins/leaves, but nobody actually migrates the underlying data between nodes yet — a key that "moves" to a new node via the ring isn't physically copied there.
- **Hints don't survive a router crash**, as noted above.

## Project structure

```
├── CMakeLists.txt          # cross-platform build (macOS/Homebrew Config mode, Linux/pkg-config)
├── proto/kvstore.proto     # gRPC service definition
├── include/
│   ├── kv_store.hpp        # single-node store (Stage 1/6)
│   ├── simple_hash.hpp     # deterministic hash + ring-hash finalizer
│   ├── consistent_hash_ring.hpp   # Stage 4 ring with virtual nodes
│   ├── failure_detector.hpp       # Stage 7 heartbeat prober
│   └── router.hpp          # client-side coordinator (Stages 4-7 combined)
├── src/
│   ├── server.cpp          # gRPC node server
│   ├── client.cpp          # minimal single-node client (Stage 2)
│   ├── router.cpp          # interactive demo of the full router
│   ├── ring_demo.cpp       # standalone proof that consistent hashing moves fewer keys than % N
│   ├── fd_demo.cpp         # standalone failure-detector liveness demo
│   └── benchmark.cpp       # load generator: throughput + latency percentiles
├── tests/                  # GoogleTest unit tests (26 tests)
└── .github/workflows/      # CI: build + test on every push
```

## Building and running

```bash
mkdir build && cd build
cmake ..
make
```

Start 4 storage nodes (separate terminals):
```bash
./kv_server 50051
./kv_server 50052
./kv_server 50053
./kv_server 50054
```

Run the router demo:
```bash
./kv_router
```

Run tests:
```bash
ctest --output-on-failure
```

## Benchmarks

Measured with `./benchmark <threads> <duration_sec> <keyspace> <write_ratio>` — 8 threads, 10 seconds, 1000-key keyspace, 50/50 read/write split, against the full 4-node cluster with quorum W=2/R=2.

**All 4 nodes healthy:**

```
Benchmark config: threads=8 duration=10s keyspace=1000 write_ratio=0.5
Pre-populated 1000 keys.
=== Results ===
Total ops:      159610
Elapsed:        10.0039s
Throughput:     15954.8 ops/sec
Latency p50:    487.75 us
Latency p95:    726.042 us
Latency p99:    895.417 us
Latency max:    8514.17 us
```

**1 of 4 nodes down (killed before the run):**

```
Benchmark config: threads=8 duration=10s keyspace=1000 write_ratio=0.5
[failure-detector] localhost:50051 is now DOWN
Pre-populated 1000 keys.
=== Results ===
Total ops:      205859
Elapsed:        10.0037s
Throughput:     20578.2 ops/sec
Latency p50:    378.292 us
Latency p95:    522.791 us
Latency p99:    631.375 us
Latency max:    8692.62 us
```

### A counterintuitive result, explained

The one-node-down run is ~29% *faster* (20,578 vs 15,954 ops/sec) with lower latency at every percentile. That's not noise, and it's not "failure is free" — it's a real gap in the current implementation.

`Router::Put()` fans out to all 3 replicas **sequentially**, and always waits on all 3 round-trips even after the 2 successes needed for write quorum have already landed. When a replica is known-dead (via the failure detector), that "call" is skipped instantly — an in-memory check, no network round-trip — and the write is queued as a hint instead. So the healthy path pays for 3 sequential network round-trips per write, while the degraded path pays for only 2, plus a free instant skip. Fewer real round-trips per write is exactly why it's faster.

This points to a concrete, valuable improvement: fire replica RPCs **concurrently** rather than sequentially, and return as soon as `write_quorum_` successes arrive rather than waiting on every replica. That would make the healthy case roughly as fast as a single round-trip (bounded by the slowest of the 2 fastest replicas) instead of the sum of 3, and would remove this anomaly entirely. Left as a documented next step rather than implemented here, since it changes the concurrency model of the write path meaningfully (needs per-replica threads or async gRPC calls plus careful handling of the "still count a late 3rd success after quorum is already met" case for repair purposes).

## Testing

26 GoogleTest unit tests covering the single-node store (concurrency, edge cases, last-write-wins) and the consistent hash ring (determinism, load balance, minimal-movement-on-resize). CI runs the full build + test suite on every push via GitHub Actions.