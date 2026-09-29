# Explain the project in an SDE-2 interview

## A concise opening

“I built a C++17 distributed key-value store with gRPC storage nodes and a coordinator. A deterministic consistent-hash ring chooses three distinct replicas. Writes and reads use quorum two. Replica calls run concurrently and have deadlines. Values use hybrid-clock versions with a writer tie-breaker. I implemented WAL persistence, snapshot recovery, tombstone deletion, durable hinted handoff, read repair and periodic anti-entropy. Membership changes pause the coordinator and migrate data before activating a persisted ring epoch. Integration tests exercise node failure, recovery, concurrent writes and process kills.”

Follow immediately with the boundaries: eventual LWW reconciliation, one authoritative coordinator, full-scan repair, retained tombstones and no transactions.

## The five traces to learn

1. PUT: Reserve -> version token -> ring -> concurrent RPCs -> version comparison -> WAL/fsync -> two ACKs -> response -> late replica/hint.
2. GET: ring -> concurrent calls -> two replies -> max version -> tombstone/absence handling -> cancel excess RPC -> async repair.
3. Failure: detector suspects node -> requests still use deadlines -> failed mutation generates durable hint -> retry backoff -> recovered node receives version -> hint removal fsync.
4. Restart: directory lock -> snapshot replay -> WAL checksum validation -> truncate incomplete final append -> serve recovered state.
5. Join: pause admission -> drain writes -> scan sources -> copy to candidate owners -> persist epoch -> activate ring.

Read src/coordinator.cpp, src/kv_store.cpp and src/journal.cpp until you can follow these without notes.

## Questions and defensible answers

**Does W+R>N give linearizability?** It gives set intersection in a stable replica universe. It does not by itself give a total order that respects real time. LWW clock/version assumptions, concurrent requests and failed partial writes still matter.

**What does “successful write” mean?** At least W replicas applied that exact version or already hold the same version/payload. With always durability, each such replica fsynced its mutation before acknowledging. A stale rejection does not count.

**Can a failed write appear later?** Yes. Quorum failure is not rollback. Some replicas or durable hints may retain it. Retrying the same token is safe; generating a new token is a new write.

**Why hybrid-clock versions?** Millisecond timestamps collide. A logical counter handles multiple writes in one millisecond and clock rollback; a writer identifier orders independent writers. Observing remote versions maintains local causal ordering, but LWW still discards concurrent intent.

**What if a timestamp is far in the future?** It can dominate subsequent versions. HLC observation is not a clock-skew/security policy. Production admission would bound skew or restrict version creation to trusted coordinators.

**Why keep tombstones?** Physically deleting a key discards evidence of deletion. A stale isolated copy can reintroduce it. We retain tombstones until a future protocol can prove all relevant replicas have observed them.

**Why do writes continue after quorum but reads get cancelled?** Late writes maintain replication coverage, bounded by deadlines. A read can return after quorum; selected state is repaired asynchronously and periodic anti-entropy covers unread discrepancies.

**What happens to repair when the queue fills?** Foreground admission and repair scheduling are bounded and rejection is measured. Periodic anti-entropy retries repair later. This does not eliminate disk saturation or unbounded hint growth across unique keys.

**Is hinted handoff enough after a node loses all its data?** No. Hints describe only observed missed writes. Full-scan anti-entropy recovers older keys from surviving copies.

**How is a snapshot crash-safe?** Publish and sync a complete snapshot before truncating the WAL. If both old WAL and new snapshot survive, version replay ignores duplicate mutations. Incomplete final WAL bytes are discarded; complete corruption fails closed.

**Why does reconfiguration stop traffic?** It gives a clear cutover barrier while outstanding writes drain. An online alternative needs versioned ownership, dual writes or a catch-up log, fencing and a reliable membership authority.

**Can two routers safely change membership?** No. Node services do not fence ring epochs. This implementation requires one authoritative coordinator. A production design would use consensus-backed metadata and reject stale epochs at nodes.

**Why not Raft for data?** Raft would provide an elected leader and replicated log for a different consistency model. This project explores quorum LWW registers and repair. Consensus would be useful for membership metadata if multi-coordinator operation were added.

**What would you optimize next?** Incremental anti-entropy, batching WAL fsync, asynchronous gRPC rather than a worker pool of blocking RPCs, sharded storage locks, streaming migration, and safe cleanup of obsolete replicas.

## Evidence to show

- Run scripts/demo.py and narrate failure, router restart and convergence.
- Run the slow-third-replica test and explain why quorum latency remains bounded.
- Show checksum-corruption and SIGKILL recovery tests.
- Run scripts/benchmarks.py under known durability settings, keep raw JSON, and discuss errors and variability.
- Show Prometheus hint backlog rising during an outage and falling after recovery.

## Resume wording

- Built a C++17/gRPC distributed KV store with consistent hashing, configurable quorum replication, deterministic version ordering and concurrent replica coordination.
- Implemented checksummed WAL/snapshot recovery, tombstone deletion, durable hinted handoff and asynchronous replica repair.
- Added failure-injection integration tests, versioned membership migration, Prometheus metrics and Docker Compose deployment.

Add benchmark numbers only after running and documenting them on your own hardware. Do not claim production readiness, linearizability, unlimited scalability or zero-loss durability in periodic/memory modes.
