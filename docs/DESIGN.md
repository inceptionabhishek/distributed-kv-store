# Design decisions and guarantees

This is a Dynamo-inspired educational database with one authoritative coordinator per cluster. It uses LWW registers and quorum replication. There is no consensus protocol or transactional isolation.

## 1. Version ordering and retry identity

Versions compare lexicographically as `(physical_ms, logical, writer)`. The hybrid clock advances even when wall time stays equal or moves backward, and observes received versions before generating another version. Writers have random process identities. Coordinator clock reservations are written to a checksummed journal before being handed to clients; restored clock state prevents regression on restart.

`kvctl` reserves a version from the endpoint, prints it before mutation, and accepts `--version` to retry. An identical current version and payload is acknowledged as a duplicate without another WAL append. The same version with a different payload is rejected as a conflict. A version below the current version is rejected as stale. Only applied mutations and exact duplicates count toward a write quorum.

This is version-based idempotency, not an unlimited request-result ledger: after a newer value overwrites a key, replaying an older operation returns STALE. Reusing an operation version for a different key is a caller error; the system does not implement global operation-ID uniqueness.

Versions order writes deterministically but do not recover all concurrent application intent. A conflicting lower version loses. They do not make wall clocks trustworthy or establish real-time global ordering across independent coordinators.

## 2. Quorum protocol

For a stable preference list of N distinct replicas, W+R>N guarantees intersection between acknowledged write replicas and responding read replicas. Default N=3/W=2/R=2 also gives write/write overlap. Configuration rejects impossible quorums, insufficient membership, duplicate nodes and non-positive deadlines.

Writes submit unary RPCs to a bounded worker pool and return when W acknowledge. Remaining writes continue with deadlines so a slow third replica cannot block the client; errors generate repair hints. Read RPCs run concurrently and collect R transport-successful responses, including valid absence responses. The largest version wins, including tombstones. Outstanding reads are cancelled. Stale or unobserved replicas are repaired through the hint queue.

A missing key, an unavailable quorum, and an equal-version conflict are separate read outcomes. A failed or timed-out write has an uncertain outcome: some replicas may already have applied it, and retained hints may apply it later. Applications must reuse the version when retrying.

The worker pool bounds queued foreground work. Queue admission failure is explicit and counted. Deadlines propagate from gateway to replica calls and cover queued RPC execution. They do not interrupt local disk fsync or time waiting for the topology lock during maintenance; a migration pauses request admission.

## 3. Storage durability

The node is an unordered map protected by a mutex. Reads copy values out of the critical section. Writes compare versions under that same mutex, append the mutation to the WAL, then publish it in memory.

WAL framing uses a little-endian 64-bit byte length, a 64-bit FNV-1a checksum, and binary mutation bytes. The checksum detects accidental corruption; it is not a cryptographic integrity guarantee. Keys and values are length-prefixed and may contain binary bytes. The gRPC API limits keys to 4 KiB and values to 1 MiB.

Recovery reads snapshot records first and then the WAL. An incomplete final WAL header or payload is treated as a torn append and truncated to the last complete record. Complete checksum failures and malformed records fail startup instead of discarding acknowledged data. An I/O error poisons the journal so subsequent mutations fail until recovery.

Modes:

| Mode | Acknowledgement policy | Crash expectation |
|---|---|---|
| always | WAL append + fsync before acknowledgement | Acknowledged replica versions survive process crash, assuming filesystem/fsync behavior |
| periodic | Append before acknowledgement; background fsync every 100 ms | Recent writes may be lost; 100 ms is a target interval, not a hard durability SLA |
| memory | No journal | Restart loses all local data |

Snapshots hold the store mutex, write the complete current state to a temporary file, fsync it, atomically rename it, fsync the directory, then truncate/fsync the WAL. A crash between snapshot publication and WAL truncation is safe because replayed duplicate versions are idempotent. There is an exclusive per-journal directory lock. Snapshotting is stop-the-world at a node and does not implement SSTables or background LSM compaction.

Node shutdown drains gRPC calls for up to three seconds. A forceful kill skips shutdown; WAL recovery still works in always mode.

## 4. Deletes and repair

A delete is a normal versioned mutation with a tombstone and an empty value. Reads return absence when the newest version is a tombstone, while scans and repair preserve that tombstone.

Tombstones are deliberately retained indefinitely. A fixed TTL alone is not enough for safe garbage collection: an isolated replica or a forgotten copy could resurrect data. Safe pruning needs replica progress tracking and a bounded retention model.

Hints are keyed by (target node, key) and retain the newest pending version. Enqueue and successful removal are fsynced; crash recovery rebuilds the queue. Failed sends remain queued with exponential backoff from 200 ms to a 30-second cap. An old replay cannot erase a newer hint. A newer target version safely supersedes an older hint. Conflicting equal versions are not silently discarded.

The default hint TTL is 24 hours; setting RouterOptions.hint_ttl_ms=0 disables expiry. Expiry is counted and processed on failed replay attempts. Backoff timers reset on router restart. In-memory embedded routers have no durable hint journal unless a state directory is configured.

Anti-entropy periodically streams complete node snapshots, merges the largest version per key, and replays those versions onto the current preference lists. It repairs unread keys and replicas that lose local memory. It scans up to 100,000 records per node and does not use Merkle trees. This trades implementation simplicity for bandwidth, memory and CPU cost. Hints and full-scan repair share a background worker and can compete under prolonged outages.

## 5. Membership migration

Membership is controlled by a single coordinator and an increasing epoch. A change:

1. Acquires exclusive topology access, pausing foreground admission and repair.
2. Drains outstanding replication and read-repair jobs.
3. Requires successful scans from every old member and every new member.
4. Merges values, tombstones and all pending hints, including hints still backing off.
5. Copies the newest records to all N owners in the candidate ring.
6. Requires each new replica to acknowledge the migrated version.
7. Durably commits the new epoch and member list, then activates the new ring.

A failed copy leaves the old ring active; extra copied data is harmless and is not rolled back. Restart restores the persisted membership rather than blindly using the seed list. Decommission removes a reachable node after migration. Failed-node replacement while an old node remains unreachable is deliberately rejected.

This protocol assumes all normal writes go through this coordinator. Direct storage writes and independent routers are not fenced by node-side epochs. There is no gossip, distributed metadata election, concurrent migration or online streaming cutover. Old non-owner copies remain on disk; deleting them safely is future work.

## 6. Operational visibility

The gateway and nodes offer a gRPC Metrics RPC. Optional HTTP endpoints serve Prometheus text at /metrics. Metrics include quorum failures, RPC timeouts, repair scheduling, hint replay, hint backlog, expired hints, membership epoch, liveness, foreground latency buckets, stored records and WAL fsync time/count.

Coordinator logs are JSON lines with operation, hashed key, success and version time/counter. They avoid raw values. The version tuple is the mutation trace identity. This is basic correlation logging, not an OpenTelemetry distributed tracing implementation.

The deployment uses insecure gRPC and unauthenticated admin APIs. It is intended for a trusted development network. TLS, authorization and rate limits would be prerequisites before exposing it publicly.

## Source references

- [gRPC C++ service and client model](https://grpc.io/docs/languages/cpp/basics/)
- [gRPC deadlines](https://grpc.io/docs/guides/deadlines/)
- [Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
