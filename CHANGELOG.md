# hakuzu Changelog

## Phase 5: Operational Resilience + Observability

### Prometheus metrics
`HakuzuMetrics` — lock-free `AtomicU64` counters following hadb's pattern.

Counters: `writes_total`, `writes_forwarded`, `reads_total`, `forwarding_errors`
Gauges: `last_write_duration_us`, `last_read_duration_us`, `journal_sequence`

API: `snapshot()` → `HakuzuMetricsSnapshot` → `to_prometheus()` text format. Designed to concatenate with hadb + graphstream metrics for a single `/metrics` endpoint.

Tests (4 unit): `test_metrics_default_zero`, `test_metrics_increment`, `test_metrics_snapshot`, `test_metrics_prometheus_format`

Files: `src/metrics.rs`, `src/lib.rs`

### Snapshot staging cleanup
Fixed snapshot cleanup on ALL error paths in `run_snapshot_loop()` — previously only cleaned staging dir on success, leaving orphaned `snapshots_tmp/` dirs on failure or panic.

Added `cleanup_stale_staging(base_dir, max_age) -> u64` helper for periodic cleanup of staging dirs older than configurable max age.

Tests (3 unit): `test_cleanup_stale_staging_removes_old_dir`, `test_cleanup_stale_staging_keeps_recent_dir`, `test_cleanup_stale_staging_nonexistent`

Files: `src/database.rs`, `src/snapshot.rs`

### RSS profiling tools
- `src/bin/rss_bench.rs` — opens local HaKuzu, writes batches (1K/5K/10K), queries, idles, prints STAGE markers
- `bench/measure_rss.py` — launches rss_bench, samples RSS via `ps -o rss=`, correlates with stage markers. Catches regressions like walrust's 70MB→20MB fix.

### Connection pool investigation
`bench/connection_cost.rs` — benchmarks `lbug::Connection` creation cost (10K iterations). Reports avg/p50/p95/p99 and recommends pool or not based on results.

### Read semaphore tuning
`bench/semaphore_tuning.rs` — benchmarks read latency with 32 concurrent readers at semaphore values 8/16/32/64, reports avg/p50/p95/p99 for each.

Files: `Cargo.toml` ([[bin]] + [[example]] entries)

## Phase 4: Structured Error Types

Public API methods (`execute`, `query`, `handoff`, `close`) now return `crate::error::Result<T>` with `HakuzuError` enum instead of `anyhow::Result`. Consumers can match on specific failure modes:

- `HakuzuError::LeaderUnavailable(msg)` — write forwarding failed, leader unreachable or returned error
- `HakuzuError::NotLeader` — follower has no leader address for forwarding
- `HakuzuError::DatabaseError(msg)` — Kuzu query, prepare, execute, or connection error
- `HakuzuError::JournalError(msg)` — graphstream write, seal, or replication error
- `HakuzuError::CoordinatorError(msg)` — hadb lease, join, leave, or handoff error
- `HakuzuError::EngineClosed` — semaphore closed, engine shut down

Internal methods and `open()` still use `anyhow::Result` since setup errors are one-time and don't need matching.

### Files
- `src/error.rs` — `HakuzuError` enum, `Display`, `Error`, `From<anyhow::Error>`, `Result<T>` type alias
- `src/database.rs` — `execute()`, `query()`, `handoff()`, `close()`, `execute_forwarded()` updated
- `src/lib.rs` — re-exports `HakuzuError`

### Tests (10 unit)
- Display for all 6 variants, Error trait, From<anyhow>, Result alias, exhaustive match

## Phase 3: Production Hardening

Five-step production hardening across graphstream and hakuzu.

### mimalloc global allocator
Added `#[global_allocator]` with mimalloc to both binaries (`ha_experiment.rs`, `ha_writer.rs`). macOS system allocator never returns freed memory to OS — Kuzu's buffer pool checkpoint cycles cause RSS to monotonically grow. mimalloc returns memory eagerly.

### Graceful handoff drain barrier
`handoff()` now acquires `write_mutex` to drain in-flight writes, seals the journal via `replicator.sync()`, then delegates to the coordinator. Previously, the last write's journal entry could be lost if handoff raced with a write. `close()` also drains and seals before leaving the cluster.

### Snapshot-based cold start recovery
Leader periodically creates tar.zst snapshots of the Kuzu database directory and uploads to S3. New nodes download the latest snapshot on `open()`, extract it, and resume from the snapshot's journal sequence — skipping replay of all prior entries. Configurable via `snapshot_interval()` and `snapshot_every_n_entries()` on the builder.

- `src/snapshot.rs` — create, upload, download, extract snapshots (608 lines, 16 unit tests)
- `src/database.rs` — `SnapshotConfig`, `SnapshotContext`, snapshot loop in role listener, cold start bootstrap in `open()`
- Dependencies: `tar`, `zstd`, `hex`

### graphstream: O(1) recovery via chain hash trailer
Replaced `recovery.json` with a 32-byte chain hash trailer in sealed `.graphj` segments. Atomic with the seal operation (header + body + trailer + fsync). Recovery reads the last sealed segment's header + trailer — O(1) instead of O(N) full entry scan. See graphstream CHANGELOG for details.

### graphstream: S3 retry & circuit breaker
Transient S3 errors retried with exponential backoff + jitter. Circuit breaker prevents hammering degraded endpoints. See graphstream CHANGELOG for details.

## Phase 2: HaKuzu Production Library API

HaKuzu is now what HaQLite is for SQLite — embed in one line, get HA automatically. All HA logic that was manual wiring in the 490-line ha_experiment.rs binary is now encapsulated in the library API.

### API

```rust
use hakuzu::{HaKuzu, QueryResult};

// HA mode
let db = HaKuzu::builder("my-bucket")
    .prefix("myapp/")
    .secret("my-token")
    .open("/data/graph", "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))")
    .await?;

// Writes — leader executes + journals; follower auto-forwards
db.execute("CREATE (p:Person {id: $id, name: $name})", Some(json!({"id": 1, "name": "Alice"}))).await?;

// Reads — always local
let result: QueryResult = db.query("MATCH (p:Person) RETURN p.id, p.name", None).await?;

// Local mode — no S3, no HA
let db = HaKuzu::local("/data/graph", "CREATE NODE TABLE IF NOT EXISTS ...")?;
```

### New files

- `src/mutation.rs` — `is_mutation()` keyword check for auto-routing reads vs writes
- `src/values.rs` — `lbug_to_json()`, `json_to_lbug()`, `json_params_to_graphstream()` value conversions
- `src/forwarding.rs` — `ForwardedExecute`, `ExecuteResult`, Bearer auth, `POST /hakuzu/execute` handler
- `src/database.rs` — `HaKuzuBuilder`, `HaKuzu`, `HaKuzuInner`. Builder → open → Coordinator + KuzuReplicator + forwarding server + role listener

### Key design (from graphd-engine)

- **Connection-per-operation**: `lbug::Connection` is NOT Send. Created in `spawn_blocking`, dropped within.
- **write_mutex**: `tokio::sync::Mutex<()>` serializes writes (Kuzu single-writer).
- **read_semaphore**: `tokio::sync::Semaphore` bounds concurrent reads.
- **snapshot_lock**: `Arc<RwLock<()>>` — read for queries, write for CHECKPOINT.
- **Journal on success**: After successful write, send `PendingEntry` to graphstream `JournalSender`.

## Phase 1: Comprehensive Tests

### graphstream tests (30 unit tests)

- Writer lifecycle: `is_alive()` false after shutdown, flush with no pending writes
- Seal edge cases: seal empty segment, multiple consecutive seals, seal-then-write
- Encrypted segment round-trip: write with key, read with key, wrong key fails
- Reader edge cases: nonexistent dir, from_sequence beyond last entry, empty journal dir
- Compact edge cases: single input, compact with encryption
- ParamValue edge cases: nested lists, empty lists, extreme values (i64::MAX, NaN)
- Large entries: entries exceeding segment size trigger correct rotation
- Chain hash validation: hashes chain correctly across segments

### hakuzu tests (20 integration tests)

- Replay edge cases: empty journal returns since_seq, typed parameter binding with prepared statements
- Replicator builder: verify config methods apply correctly
- Replicator sync: seal current segment, sync with no pending data
- Replicator multi-db: add multiple databases, write to each, remove individually
- Replicator idempotency: add same name twice, remove nonexistent name
- Replay across segments: write enough entries to trigger rotation, replay all
- HaKuzu integration: single_node_local_mode, single_node_execute_and_query, two_node_forwarded_write, forwarding_error_no_leader, close_is_clean, auth_rejects_wrong_secret, auth_accepts_correct_secret
