# hakuzu Changelog

## Phase GraphFjord: Track hadb 0.4 final form + no silent fallbacks

> After: Phase GraphForge

Adapts hakuzu to the finalized hadb 0.4 lease/coordinator wiring
(hadb Phase Anvil i / Fjord / Driftwood / Shoal) and tightens the
builder contract so the caller explicitly picks every backend with
safety implications.

API shape now matches haqlite's:

- `LeaseConfig::new(store, instance_id, address)` — 3 args, store
  owned by the config (hadb Phase Fjord).
- `Coordinator::new` 6 args (lease store moved into `config.lease`).
- Builder preserves caller-provided LeaseConfig timing instead of
  overwriting it (mirrors haqlite Phase Driftwood fix).
- New `.lease_ttl()` / `.lease_renew_interval()` /
  `.lease_follower_poll_interval()` setters; `serve.rs` routes CLI
  timing knobs through them (no placeholder lease store).
- `MockObjectStore` test fixture reimplemented on
  `hadb_storage::StorageBackend`.

Safety gate: `HaKuzuBuilder::open()` errors when `.lease_store()` or
`.storage()` is missing. Leader election CAS atomicity and storage
placement are both caller decisions — hakuzu no longer silently
builds an `S3LeaseStore` / `S3Storage`, because Tigris and most
S3-compatible backends do not enforce atomic conditional PUTs and
split-brain the lease. `bucket` constructor arg and `.endpoint()`
setter removed (dead after default removal). Two new regression
tests cover the gates.

`hakuzu serve` bails via `serve::resolve_lease_store` /
`serve::resolve_storage` until Phase GraphRedline wires up a
config-driven dispatcher. 277 hakuzu tests green at ship.

## Phase GraphForge: hadb-storage Trait Migration

> After: Phase Cascade . Before: Phase GraphCinch (cinch-cloud)

Builder now accepts an `Arc<dyn hadb_storage::StorageBackend>` via a new
`.storage()` knob, defaulting to `hadb_storage_s3::S3Storage` built from the
builder's bucket/endpoint when absent. Embedders targeting the Cinch HTTP
protocol (cinch-engine, future customer SDKs) pass in `CinchHttpStorage` with
a `FenceSource` wired to the coordinator's lease. No more raw
`aws_sdk_s3::Client` in the replicator or snapshot paths.

- `KuzuReplicator`, `KuzuFollowerBehavior`, snapshot upload/download, and
  `SnapshotContext` all take `Arc<dyn StorageBackend>`.
- `hadb_io` dep deleted.
- Re-exported `hadb_storage::{CasResult, StorageBackend}` from `lib.rs`.
- Public re-export of `database::StagedJournalEntry` for the bolt-server
  staged-commit flow (used in cinch-engine's Phase GraphDurability).
- 167 lib tests green.

## Phase Cascade: ObjectStore Migration

Migrated hakuzu from raw S3 client to `ObjectStore` trait (aligned with graphstream Phase Aether).

- `KuzuFollowerBehavior`: `s3_client + bucket` replaced by `Arc<dyn ObjectStore>`. Constructor takes one arg instead of two.
- `KuzuReplicator`: `bucket + s3_client` replaced by `Arc<dyn ObjectStore>`. Removed `with_s3_client()`, `pull()` uses shared ObjectStore directly.
- `HaKuzuBuilder`: Constructs `S3Backend` from existing S3 client, shares across replicator + follower.
- `MockObjectStore` added in `tests/common/mod.rs` for non-S3 tests.
- Direct S3 deps (`aws-sdk-s3`, `aws-config`) still used by snapshot.rs, snapshot_loop.rs, database.rs, ha_experiment.rs.

176 tests passing (8 files changed, -9 net lines).

## Phase Drain: Synchronous Upload Ack

`KuzuReplicator::sync()` now uses `UploadWithAck` and awaits the oneshot response. Upload errors propagate instead of silent fire-and-forget. `HaMetrics` follower_caught_up/replay_position gauges wired.

## Phase Parity: Delegate Readiness to hadb Coordinator

Deleted local caught_up/replay_position from `KuzuFollowerBehavior`, repointed to coordinator-owned atomics via `JoinResult`. Deleted `readiness_state()` method. Atomic with hadb Phase Beacon and haqlite Phase Rampart-e.

## Phase 10: Follower Readiness & Edge Cases

### Follower readiness API
`is_caught_up()` returns whether a follower has replayed all available journal entries. Leaders and local-mode instances always return `true`. `replay_position()` returns the last successfully replayed sequence number. Both backed by `Arc<AtomicBool>` / `Arc<AtomicU64>` shared between `KuzuFollowerBehavior` and `HaKuzuInner`. Prometheus output includes `hakuzu_follower_caught_up` and `hakuzu_replay_position` gauges.

Role transitions update readiness: promoted → caught_up=true, demoted/fenced → caught_up=false.

Files: `src/database.rs`, `src/follower_behavior.rs`

### u64 precision verification
Verified as non-issue: `json_to_param_value()` handles all values in [0, i64::MAX] exactly via `as_i64()`. Values > i64::MAX fall through to f64, but Kuzu uses INT64 so they can't be stored anyway. Added boundary test and documentation.

Files: `src/values.rs`

### CALL mutation detection
Added `CALL` to `MUTATION_KEYWORDS` as safe default. Read-only CALLs (e.g. `CALL current_setting('threads')`) are routed to the write path — slower but correct. Missing a mutating CALL (e.g. `CALL db.checkpoint()`) would be catastrophic. Word-boundary check prevents false positives on `CALLBACK`.

Files: `src/mutation.rs`

### Tests (8 new, 176 total)
- `leader_is_always_caught_up` — local mode is_caught_up=true, replay_position=0
- `ha_leader_is_caught_up` — HA leader via from_coordinator, is_caught_up=true
- `readiness_metrics_in_prometheus` — verify follower_caught_up and replay_position gauges
- `test_param_value_i64_max_is_exact_int` — boundary test for i64::MAX precision
- `test_call_is_mutation` — CALL db.checkpoint() detected as mutation
- `test_call_read_only_is_mutation` — read-only CALL routed to write path
- `test_call_case_insensitive` — case insensitive CALL detection
- `test_callback_is_not_mutation` — CALLBACK does not match CALL (word boundary)

176 tests pass (121 lib + 19 ha_database + 14 integration + 22 real_world).

## Phase 9.5: Review Fixes — Metrics Wiring & Test Coverage

### Metrics wired into code paths
`writes_total` incremented in `execute_write_local()` on success. `reads_total` and `last_read_duration_us` recorded in `query()`. `writes_forwarded` incremented in `execute_forwarded()` on success. `forwarding_errors` incremented on 4xx and exhausted retries. `last_write_duration_us` recorded in `execute_write_local()`. Previously all counters were declared but never incremented — `prometheus_metrics()` returned zeros.

Files: `src/database.rs`

### Snapshot cleanup — remaining silent drops fixed
Two `let _ = remove_dir_all(...)` calls in `snapshot_loop.rs` (upload failure cleanup and success cleanup) converted to `remove_dir_logged()`. Now ALL error paths in the snapshot loop log errors.

Files: `src/snapshot_loop.rs`

### Weak tests replaced
- `forwarding_retry_on_server_error` → `forwarding_retry_exhausts_backoff` — shuts down leader, verifies follower retries with backoff (>1500ms elapsed), gets `LeaderUnavailable`, and `forwarding_errors` counter is 1.
- `close_rejects_new_reads` → `close_completes_with_data` (verifies close works with data) + `close_then_reopen_preserves_data` (close/reopen cycle preserves 5 entries).
- `metrics_track_writes_and_reads` — 3 writes + 2 reads, verifies exact counter values and non-zero durations in prometheus output.

### Tests (3 new, 168 total)
- `metrics_track_writes_and_reads` — verify prometheus counters reflect actual operations
- `forwarding_retry_exhausts_backoff` — verify retry timing and forwarding_errors metric
- `close_then_reopen_preserves_data` — verify close/reopen preserves all data
- `close_completes_with_data` — renamed from `close_rejects_new_reads`
- `prometheus_metrics_includes_hakuzu` — now verifies counter values, not just metric names

168 tests pass (116 lib + 16 ha_database + 14 integration + 22 real_world).

## Phase 9: Production Hardening — Graceful Ops

### Graceful shutdown
`close()` now closes the read semaphore before teardown — new reads immediately get `EngineClosed` while in-flight reads complete naturally. Previously, `close()` aborted tasks while reads could still be starting.

Files: `src/database.rs`

### Forwarding retry with backoff
`execute_forwarded()` retries transient failures (connection errors, 5xx) with exponential backoff: 100ms, 400ms, 1600ms. Client errors (4xx) fail immediately without retry. Previously, a single transient network failure between follower→leader failed the write.

Files: `src/database.rs`

### Snapshot cleanup error propagation
All `let _ = remove_dir_all(...)` calls in the snapshot loop now log errors at `error!` level instead of silently dropping them. Added periodic stale staging directory cleanup (every ~5 minutes) via `cleanup_stale_staging()` — removes orphaned `snapshots_tmp/` dirs older than 1 hour.

Files: `src/snapshot_loop.rs`, `src/database.rs`

### Forwarding latency metric
Added `last_forward_duration_us` to `HakuzuMetrics`, exposed as `hakuzu_last_forward_duration_seconds` in Prometheus format. Records timing on every forwarded write (success, client error, and exhausted retries). `prometheus_metrics()` now returns hakuzu operation metrics alongside hadb coordinator metrics (changed return type from `Option<String>` to `String`).

Files: `src/metrics.rs`, `src/database.rs`

### Tests (4 new, 166 total)
- `close_rejects_new_reads` — verify close() completes cleanly with data present
- `close_with_concurrent_reads` — close with 5 concurrent reads in flight
- `forwarding_retry_on_server_error` — verify leader path works with retry infrastructure
- `prometheus_metrics_includes_hakuzu` — verify hakuzu metrics in prometheus output

166 tests pass (116 lib + 14 ha_database + 14 integration + 22 real_world). *(Updated to 168 in Phase 9.5.)*

## Phase 8: Production Hardening — Silent Failure Elimination

### Journal send failure propagation
`execute_write_local()` now returns `Err` when the journal channel is closed (writer crashed). Previously logged at `error!` and silently continued — local state would diverge from journal without the caller knowing. Callers receive `HakuzuError::JournalError` for journal-specific failures vs `HakuzuError::DatabaseError` for Kuzu failures.

Files: `src/database.rs`

### Configurable read concurrency
`HaKuzuBuilder::read_concurrency(n)` configures the read semaphore. Default changed from 16 to 8 based on semaphore tuning benchmarks (8 permits is optimal for most workloads). `HaKuzu::local()` also uses the new default.

Files: `src/database.rs`

### Follower bootstrap documentation
Clarified `replicator.rs` `pull()` error handling: swallowing errors is intentional because (a) pull is called during `coordinator.join()` — failing would prevent follower startup, (b) the follower loop retries downloads, and (c) snapshot bootstrap in `open()` already populated the DB if available.

Files: `src/replicator.rs`

### Tests (3 new, 162 total)
- `journal_failure_returns_error` — kill journal writer, verify write returns `JournalError`
- `read_concurrency_default_works` — 20 concurrent reads with semaphore(8)
- `journal_error_contains_message` — verify `JournalError` Display format

162 tests pass (116 lib + 10 ha_database + 14 integration + 22 real_world).

## Phase 7: Real-World Integration Tests

22 tests closing critical coverage gaps. Tests exercise the full production path: rewrite → execute → journal → replay → verify.

### Rewriter execution tests (7 tests)
Verify rewritten queries actually execute against Kuzu and produce correct results:
- `gen_random_uuid()` → valid UUID v4 stored in database
- `current_timestamp()` → ISO 8601 with Z suffix stored
- `current_date()` → ISO 8601 date stored
- `REMOVE n.prop` → property nulled via SET NULL rewrite
- `REMOVE n.a, n.b` → multi-property null
- Mixed UUID + timestamp in same query
- User params + generated params coexist

### Leader/follower deterministic replay verification (5 tests)
Execute on leader via rewriter → journal → replay on follower → compare results:
- UUID replay: 3 `gen_random_uuid()` writes, follower has identical UUIDs
- Timestamp replay: `current_timestamp()` writes, follower timestamps match exactly
- Date replay: `current_date()` match
- REMOVE replay: leader REMOVE → follower SET NULL → both NULL
- Mixed sequence: plain + rewritten writes, full state comparison

### Complex Cypher coverage (7 tests)
Realistic Cypher through the full execute → journal → replay path:
- MERGE (create-or-match + update)
- SET with multiple properties (STRING, INT64, DOUBLE)
- DELETE nodes
- Relationships: CREATE REL TABLE + edges with properties, multi-hop query
- DELETE relationships (nodes preserved)
- All param types: INT64, STRING, BOOLEAN, DOUBLE

### Concurrent write stress tests (3 tests)
- 50 concurrent writes via tokio::spawn, verify count correct
- 30 concurrent UUID writes, verify all unique
- Interleaved reads + writes (50 writers, 10 readers), counts monotonically non-decreasing
- 1000 sequential writes, verify count + sample IDs

Files: `tests/real_world.rs` (new, 22 tests)

159 tests pass (116 lib + 7 ha_database + 14 integration + 22 real_world).

## Phase 6: Behavioral Parity with graphd/strana

Deterministic query rewriting and correctness fixes from behavioral audit against graphd.

### Deterministic query rewriter
Ported from strana's `rewriter.rs`. Rewrites non-deterministic Cypher functions to parameter references with concrete values before execution and journaling, ensuring followers replay identical results.

- `gen_random_uuid()` → `$__hakuzu_uuid_N` (UUID v4)
- `current_timestamp()` → `$__hakuzu_now_N` (ISO 8601 UTC with Z suffix)
- `current_date()` → `$__hakuzu_date_N` (ISO 8601 date)
- `REMOVE n.prop` → `SET n.prop = NULL` (Kuzu doesn't support REMOVE)
- Multi-property: `REMOVE n.a, n.b` → `SET n.a = NULL, n.b = NULL`
- String-literal aware, case-insensitive, word-boundary checks, backtick-quoted identifiers

Wired into `execute_write_local()` — journals the **rewritten** query + **merged** params.

Files: `src/rewriter.rs` (new, 732 lines), `src/database.rs`

Tests (41 unit): function rewrites, string literal protection, case/whitespace, REMOVE variants, merge_params, word boundary regression, multi-item REMOVE, date helpers

### Silent failure elimination
- `replay.rs`: entry failure now aborts transaction + returns error (was: `warn!` + skip + advance sequence — silent data divergence)
- `replicator.rs`: pull failure logged at `error!` level (was: `warn!`)
- `REMOVE` added to mutation keywords (label removal like `REMOVE n:Label` was not detected as a mutation)

### Param type conversion fixes
- `json_to_param_value()`: u64 > i64::MAX → f64 (was: Null — data loss)
- `json_to_param_value()`: Object → serialized JSON string (was: Null — data loss)
- `json_to_lbug()`: Object → `lbug::Value::String` of serialized JSON (was: error)

Files: `src/values.rs` (27 new tests)

### Follower replay locking
`KuzuFollowerBehavior` now holds `write_mutex` + `snapshot_lock` during replay via `with_locks()`. Prevents concurrent reads from seeing partial replay state. `write_mutex` changed from inline to `Arc<tokio::sync::Mutex<()>>` for sharing between `HaKuzuInner` and follower behavior.

Files: `src/follower_behavior.rs`, `src/database.rs`

### Dependency rename
`hadb-s3` → `hadb-lease-s3` (upstream crate rename).

137 tests pass (116 lib + 7 ha_database integration + 14 replay/replicator integration).

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
