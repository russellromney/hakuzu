# hakuzu Roadmap

## Phase Parity: Delegate Readiness to hadb Coordinator (DONE)

Deleted local caught_up/replay_position from KuzuFollowerBehavior, repointed to coordinator-owned atomics via JoinResult. Deleted readiness_state() method. Atomic with hadb Phase Beacon and haqlite Phase Rampart-e.

## Phase Drain: Synchronous Upload Ack (DONE)

KuzuReplicator::sync() now uses UploadWithAck and awaits the oneshot response. Upload errors propagate instead of silent fire-and-forget. HaMetrics follower_caught_up/replay_position gauges wired.

---

## Phase Cascade: ObjectStore Migration (after graphstream Phase Aether)

> After: graphstream Phase Aether · Before: (none)

graphstream Phase Aether changes `download_new_segments` from `(&aws_sdk_s3::Client, &str, ...)` to `(&dyn ObjectStore, ...)`. This breaks hakuzu's `KuzuFollowerBehavior` which calls the function directly (`src/follower_behavior.rs`). hakuzu must update to pass an ObjectStore instead of a raw S3 client.

### Cascade-a: Update KuzuFollowerBehavior to use ObjectStore

- Replace `s3_client: aws_sdk_s3::Client` + `bucket: String` fields with `object_store: Arc<dyn hadb_io::ObjectStore>` in KuzuFollowerBehavior struct
- Update `new()` constructor to accept `Arc<dyn ObjectStore>` instead of `(aws_sdk_s3::Client, String)`
- Update all `graphstream::download_new_segments` calls to pass `&*self.object_store` instead of `(&self.s3_client, &self.bucket, ...)`
- Same for `catchup_on_promotion`

Source: graphstream Phase Aether (new `download_new_segments` signature)

### Cascade-b: Update HaKuzuBuilder to construct ObjectStore

- In `open()` (`src/database.rs`), build `Arc<dyn ObjectStore>` from S3 config instead of raw `aws_sdk_s3::Client`
- Pass to `KuzuFollowerBehavior::new(object_store)` instead of `(client, bucket)`
- Remove direct `aws-sdk-s3` and `aws-config` from Cargo.toml if no longer used directly

Source: `hadb-io/src/s3.rs` (S3Backend constructor), `walrust/src/lib.rs` (reference pattern from Phase 1b)

### Cascade-c: Tests

- All existing 121 tests pass unchanged (behavioral no-op)
- Verify hakuzu builds with graphstream's new ObjectStore-based API

### Implementation context for a new session

**Ecosystem context:** hakuzu is HA Kuzu/LadybugDB (graph database). It depends on graphstream for journal replication and hadb for coordination. KuzuFollowerBehavior downloads journal segments from S3 and replays them against a local Kuzu database.

**Prerequisite:** graphstream Phase Aether MUST be complete first. Aether changes the function signatures that hakuzu calls. If Aether isn't done, this phase has nothing to update against.

**Current state:** 121 tests passing. Phase Parity and Drain are done. KuzuFollowerBehavior currently has `s3_client: aws_sdk_s3::Client` and `bucket: String` fields.

**What changes:** After Aether, graphstream's `download_new_segments()` takes `&dyn ObjectStore` instead of `&aws_sdk_s3::Client` + `&str` bucket. hakuzu creates an `S3Backend` (from hadb-io) and passes it as the ObjectStore. The follower behavior becomes storage-backend-agnostic.

**Build commands:**
```bash
cd ~/Documents/Github/hakuzu
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" ~/.cargo/bin/cargo test --lib --test ha_database
```

**Key files to read:**
- `hakuzu/src/follower_behavior.rs` -- KuzuFollowerBehavior with current s3_client field
- `hakuzu/src/database.rs` -- HaKuzuBuilder::open() where S3 client is constructed
- `graphstream/src/sync.rs` -- download_new_segments (after Aether changes)
- `hadb-io/src/s3.rs` -- S3Backend::new(client, bucket)

---

## Known Limitations (documented, accepted)

- **No schema migration story** -- Schema is a string at `open()`. Adding a node table to a running cluster requires coordinated restart. Solving this (ALTER TABLE journaling, rolling schema changes) is a major feature. Most embedded DB users handle this at the application level.
- **String-based rewriter** -- Works for the 4 functions it handles. An AST-based Cypher parser would be a massive dependency. Edge case: `'gen_random_uuid()' + gen_random_uuid()` in the same expression. String-literal protection handles real-world queries correctly.
- **No read-your-writes guarantee** -- Inherent to async replication. A follower forwarding a write then immediately reading locally won't see the write. Solving this requires sticky routing or causal consistency tokens.
- **Connection-per-operation** -- 714ns per connection (benchmarked). Not a bottleneck. Revisit if Kuzu connection cost increases.
- **Lease TTL observability** -- hadb has `HaMetrics` with lease counters. Needs to be surfaced through hakuzu's `prometheus_metrics()`. Small but lives in hadb layer.
