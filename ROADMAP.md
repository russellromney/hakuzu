# hakuzu Roadmap

## Phase Parity: Delegate Readiness to hadb Coordinator

> After: hadb Phase Beacon (atomic) · Before: Phase Cascade

**Atomicity: Phase Parity MUST land simultaneously with hadb Phase Beacon and haqlite Phase Rampart-e.** Phase Beacon changes the `FollowerBehavior` trait signature, breaking this crate until it updates. All three repos change in one coordinated commit.

hakuzu currently owns follower readiness tracking (`caught_up: Arc<AtomicBool>`, `replay_position: Arc<AtomicU64>`) in `KuzuFollowerBehavior` and threads it into `HaKuzuInner` via `readiness_state()`. After hadb Phase Beacon pushes this into the coordinator, hakuzu can delete its own plumbing and consume readiness from the coordinator directly.

### Parity-a: Remove readiness from KuzuFollowerBehavior

- Delete `caught_up: Arc<AtomicBool>` and `replay_position: Arc<AtomicU64>` fields from `KuzuFollowerBehavior` (`src/follower_behavior.rs:35-37`)
- Delete `readiness_state()` method (`src/follower_behavior.rs:54-56`)
- **Repoint** (not delete) all `self.caught_up.store(...)` calls to use the new `caught_up` trait parameter instead (`src/follower_behavior.rs:112,116,161`). The state machine stays the same, it just writes to the coordinator-owned atomic instead of a self-owned one.
- Delete `self.replay_position.store(...)` calls (`src/follower_behavior.rs:155,243`). The coordinator's `position: Arc<AtomicU64>` (already passed as a trait parameter) serves this role. The follower behavior already writes to `position` at line 154, so `replay_position` was redundant.

Source: `hakuzu/src/follower_behavior.rs` (simplify), `hadb/hadb/src/follower.rs` (new trait signature from Phase Beacon)

### Parity-b: Replace readiness in HaKuzuInner with cached JoinResult refs

- Delete `follower_caught_up: Arc<AtomicBool>` and `follower_replay_position: Arc<AtomicU64>` from `HaKuzuInner` (`src/database.rs:354-355`)
- Delete the `readiness_state()` call in `open_with_coordinator` that threads these into the inner struct (`src/database.rs:282,325`)
- Replace with cached Arc refs from `Coordinator::join()` (which now returns `JoinResult { role, caught_up, position }` per Phase Beacon):
  ```rust
  let result = coordinator.join(&db_name, &db_path).await?;
  // Cache in HaKuzuInner for zero-overhead health checks
  inner.follower_caught_up = result.caught_up;     // Arc<AtomicBool>
  inner.follower_position = result.position;        // Arc<AtomicU64>
  ```
- `is_caught_up()` (`src/database.rs:682-687`) stays a single atomic load on the cached Arc (no coordinator lock)
- `replay_position()` (`src/database.rs:691-693`) stays a single atomic load on the cached Arc
- `prometheus_metrics()` readiness section (`src/database.rs:703-717`) unchanged in behavior

Source: `hakuzu/src/database.rs` (simplify), `hadb/hadb/src/coordinator.rs` (JoinResult from Phase Beacon)

### Parity-c: Tests

- All existing tests pass unchanged (behavioral no-op)
- Delete or simplify `test_readiness_state` if it tested the internal plumbing
- Add test: `is_caught_up` returns coordinator's value via cached Arc, not a local atomic
- Update all `coordinator.join()` call sites to destructure `JoinResult`

### Verification

```bash
cd ~/Documents/Github/hakuzu
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" ~/.cargo/bin/cargo test --lib --test ha_database
```

---

## Phase Cascade: ObjectStore Migration (after graphstream Phase Aether)

> After: graphstream Phase Aether · Before: (none)

graphstream Phase Aether changes `download_new_segments` from `(&aws_sdk_s3::Client, &str, ...)` to `(&dyn ObjectStore, ...)`. This breaks hakuzu's `KuzuFollowerBehavior` which calls the function directly (`src/follower_behavior.rs:102-108`). hakuzu must update to pass an ObjectStore instead of a raw S3 client.

### Cascade-a: Update KuzuFollowerBehavior to use ObjectStore

- Replace `s3_client: aws_sdk_s3::Client` + `bucket: String` fields with `object_store: Arc<dyn hadb_io::ObjectStore>` (`src/follower_behavior.rs:27-28`)
- Update `new()` constructor to accept `Arc<dyn ObjectStore>` instead of `(aws_sdk_s3::Client, String)`
- Update all `graphstream::download_new_segments` calls to pass `&*self.object_store` instead of `(&self.s3_client, &self.bucket, ...)`
- Same for `catchup_on_promotion` (`src/follower_behavior.rs:203-211`)

Source: graphstream Phase Aether (new `download_new_segments` signature)

### Cascade-b: Update HaKuzuBuilder to construct ObjectStore

- In `open()` (`src/database.rs:172-328`), build `Arc<dyn ObjectStore>` from S3 config instead of raw `aws_sdk_s3::Client`
- Pass to `KuzuFollowerBehavior::new(object_store)` instead of `(client, bucket)`
- Remove direct `aws-sdk-s3` and `aws-config` from Cargo.toml (comes via hadb-io)

Source: `hadb-io/src/s3.rs` (S3Backend constructor), `walrust/src/lib.rs` (reference pattern from Phase 1b)

### Cascade-c: Tests

- All existing tests pass unchanged (behavioral no-op)
- Verify hakuzu builds with graphstream's new ObjectStore-based API

### Verification

```bash
cd ~/Documents/Github/hakuzu
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" ~/.cargo/bin/cargo test --lib --test ha_database
```

---

## Known Limitations (documented, accepted)

- **No schema migration story** — Schema is a string at `open()`. Adding a node table to a running cluster requires coordinated restart. Solving this (ALTER TABLE journaling, rolling schema changes) is a major feature. Most embedded DB users handle this at the application level.
- **String-based rewriter** — Works for the 4 functions it handles. An AST-based Cypher parser would be a massive dependency. Edge case: `'gen_random_uuid()' + gen_random_uuid()` in the same expression. String-literal protection handles real-world queries correctly.
- **No read-your-writes guarantee** — Inherent to async replication. A follower forwarding a write then immediately reading locally won't see the write. Solving this requires sticky routing or causal consistency tokens.
- **Connection-per-operation** — 714ns per connection (benchmarked). Not a bottleneck. Revisit if Kuzu connection cost increases.
- **Lease TTL observability** — hadb has `HaMetrics` with lease counters. Needs to be surfaced through hakuzu's `prometheus_metrics()`. Small but lives in hadb layer.
