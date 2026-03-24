# hakuzu Roadmap

## Phase 10: Follower Readiness & Edge Cases

### 10.1 `is_caught_up()` / read-readiness
Followers need to signal they've replayed all available journal entries. Load balancers can't safely route reads without this. Needs sequence tracking: compare local replay seq vs last known S3 seq. Health check endpoint for LB polling.

### 10.2 Large u64 precision loss
`u64 > i64::MAX → f64` loses precision (values.rs). Verify whether this is a real issue — Kuzu uses INT64 (signed), so values > i64::MAX can't be stored anyway. If it is: map to String for out-of-range values.

### 10.3 Mutation detection for CALL
`is_mutation()` misses `CALL db.checkpoint()` and future Kuzu built-ins. Add `CALL` to keyword list, but CALL can be read-only too (`CALL db.stats()`). Need whitelist of mutating CALL targets, or default CALL to write path (safe but slower).

## Known Limitations (documented, accepted)

- **No schema migration story** — Schema is a string at `open()`. Adding a node table to a running cluster requires coordinated restart. Solving this (ALTER TABLE journaling, rolling schema changes) is a major feature. Most embedded DB users handle this at the application level.
- **String-based rewriter** — Works for the 4 functions it handles. An AST-based Cypher parser would be a massive dependency. Edge case: `'gen_random_uuid()' + gen_random_uuid()` in the same expression. String-literal protection handles real-world queries correctly.
- **No read-your-writes guarantee** — Inherent to async replication. A follower forwarding a write then immediately reading locally won't see the write. Solving this requires sticky routing or causal consistency tokens.
- **Connection-per-operation** — 714ns per connection (benchmarked). Not a bottleneck. Revisit if Kuzu connection cost increases.
- **Lease TTL observability** — hadb has `HaMetrics` with lease counters. Needs to be surfaced through hakuzu's `prometheus_metrics()`. Small but lives in hadb layer.
