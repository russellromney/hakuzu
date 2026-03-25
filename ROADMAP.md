# hakuzu Roadmap

## Known Limitations (documented, accepted)

- **No schema migration story** — Schema is a string at `open()`. Adding a node table to a running cluster requires coordinated restart. Solving this (ALTER TABLE journaling, rolling schema changes) is a major feature. Most embedded DB users handle this at the application level.
- **String-based rewriter** — Works for the 4 functions it handles. An AST-based Cypher parser would be a massive dependency. Edge case: `'gen_random_uuid()' + gen_random_uuid()` in the same expression. String-literal protection handles real-world queries correctly.
- **No read-your-writes guarantee** — Inherent to async replication. A follower forwarding a write then immediately reading locally won't see the write. Solving this requires sticky routing or causal consistency tokens.
- **Connection-per-operation** — 714ns per connection (benchmarked). Not a bottleneck. Revisit if Kuzu connection cost increases.
- **Lease TTL observability** — hadb has `HaMetrics` with lease counters. Needs to be surfaced through hakuzu's `prometheus_metrics()`. Small but lives in hadb layer.
