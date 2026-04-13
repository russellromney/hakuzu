# hakuzu Roadmap

## Phase Rubicon: cinch-cloud Integration (Dedicated+Replicated)

First milestone. Graph databases in cinch-cloud get basic HA with journal replication to S3, leader/follower failover, and write forwarding.

### a. Builder API for external engine
- [ ] `HaKuzuBuilder::engine(Arc<graphd_engine::Engine>)`: accept an externally-created engine instead of opening a new `lbug::Database`
- [ ] When set, hakuzu uses this engine for query execution + journal writing
- [ ] cinch-cloud creates `graphd_engine::Engine` (with sandbox, FTS, connection pool), passes to hakuzu for HA wrapping

### b. Pluggable LeaseStore
- [ ] `HaKuzuBuilder::lease_store(Arc<dyn LeaseStore>)`: skip S3LeaseStore construction, use any custom store (NATS, Redis, etcd)
- [ ] Same pattern as haqlite Phase Volt-c
- [ ] Enables NATS leases from cinch-cloud's shared lease infrastructure

### c. Tests
- [ ] Two-node HA with external engine: leader writes, follower replays, failover works
- [ ] Custom lease store (in-memory) used correctly
- [ ] External engine lifecycle: hakuzu.close() drains journal and seals segment

---

## Phase GraphMeridian: Durability Modes + Shared Mode

Full mode matrix for graph databases. Depends on turbograph Phase GraphZenith (S3Primary mode).

### a. Durability enum
- [ ] `Durability { Replicated, Synchronous }` (Eventual deferred)
- [ ] `Replicated` = current behavior (graphstream journal shipping, snapshot bootstrap)
- [ ] `Synchronous` = turbograph S3Primary on every write (RPO=0)

### b. HaMode enum
- [ ] `HaMode { Dedicated, Shared }`
- [ ] `Shared+Synchronous`: lease-per-write. Acquire lease, catch up from turbograph manifest, execute Cypher, sync to S3, release lease
- [ ] Validate: `Shared+Replicated` returns error

### c. TurbographReplicator
- [ ] Implement `hadb::Replicator` for turbograph integration
- [ ] `add()` = no-op (VFS registered at init)
- [ ] `pull()` = fetch turbograph manifest from ManifestStore, apply via `turbograph_set_manifest` UDF
- [ ] `sync()` = call `turbograph_sync()` UDF
- [ ] `remove()` = no-op

### d. Dual follower catch-up paths
- [ ] Replicated: graphstream journal pull (current behavior)
- [ ] Synchronous: turbograph manifest fetch + apply via UDF

### e. Tests
- [ ] Mode matrix: all 3 valid combos (Ded+Rep, Ded+Sync, Shared+Sync)
- [ ] Write/read/failover for each mode
- [ ] Shared mode: two nodes alternating writes, both see all data

---

## Phase GraphThermopylae: Chaos Tests

Prove every mode combination works under failure. Port haqlite's Thermopylae test patterns.

- [ ] Kill-all-and-recover (per mode)
- [ ] Linearizability (Shared mode: last-writer-wins, no lost updates)
- [ ] RPO measurement (Synchronous=0, Replicated=sync_interval)
- [ ] SIGKILL during sync (10 iterations, no corruption)
- [ ] Concurrent readers during failover
- [ ] Rapid kill/restart cycles
- [ ] Network partition / split brain
- [ ] Scale tests (5 min sustained, 10K nodes)
- [ ] Journal chain integrity after double failover

---

## Phase Summit: CLI Serve + Full Parity

Production-ready standalone hakuzu server.

- [ ] `hakuzu serve` CLI command (port from `haqlite serve`)
- [ ] Prometheus metrics surface via `prometheus_metrics()`
- [ ] Mode/durability config via CLI args and env vars
- [ ] Health/status/metrics HTTP endpoints

---

## Drop ladybug-fork

> Blocked on: lbug crate publish with StatementType

hakuzu and graphstream are published to crates.io (0.2.0). hakuzu still uses `[patch.crates-io]` for lbug locally because the published lbug crate doesn't include the `StatementType` enum (upstreamed in LadybugDB/ladybug-rust#7, not yet published). Once a new lbug version ships:

1. Remove `[patch.crates-io]` override
2. `cargo publish` without `--no-verify`

---

## Multi-language SDKs (future)

> After: crates.io publish · Before: (none)

hakuzu's HA coordination (leader election, follower replay, write forwarding) is complex. Multi-language SDKs should be FFI wrappers around the Rust implementation (via PyO3, napi-rs, CGO), not native reimplementations that drift.

### Upstream priorities

For each new language binding, the upstream dependency chain is:
1. **ladybug C/C++ core** -- already stable, ships Python/Node APIs
2. **lbug (ladybug-rust)** -- Rust FFI bindings. StatementType merged. CXX-based shared enums are manually maintained; if enum count grows, consider codegen from a single typespec source (ladybug maintainer suggested tsc-py)
3. **graphstream** -- pure Rust, no language barrier
4. **hadb** -- pure Rust traits, exposed via FFI

### Language binding approach

- **Python**: PyO3 wrapping hakuzu. `HaKuzu` class with `execute()`, `query()`, `close()`. Async via `pyo3-asyncio`.
- **Node**: napi-rs wrapping hakuzu. Same API surface.
- **Go**: CGO wrapping hakuzu. Slightly more friction (CGO overhead), but the coordination logic stays in Rust.

Each binding is thin: translate language-native types to/from hakuzu's Rust API. The HA logic, follower state machine, and write forwarding all stay in Rust.

### Codegen for shared enums

When multiple language bindings exist, shared enums (StatementType, HakuzuError variants, Role) should be generated from a single source to prevent drift. Options:
- tsc-py (typespec, suggested by ladybug maintainer)
- Simple Python script reading the C++ header and emitting per-language code
- Not needed until 3+ language bindings exist

---

## Known Limitations (documented, accepted)

- **No schema migration story** -- Schema is a string at `open()`. Adding a node table to a running cluster requires coordinated restart. Solving this (ALTER TABLE journaling, rolling schema changes) is a major feature. Most embedded DB users handle this at the application level.
- **String-based rewriter** -- Works for the 4 functions it handles. An AST-based Cypher parser would be a massive dependency. Edge case: `'gen_random_uuid()' + gen_random_uuid()` in the same expression. String-literal protection handles real-world queries correctly.
- **No read-your-writes guarantee** -- Inherent to async replication. A follower forwarding a write then immediately reading locally won't see the write. Solving this requires sticky routing or causal consistency tokens.
- **Connection-per-operation** -- 714ns per connection (benchmarked). Not a bottleneck. Revisit if Kuzu connection cost increases.
- **Lease TTL observability** -- hadb has `HaMetrics` with lease counters. Needs to be surfaced through hakuzu's `prometheus_metrics()`. Small but lives in hadb layer.
