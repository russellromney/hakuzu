# hakuzu Roadmap

## Phase GraphRedline: Lease backend selection for `hakuzu serve`

> After: Phase GraphFjord

`HaKuzuBuilder::open()` requires `.lease_store(...)` (no default — silently
picking S3LeaseStore was unsafe, since Tigris and most S3-compatible
backends do not enforce atomic conditional PUTs and split-brain the
lease). Library embedders inject the right store directly, but
`hakuzu serve` (the standalone CLI) currently bails because the
`[lease]` config section only carries timing knobs — no backend choice.

This phase wires up backend dispatch.

### a. Config schema

- [ ] Add `[lease]` `backend` field to the SharedConfig CLI schema
  (`hadb-cli`). Variants: `"nats-kv"`, `"cinch"`, `"s3"`, `"in-memory"`.
- [ ] Backend-specific subsection (e.g. `[lease.nats] url = "..."`,
  `[lease.cinch] endpoint = "..."`, `[lease.s3] bucket = "..."`). Reject
  config that sets timing knobs but no backend.

### b. Construction in `serve::resolve_lease_store`

- [ ] `nats-kv` → `hadb_lease_nats::NatsKvLeaseStore::new(...)`
- [ ] `cinch` → `hadb_lease_cinch::CinchLeaseStore::new(...)` (use the
  same admin-key auth path haqlite uses)
- [ ] `s3` → `hadb_lease_s3::S3LeaseStore::new(...)` **only when the
  endpoint is unset or explicitly tagged as AWS** — refuse otherwise
  with the same Tigris-incompatible error
- [ ] `in-memory` → `hadb::InMemoryLeaseStore::new()`, gated behind a
  dev-mode flag so it can't accidentally ship to prod

### c. Tests

- [ ] Each backend variant: parse config → construct → bind to a
  HaKuzu and run a one-node smoke
- [ ] Reject config: `s3` + custom endpoint → loud error with the
  Tigris-incompatibility note
- [ ] Reject config: timing knobs without a backend → error

### d. Documentation

- [ ] README.md: `[lease]` config block reference, with a callout that
  `s3` is AWS-only and Tigris/MinIO/RustFS users must pick `nats-kv`
- [ ] Migration note: pre-Phase-GraphFjord users who relied on the
  silent S3 default need to add `[lease] backend = "s3"` (AWS) or
  switch to a safe backend

---

## Phase GraphFjord: Track hadb 0.4 final form

> After: Phase GraphParity

hadb shipped Phase Anvil i + Phase Fjord + Phase Driftwood + Phase Shoal,
which finalized the lease/coordinator wiring. hakuzu still calls the old
APIs and no longer compiles against hadb 0.4. This phase mirrors haqlite's
matching adapt — see haqlite commits `00eaaeb` (Fjord), `8a418d7`
(Driftwood timing-knob fix), and `5de12dd` (Driftwood HaNode re-export
drop). hakuzu does not host its own SQLite-style page-storage adapter, so
it does **not** need an `AtomicFence` wiring (the journal/manifest path
gets ordering from CAS at the manifest level — see CLAUDE.md
"Architecture Invariants").

### a. LeaseConfig owns the store

- [ ] `LeaseConfig::new` is now `(Arc<dyn LeaseStore>, instance_id, address)` — pass the lease store at construction
- [ ] `Coordinator::new` drops the separate `Option<Arc<dyn LeaseStore>>` positional (now in `config.lease`); 7 args → 6 args
- [ ] Update `src/builder.rs`, `src/serve.rs`, `src/bin/ha_experiment.rs`, and every `tests/*.rs` call site

### b. Builder preserves caller-provided LeaseConfig timing

- [ ] Stop overwriting `config.lease` unconditionally. Mirror haqlite `src/database.rs` line 440-463: take the existing LeaseConfig if any, patch in store/id/address; only build a fresh one when the caller didn't supply one
- [ ] Forward CLI lease timing knobs (ttl_secs, renew_interval, follower_poll_interval) to the resolved LeaseConfig when set

### c. Version bump + clean re-export of LeaseData

- [ ] `Cargo.toml`: hakuzu 0.3.0 → 0.4.0
- [ ] `src/lib.rs`: confirm `LeaseData` re-export points at the canonical hadb-lease shape (Phase Shoal). It is already re-exported via `hadb`; nothing else to do unless we expose it under our own module path

### d. Tests must continue to verify HA semantics

- [ ] Migrate test helpers in tests/thermopylae.rs, tests/rubicon.rs, tests/ha_database.rs to the new construction APIs
- [ ] `cargo test --workspace` green, including the chaos tests in `thermopylae.rs` (kill-leader, rapid restart, concurrent writers, handoff under load) — these tests must continue to exercise the real lease/coordinator behavior, not stub it out

---

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

**Status:** a/b/d done. c deferred until GraphZenith.

### Remaining: Shared mode (c)
- [ ] `Shared+Synchronous`: lease-per-write `execute_shared()`. Acquire lease, catch up from turbograph manifest, execute, sync to S3, release
- [ ] Tests: two nodes alternating writes, both see all data

---

## Phase GraphThermopylae: Chaos Tests

Prove every mode combination works under failure. Port haqlite's Thermopylae test patterns.

**Status:** Dedicated+Replicated done (9 tests). Synchronous/Shared mode deferred until extensions available.

### Done (Ded+Rep, in-memory backends)
- Kill-leader/follower-promotes with data verification
- Rapid restart cycles (5 cycles, 50 nodes)
- Close/reopen different leader, data visible
- Concurrent readers + writers (100 writes, 200 reads)
- Handoff under active writes
- Close with concurrent reads (10 reader tasks)
- Metrics consistency through lifecycle
- Stress: 200 writes + 5 concurrent readers
- Schema DDL survives restart

### Remaining (needs turbograph extension or Shared mode)
- [ ] RPO measurement (Synchronous=0, Replicated=sync_interval)
- [ ] SIGKILL during sync (separate binary, process-level control)
- [ ] Linearizability (Shared mode: last-writer-wins, no lost updates)
- [ ] Network partition / split brain simulation
- [ ] Scale: 5 min sustained writes, 10K nodes

---

## Phase Summit: CLI Serve + Full Parity

Production-ready standalone hakuzu server.

**Status:** Done.

- `hakuzu serve` via hadb-cli framework (serve, explain, restore, list, verify, compact, replicate, snapshot)
- Prometheus metrics via GET /metrics (exposition format)
- Mode/durability config via TOML ([serve] mode/durability) + env vars (HAKUZU_MODE, HAKUZU_DURABILITY)
- Health/status/metrics HTTP endpoints (GET /health, /status, /metrics)
- Cypher HTTP API (POST /cypher with JSON body)
- Bearer token auth on all endpoints except /health
- SIGTERM graceful shutdown (drain writes, close HaKuzu)

---

## Phase GraphParity: Semantic alignment with haqlite

> After: Phase Summit, Phase GraphBridge

Cross-stack consistency fixes found during semantic review of hakuzu vs haqlite.

### hadb changes (upstream)
- [ ] Move `validate_mode_durability()` to hadb (currently hakuzu-only, should be shared)

### hakuzu changes
- [x] Use `meta()` before `get()` in TurbographFollowerBehavior (cheap poll, full fetch on version change only)
- [x] CAS conflict in sync() should error, not warn (fencing violation in Dedicated mode)

### haqlite changes
- [ ] Add ManifestChanged fast-path wakeup for turbolite Synchronous mode (hakuzu has this, haqlite doesn't)

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
