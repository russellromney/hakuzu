# hakuzu Roadmap

This is the *unshipped* work. See CHANGELOG.md for shipped phases.
Order is by `> After:` adjacency, not top-to-bottom position.

Highest-priority unblock right now: **Phase GraphRedline** — without it,
`hakuzu serve` cannot run in any production-like environment (bails
immediately because it has no way to pick a lease or storage backend
from config).

---

## Phase GraphRedline: `hakuzu serve` backend config dispatch

> After: Phase GraphFjord (shipped)

`HaKuzuBuilder::open()` now requires explicit `.lease_store(...)` and
`.storage(...)` — silently picking S3LeaseStore was unsafe (Tigris does
not enforce atomic conditional PUTs and split-brains the lease), and
silently picking S3Storage hid which bucket / endpoint bytes landed in.
Library embedders (cinch-engine) inject both directly and are
unaffected. `hakuzu serve` currently fails at startup with a pointer
to this phase, because the SharedConfig CLI schema has no way to
express which backend to use.

Close the gap.

### a. CLI config schema

- [ ] `[lease.backend]` field in `hadb-cli::SharedConfig::LeaseSection`.
  Variants: `"nats-kv"`, `"cinch"`, `"s3"`, `"in-memory"`
- [ ] `[storage.backend]` field in a new `StorageSection`. Variants:
  `"s3"`, `"cinch"`
- [ ] Backend-specific subsections (`[lease.nats] url = ...`,
  `[lease.cinch] endpoint = ... admin_key = ...`,
  `[lease.s3] bucket = ...`, `[storage.s3] bucket = ...
  endpoint = ...`)
- [ ] Reject config that sets timing knobs but no backend; reject
  mismatched subsections (e.g. `backend = "nats-kv"` with a
  `[lease.s3]` block present)

### b. `serve::resolve_lease_store` dispatch

- [ ] `nats-kv` → `hadb_lease_nats::NatsKvLeaseStore::new(...)`
- [ ] `cinch` → `hadb_lease_cinch::CinchLeaseStore::new(...)` (same
  admin-key auth path haqlite uses)
- [ ] `s3` → `hadb_lease_s3::S3LeaseStore::new(...)` **only when
  endpoint is unset (real AWS) or tagged with
  `[lease.s3] allow_non_aws = true` (explicit opt-in).** Tigris,
  MinIO, RustFS, R2 all error here.
- [ ] `in-memory` → `hadb::InMemoryLeaseStore::new()`, gated behind
  `[lease] dev_mode = true` so it can't accidentally ship to prod

### c. `serve::resolve_storage` dispatch

- [ ] `s3` → `hadb_storage_s3::S3Storage::new(...)` — reads bucket,
  endpoint, credentials from standard AWS env (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`). This is what points at
  a local RustFS in tests.
- [ ] `cinch` → `hadb_storage_cinch::CinchHttpStorage::new(...)`

### d. Tests — use RustFS running locally, not mocks

- [ ] Stand up RustFS + NATS locally (see `cinch-rustfs.internal:9000`
  pattern from the main cinch stack). Point `AWS_ENDPOINT_URL` at
  RustFS and run the `--ignored` e2e tests already in the repo
  (`tests/e2e.rs`) as part of the suite. Un-ignore them behind a feature
  or env check.
- [ ] Wire each backend variant through a one-node smoke that parses
  config → `resolve_*` → open → write → read
- [ ] Two-node smoke with `nats-kv` leases: leader claims, follower
  replays, kill leader, follower promotes
- [ ] Reject config: `backend = "s3"` + non-AWS endpoint without
  `allow_non_aws = true` → loud error mentioning Tigris
- [ ] Reject config: `backend = "in-memory"` without `dev_mode = true`
  → loud error

### e. e2e_ha.sh gate

- [ ] `tests/e2e_ha.sh` (591 lines, never run in this session) exercises
  release binaries + real object storage + kill-process failover +
  writer-reconnect. Point it at the local RustFS + NATS stack and
  confirm all 7 scenarios pass. This is the closest thing to a
  production smoke test.

### f. Documentation

- [ ] README: new `[lease]` / `[storage]` config block reference with
  a callout that `s3` is AWS-only by default and Tigris / MinIO /
  RustFS deployments need `allow_non_aws = true` (storage only — never
  valid for leases)
- [ ] "How to run locally" section documenting the RustFS + NATS local
  test harness

---

## Phase GraphGuardrail: Fence tokens on journal segment writes

> After: Phase GraphRedline

**Open design question.** `graphstream`'s uploader calls
`storage.put(key, bytes)` with no fence token. If two engines believe
they're the leader for the same scope (Fly network partition during
lease handoff) both can `put()` journal segments to the same S3
prefix. The manifest-level CAS eventually catches it, but not before
bytes land.

haqlite solves the analogous SQLite page-write problem with an
`AtomicFence` wired into `CinchHttpStorage`; every page PUT carries a
`Fence-Token` header that the server rejects when the token is stale.
For hakuzu the question is:

- Should journal segment PUTs be fenced at the storage adapter layer
  (like haqlite's pages), or is manifest-level CAS sufficient because
  segments are append-only and sequence-numbered?
- If the answer is "fence them": does the fence sit at graphstream's
  uploader (pass a `FenceSource` into `spawn_journal_uploader`) or at
  the `CinchHttpStorage` adapter (same as haqlite)?

### Tasks

- [ ] Write up the threat model: what exactly goes wrong if a stale
  leader's segment PUT lands after the new leader's first manifest
  publish? Is the manifest CAS sufficient to make the stale segment
  inert, or does it poison recovery?
- [ ] Decide: fence at uploader, fence at storage adapter, or no
  fence (manifest CAS is enough)
- [ ] If fencing is needed: thread a `FenceSource` through the
  hakuzu → graphstream → storage layer. haqlite's wiring is the
  reference
- [ ] Test: a synthetic "stale leader holds segment PUT while new
  leader takes over" scenario, assert the stale segment either fails
  to land or is recoverably ignored

---

## Phase GraphManifestWakeup: Fix dead manifest-changed wakeup in Synchronous mode

> After: Phase GraphRedline (low priority until Synchronous mode ships)

Latent bug, pre-existing, flagged during the GraphFjord review.
`src/builder.rs` creates a `manifest_wakeup: Arc<tokio::sync::Notify>`
in Synchronous mode and wires it into `TurbographFollowerBehavior`,
but passes `None` for `Coordinator::new`'s `manifest_store` arg.
Without a `ManifestStore`, the lease monitor never emits
`ManifestChanged` events, so the `Notify` is never triggered and
followers fall back to polling on the full `follower_pull_interval`
instead of waking on manifest change.

Not a correctness issue in Dedicated+Replicated (the only mode that
ships today). Becomes a footgun the moment anyone enables
Synchronous.

### Tasks

- [ ] Pass `self.manifest_store` into `Coordinator::new` instead of
  `None`, so the lease monitor's manifest poller can fire
  ManifestChanged into `self.role_tx`
- [ ] Verify the existing
  `manifest_changed_wakeup_triggers_correctly` test in
  `tests/graph_meridian.rs` actually ends-to-end: event emitted →
  listener calls `notify.notify_one()` → follower wakes. Add a
  follower-loop assertion if the current test only covers the notify
  mechanics
- [ ] Delete the `manifest_wakeup` field from
  `TurbographFollowerBehavior` if the role-event path + Coordinator
  wakeup covers it end-to-end (haqlite's Phase Lucid pattern)

---

## Phase Rubicon: External engine support for cinch-cloud

> After: Phase GraphRedline

Partial. `.lease_store()` and `.storage()` paths shipped in Phase
GraphFjord. Remaining: external lbug::Database + engine wiring so
cinch-engine can own the database lifecycle and hand hakuzu a
ready-to-use handle.

### a. Builder API for external engine
- [ ] `HaKuzuBuilder::engine(Arc<graphd_engine::Engine>)`: accept an
  externally-created engine instead of opening a new `lbug::Database`
- [ ] When set, hakuzu uses this engine for query execution + journal
  writing
- [ ] cinch-cloud creates `graphd_engine::Engine` (with sandbox, FTS,
  connection pool), passes to hakuzu for HA wrapping

### b. Tests
- [ ] Two-node HA with external engine: leader writes, follower
  replays, failover works
- [ ] External engine lifecycle: `hakuzu.close()` drains journal and
  seals segment

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
