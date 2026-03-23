# hakuzu Roadmap

## Phase 1: Comprehensive Tests

### graphstream test expansion
- Writer lifecycle: `is_alive()` false after shutdown, flush with no pending writes
- Seal edge cases: seal empty segment (no writes), multiple consecutive seals, seal-then-write
- Encrypted segment round-trip: write with key, read with key, wrong key fails
- Reader edge cases: nonexistent dir, from_sequence beyond last entry, empty journal dir
- Compact edge cases: single input, compact with encryption
- ParamValue edge cases: nested lists, empty lists, extreme values (i64::MAX, NaN)
- Large entries: entries exceeding segment size trigger correct rotation
- Chain hash validation: verify hashes chain correctly across segments

### hakuzu test expansion
- Replay edge cases: empty journal returns since_seq, invalid query is skipped (warn, not fail), typed parameter binding with prepared statements
- Replicator builder: verify `with_segment_max_bytes`, `with_fsync_ms`, `with_upload_interval` apply correctly
- Replicator sync: seal current segment via `sync()`, sync with no pending data
- Replicator multi-db: add multiple databases, write to each, remove individually
- Replicator idempotency: add same name twice, remove nonexistent name
- Replicator journal access: `journal_sender`/`journal_state` return None for unknown names
- Replay across segments: write enough entries to trigger rotation, replay all from follower

## Phase 2: HA Experiment Binaries

### `hakuzu-ha-experiment` binary
E2E HA experiment modeled on haqlite's `ha_experiment.rs`. Proves leader election, journal replication, write forwarding, and failover recovery for graph databases.

- HTTP server wrapping a Kuzu DB with KuzuReplicator + hadb Coordinator
- Routes: `POST /cypher` (execute query), `GET /count` (node count), `GET /status` (role + count), `GET /verify` (data integrity), `GET /metrics` (HA metrics)
- CLI args: `--bucket`, `--prefix`, `--endpoint`, `--db`, `--instance`, `--port`, `--secret`
- Auto-forwards writes to leader when running as follower
- Graceful shutdown on SIGTERM/SIGINT

### `hakuzu-ha-writer` binary
Remote Cypher write client modeled on haqlite's `ha_writer.rs`.

- Discovers leader from S3 lease via hadb
- Sends CREATE node queries at configurable interval
- Automatic leader re-discovery on failover
- Post-test verification against multiple nodes
- CLI args: `--bucket`, `--prefix`, `--endpoint`, `--db-name`, `--interval-ms`, `--total-writes`, `--verify-nodes`

### New dependencies for binaries
- `axum` (HTTP server)
- `reqwest` (HTTP client for forwarding + writer)
- `clap` (CLI args)
- `tracing-subscriber` (logging)
