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

## Phase 2: HaKuzu Production Library API

HaKuzu needs to be what HaQLite is for SQLite — embed in one line, get HA automatically. Currently all HA logic is manual wiring in the 490-line ha_experiment.rs binary. The library API encapsulates everything.

### Target API

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

- `src/mutation.rs` (~55 lines) — `is_mutation()` keyword check for auto-routing reads vs writes. Copied from graphd-engine's proven implementation.
- `src/values.rs` (~80 lines) — `lbug_to_json()`, `json_to_lbug()`, `json_params_to_graphstream()` value conversions. Pattern from graphd-engine's values.rs.
- `src/forwarding.rs` (~120 lines) — `ForwardedExecute`, `ExecuteResult`, Bearer auth, `POST /hakuzu/execute` handler. Pattern from haqlite's forwarding.rs.
- `src/database.rs` (~450 lines) — `HaKuzuBuilder`, `HaKuzu`, `HaKuzuInner`. Builder → open → Coordinator + KuzuReplicator + forwarding server + role listener. Pattern from haqlite's database.rs + graphd-engine's engine.rs.

### Key design (from graphd-engine)

- **Connection-per-operation**: `lbug::Connection` is NOT Send. Created in `spawn_blocking`, dropped within.
- **write_mutex**: `tokio::sync::Mutex<()>` serializes writes (Kuzu single-writer).
- **read_semaphore**: `tokio::sync::Semaphore` bounds concurrent reads.
- **snapshot_lock**: `Arc<RwLock<()>>` — read for queries, write for CHECKPOINT.
- **Journal on success**: After successful write, send `PendingEntry` to graphstream `JournalSender`.

### Comprehensive test plan (`tests/ha_database.rs`)

Unit-level tests:
- `is_mutation()` — verify CREATE, MERGE, DELETE, SET, DROP, ALTER, COPY, MATCH, RETURN, EXPLAIN, PROFILE, CALL, UNWIND
- `lbug_to_json()` — Null, Bool, Int64, Double, String, List, Date, Timestamp, Node, Rel
- `json_to_lbug()` — round-trip primitives, invalid types error
- `json_params_to_graphstream()` — JSON object to `Vec<(String, ParamValue)>`

Integration tests:
1. **single_node_local_mode** — `HaKuzu::local()`, execute schema + writes, query results, close
2. **single_node_execute_and_query** — `from_coordinator`, 5 writes, query count, verify all present
3. **two_node_forwarded_write** — leader + follower, write through follower, verify data reaches leader
4. **forwarding_error_no_leader** — follower with unreachable leader address, execute returns error
5. **close_is_clean** — write, close, reopen same path, verify data persists
6. **auth_rejects_wrong_secret** — wrong Bearer token, forwarded write rejected (401)
7. **auth_accepts_correct_secret** — matching Bearer token, forwarded write succeeds

E2E regression:
- `tests/e2e_ha.sh` — all 7 existing HA scenarios continue passing (leader election, failover, recovery, journal replay, multi-node convergence, graceful handoff, writer client)

### Review checklist

- [ ] All `is_mutation()` keywords match graphd-engine's proven list
- [ ] Connection created + dropped inside `spawn_blocking` (never held across await)
- [ ] write_mutex acquired before every mutation (no data races)
- [ ] JournalSender populated on Promoted, cleared on Demoted/Fenced/Sleeping
- [ ] Forwarding handler checks role == Leader before executing
- [ ] Bearer auth checked on all forwarding endpoints
- [ ] `close()` drains coordinator, shuts down replicator, joins all tasks
- [ ] QueryResult serializes all lbug types correctly (especially Node/Rel)
- [ ] No panics in public API — all errors returned as `Result`
- [ ] E2E scenarios still pass after library changes
