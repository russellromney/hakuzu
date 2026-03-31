# hakuzu

> **Experimental.** hakuzu is under active development and not yet stable. APIs will change without notice.

HA Kuzu/LadybugDB with one line of code. Leader election, journal replication, write forwarding — just your app + an S3 bucket.

hakuzu is to Kuzu/graph databases what [haqlite](https://github.com/russellromney/haqlite) is to SQLite — the HA layer that wraps [graphstream](https://github.com/russellromney/graphstream)'s journal replication and [hadb](https://github.com/russellromney/hadb)'s coordination framework.

## Quick Start

```rust
use hakuzu::{HaKuzu, HakuzuError, QueryResult};

// HA mode — leader election, journal replication, write forwarding
let db = HaKuzu::builder("my-bucket")
    .prefix("myapp/")
    .secret("my-token")
    .snapshot_interval(Duration::from_secs(300))
    .open("/data/graph", "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))")
    .await?;

// Writes — leader executes + journals; follower auto-forwards to leader
db.execute("CREATE (p:Person {id: $id, name: $name})", Some(json!({"id": 1, "name": "Alice"}))).await?;

// Non-deterministic functions are rewritten automatically:
// gen_random_uuid()    → $__hakuzu_uuid_N  (UUID v4)
// current_timestamp()  → $__hakuzu_now_N   (ISO 8601 UTC)
// current_date()       → $__hakuzu_date_N  (ISO 8601 date)
// REMOVE n.prop        → SET n.prop = NULL  (Kuzu doesn't support REMOVE)

// Reads — always local, bounded concurrency
let result: QueryResult = db.query("MATCH (p:Person) RETURN p.id, p.name", None).await?;

// Structured errors for matching on failure modes
match db.execute("...", None).await {
    Err(HakuzuError::LeaderUnavailable(msg)) => { /* forwarding failed */ }
    Err(HakuzuError::NotLeader) => { /* no leader address */ }
    Err(HakuzuError::DatabaseError(msg)) => { /* Kuzu query error */ }
    Err(HakuzuError::JournalError(msg)) => { /* graphstream error */ }
    Err(HakuzuError::CoordinatorError(msg)) => { /* hadb lease error */ }
    Err(HakuzuError::EngineClosed) => { /* shutdown */ }
    Ok(result) => { /* success */ }
}

// Local mode — no S3, no HA, always leader
let db = HaKuzu::local("/data/graph", "CREATE NODE TABLE IF NOT EXISTS ...")?;
```

## Architecture

- **Single-writer**: Kuzu is single-writer. `write_mutex` serializes all writes; shared with follower replay via `Arc`.
- **Connection-per-operation**: `lbug::Connection` is NOT Send. Created per-operation inside `spawn_blocking`, dropped within.
- **Deterministic rewriter**: Non-deterministic Cypher functions rewritten to concrete parameter values before execution and journaling, ensuring followers replay identical results.
- **Journal replication**: Writes are journaled via graphstream `.graphj` segments, uploaded to S3, replayed by followers.
- **Snapshot bootstrap**: Leader periodically creates tar.zst snapshots of the Kuzu database, uploads to S3. New nodes download the latest snapshot on startup, skipping full replay.
- **Leader election**: hadb Coordinator handles leader election via S3 CAS leases, automatic failover.
- **Write forwarding**: Followers auto-forward mutations to the leader via HTTP with Bearer auth.
- **Read concurrency**: Bounded by `read_semaphore`. Reads are always local.
- **Follower replay locking**: Followers hold `write_mutex` + `snapshot_lock` during replay to prevent concurrent reads from seeing partial state.
- **Prometheus metrics**: Lock-free counters for writes, reads, forwarding, errors. `prometheus_metrics()` concatenates hadb + graphstream + hakuzu metrics.

## Dependencies

- [hadb](https://github.com/russellromney/hadb) -- HA coordination (leader election, follower readiness, pluggable LeaseStore)
- [hadb-io](https://github.com/russellromney/hadb/tree/main/hadb-io) -- ObjectStore trait, S3 backend, retry, circuit breaker
- [graphstream](https://github.com/russellromney/graphstream) -- Journal replication via ObjectStore (hadb-io migration complete)
- [lbug](https://github.com/Vela-Engineering/ladybug) -- Rust bindings for LadybugDB/Kuzu

### macOS: ladybug-fork required

The published `lbug` crate doesn't build on macOS due to antlr4/libc++ ABI issues with Homebrew LLVM. Use the ladybug-fork with a `[patch.crates-io]` override:

```toml
[patch.crates-io]
lbug = { path = "../ladybug-fork/tools/rust_api" }
```

## Development

```bash
# Build (requires Homebrew LLVM on macOS)
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" cargo build

# Test (lib + integration)
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" cargo test --lib --test ha_database

# E2E HA test
soup run -p hadb -e development -- bash tests/e2e_ha.sh
```

## Tests

176 tests: 121 lib + 19 ha_database + 14 integration + 22 real_world.

## License

Apache-2.0
