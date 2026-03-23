# hakuzu

HA Kuzu/LadybugDB with one line of code. Leader election, journal replication, write forwarding — just your app + an S3 bucket.

hakuzu is to Kuzu/graph databases what [haqlite](https://github.com/russellromney/haqlite) is to SQLite — the HA layer that wraps [graphstream](https://github.com/russellromney/graphstream)'s journal replication and [hadb](https://github.com/russellromney/hadb)'s coordination framework.

## Quick Start

```rust
use hakuzu::{HaKuzu, QueryResult};

let db = HaKuzu::builder("my-bucket")
    .prefix("myapp/")
    .secret("my-token")
    .open("/data/graph", "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))")
    .await?;

// Writes — leader executes + journals; follower auto-forwards to leader
db.execute("CREATE (p:Person {id: $id, name: $name})", Some(json!({"id": 1, "name": "Alice"}))).await?;

// Reads — always local, bounded concurrency
let result: QueryResult = db.query("MATCH (p:Person) RETURN p.id, p.name", None).await?;

// Local mode — no S3, no HA, always leader
let db = HaKuzu::local("/data/graph", "CREATE NODE TABLE IF NOT EXISTS ...")?;
```

## Architecture

- **Single-writer**: Kuzu is single-writer. `write_mutex` serializes all writes.
- **Connection-per-operation**: `lbug::Connection` is NOT Send. Created per-operation inside `spawn_blocking`, dropped within.
- **Journal replication**: Writes are journaled via graphstream `.graphj` segments, uploaded to S3, replayed by followers.
- **Leader election**: hadb Coordinator handles leader election via S3 CAS leases, automatic failover.
- **Write forwarding**: Followers auto-forward mutations to the leader via HTTP.
- **Read concurrency**: Bounded by `read_semaphore`. Reads are always local.

## Dependencies

- [hadb](https://github.com/russellromney/hadb) — Database-agnostic HA coordination
- [graphstream](https://github.com/russellromney/graphstream) — Journal replication for graph databases
- [lbug](https://github.com/Vela-Engineering/ladybug) — Rust bindings for LadybugDB/Kuzu

## Development

```bash
# Build (requires Homebrew LLVM on macOS)
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" cargo build

# Test
CC=/opt/homebrew/opt/llvm/bin/clang CXX=/opt/homebrew/opt/llvm/bin/clang++ \
  RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" cargo test

# E2E HA test
soup run -p hadb -e development -- bash tests/e2e_ha.sh
```

## License

Apache-2.0
