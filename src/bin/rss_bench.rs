//! RSS profiling bench for hakuzu.
//!
//! Opens a local HaKuzu, writes batches, queries, and idles —
//! printing stage markers that measure_rss.py correlates with RSS samples.
//!
//! Usage: cargo build --release --bin rss-bench && python3 bench/measure_rss.py

use hakuzu::HaKuzu;
use serde_json::json;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn stage(name: &str) {
    println!("STAGE: {name}");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("rss_bench_db");
    let schema = "CREATE NODE TABLE IF NOT EXISTS Bench(id INT64, data STRING, PRIMARY KEY(id))";

    stage("before_open");
    let db = HaKuzu::local(db_path.to_str().unwrap(), schema)?;
    stage("after_open");

    // Write batch 1: 1000 entries
    for i in 1..=1000 {
        db.execute(
            "CREATE (:Bench {id: $id, data: $data})",
            Some(json!({"id": i, "data": "x".repeat(100)})),
        )
        .await?;
    }
    stage("after_1000_writes");

    // Write batch 2: 5000 entries
    for i in 1001..=6000 {
        db.execute(
            "CREATE (:Bench {id: $id, data: $data})",
            Some(json!({"id": i, "data": "y".repeat(100)})),
        )
        .await?;
    }
    stage("after_6000_writes");

    // Write batch 3: 10000 more
    for i in 6001..=16000 {
        db.execute(
            "CREATE (:Bench {id: $id, data: $data})",
            Some(json!({"id": i, "data": "z".repeat(100)})),
        )
        .await?;
    }
    stage("after_16000_writes");

    // Read phase
    let result = db
        .query("MATCH (b:Bench) RETURN count(b) AS cnt", None)
        .await?;
    println!("  Count: {:?}", result.rows[0][0]);
    stage("after_query");

    // Idle for 2 seconds
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    stage("after_idle");

    db.close().await?;
    stage("after_close");

    Ok(())
}
