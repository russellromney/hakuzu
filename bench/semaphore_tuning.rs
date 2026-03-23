//! Benchmark: read semaphore tuning.
//!
//! Spawns N concurrent reads at different semaphore values (8, 16, 32, 64)
//! and measures latency distribution.
//!
//! Usage: cargo build --release --example semaphore_tuning && ./target/release/examples/semaphore_tuning

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Semaphore;

async fn bench_semaphore(
    db: Arc<lbug::Database>,
    permits: usize,
    concurrent_readers: usize,
    reads_per_reader: usize,
) -> Vec<std::time::Duration> {
    let semaphore = Arc::new(Semaphore::new(permits));
    let mut handles = Vec::new();

    for _ in 0..concurrent_readers {
        let db = db.clone();
        let sem = semaphore.clone();
        handles.push(tokio::spawn(async move {
            let mut durations = Vec::new();
            for _ in 0..reads_per_reader {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();
                let db = db.clone();
                tokio::task::spawn_blocking(move || {
                    let conn = lbug::Connection::new(&db).unwrap();
                    conn.query("MATCH (t:T) RETURN count(t)").unwrap();
                })
                .await
                .unwrap();
                durations.push(start.elapsed());
            }
            durations
        }));
    }

    let mut all_durations = Vec::new();
    for handle in handles {
        all_durations.extend(handle.await.unwrap());
    }
    all_durations.sort();
    all_durations
}

#[tokio::main]
async fn main() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("sem_bench");
    let db = Arc::new(lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap());

    // Schema + seed data
    {
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query("CREATE NODE TABLE IF NOT EXISTS T(id INT64, data STRING, PRIMARY KEY(id))")
            .unwrap();
        for i in 0..1000 {
            conn.query(&format!("CREATE (:T {{id: {i}, data: '{}'}})", "x".repeat(50)))
                .unwrap();
        }
    }

    let concurrent_readers = 32;
    let reads_per_reader = 100;

    println!(
        "Semaphore tuning: {} concurrent readers, {} reads each",
        concurrent_readers, reads_per_reader
    );
    println!();

    for permits in [8, 16, 32, 64] {
        let durations = bench_semaphore(
            db.clone(),
            permits,
            concurrent_readers,
            reads_per_reader,
        )
        .await;

        let n = durations.len();
        let total: std::time::Duration = durations.iter().sum();
        let avg = total / n as u32;
        let p50 = durations[n / 2];
        let p95 = durations[n * 95 / 100];
        let p99 = durations[n * 99 / 100];

        println!("  permits={permits:>3}:  avg={avg:>8?}  p50={p50:>8?}  p95={p95:>8?}  p99={p99:>8?}");
    }
}
