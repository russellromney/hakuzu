//! Benchmark: measure lbug::Connection creation cost.
//!
//! If <50us, no pool needed. If >100us, investigate thread_local! caching.
//!
//! Usage: cargo build --release --example connection_cost && ./target/release/examples/connection_cost

use std::sync::Arc;
use std::time::Instant;

fn main() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("conn_bench");
    let db = Arc::new(lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap());

    // Schema setup
    {
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query("CREATE NODE TABLE IF NOT EXISTS T(id INT64, PRIMARY KEY(id))")
            .unwrap();
    }

    // Warm up
    for _ in 0..100 {
        let _ = lbug::Connection::new(&db).unwrap();
    }

    // Benchmark
    let n = 10_000;
    let mut durations = Vec::with_capacity(n);

    for _ in 0..n {
        let start = Instant::now();
        let _conn = lbug::Connection::new(&db).unwrap();
        durations.push(start.elapsed());
    }

    durations.sort();

    let total: std::time::Duration = durations.iter().sum();
    let avg = total / n as u32;
    let p50 = durations[n / 2];
    let p95 = durations[n * 95 / 100];
    let p99 = durations[n * 99 / 100];

    println!("lbug::Connection creation cost ({n} iterations):");
    println!("  avg:  {:>8?}", avg);
    println!("  p50:  {:>8?}", p50);
    println!("  p95:  {:>8?}", p95);
    println!("  p99:  {:>8?}", p99);
    println!();

    if avg.as_micros() < 50 {
        println!("Result: <50us avg — connection pool NOT needed.");
    } else if avg.as_micros() < 100 {
        println!("Result: 50-100us avg — borderline, monitor under load.");
    } else {
        println!("Result: >100us avg — consider thread_local! connection caching.");
    }
}
