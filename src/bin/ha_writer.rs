//! HTTP writer for hakuzu HA experiment.
//!
//! Discovers the leader by querying experiment nodes for role=Leader,
//! sends Cypher writes via HTTP POST /cypher, and verifies data integrity.
//!
//! On leader death, automatically discovers the new leader and reconnects.
//!
//! Usage:
//!   hakuzu-ha-writer --nodes http://localhost:9001,http://localhost:9002

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::Parser;
use tracing::{error, info, warn};

#[derive(Parser)]
#[command(name = "hakuzu-ha-writer")]
#[command(about = "HTTP Cypher writer for hakuzu HA experiment")]
struct Args {
    /// Comma-separated app server addresses
    #[arg(long)]
    nodes: String,

    /// Write interval in milliseconds
    #[arg(long, default_value = "100")]
    interval_ms: u64,

    /// Total writes to perform before stopping (0 = unlimited)
    #[arg(long, default_value = "0")]
    total_writes: u64,

    /// Verify data integrity on specified nodes after completion.
    /// Comma-separated app server addresses (e.g., "http://localhost:9001,http://localhost:9002").
    /// If not set, verifies on --nodes.
    #[arg(long)]
    verify_nodes: Option<String>,

    /// Enable benchmark mode: print tab-separated summary
    #[arg(long)]
    bench: bool,
}

/// Find the leader node by querying /status for role=Leader.
async fn find_leader(http: &reqwest::Client, nodes: &[&str]) -> Option<String> {
    for node in nodes {
        let url = format!("{}/status", node);
        if let Ok(resp) = http.get(&url).send().await {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                if body.get("role").and_then(|v| v.as_str()) == Some("Leader") {
                    return Some(node.to_string());
                }
            }
        }
    }
    None
}

/// Find any healthy node (fallback if no leader found yet).
async fn find_healthy_node(http: &reqwest::Client, nodes: &[&str]) -> Option<String> {
    for node in nodes {
        let url = format!("{}/health", node);
        if let Ok(resp) = http.get(&url).send().await {
            if resp.status().is_success() {
                return Some(node.to_string());
            }
        }
    }
    None
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!("=== hakuzu HA HTTP writer ===");
    info!("Nodes: {}", args.nodes);

    let nodes: Vec<&str> = args.nodes.split(',').map(|s| s.trim()).collect();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        let _ = shutdown_tx.send(true);
    });

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    // Wait for a leader.
    let mut leader = loop {
        if let Some(node) = find_leader(&http, &nodes).await {
            info!("Connected to leader: {}", node);
            break node;
        }
        // Fallback: try any healthy node (might become leader soon).
        if let Some(node) = find_healthy_node(&http, &nodes).await {
            info!("Connected to node (awaiting leader): {}", node);
            break node;
        }
        info!("Waiting for a leader...");
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
            _ = shutdown_rx.changed() => {
                info!("Shutdown before leader found");
                return Ok(());
            }
        }
    };

    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(args.interval_ms));
    let mut writes: u64 = 0;
    let mut errors: u64 = 0;
    let mut consecutive_errors: u64 = 0;
    let start = std::time::Instant::now();

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let id = 1_000_000 + writes + 1;
                let query = format!(
                    "CREATE (:TestData {{id: {}, value: 'writer-row-{}'}})",
                    id, id
                );

                let url = format!("{}/cypher", leader);
                let body = serde_json::json!({"query": query});

                match http.post(&url).json(&body).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        writes += 1;
                        consecutive_errors = 0;
                        if writes % 100 == 0 {
                            let elapsed = start.elapsed().as_secs_f64();
                            let rate = writes as f64 / elapsed;
                            info!(
                                "{} writes ({:.0}/s), {} errors, leader={}",
                                writes, rate, errors, leader
                            );
                        }
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        errors += 1;
                        consecutive_errors += 1;

                        if consecutive_errors <= 3 || consecutive_errors % 10 == 0 {
                            warn!(
                                "Write failed: HTTP {} (consecutive #{})",
                                status, consecutive_errors
                            );
                        }

                        // On 421 Misdirected (follower) or server error: immediately find leader.
                        if status.as_u16() == 421 || status.is_server_error() {
                            if let Some(new_leader) = find_leader(&http, &nodes).await {
                                if new_leader != leader {
                                    info!("Discovered new leader: {}", new_leader);
                                    leader = new_leader;
                                    consecutive_errors = 0;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        errors += 1;
                        consecutive_errors += 1;
                        if consecutive_errors <= 3 || consecutive_errors % 10 == 0 {
                            warn!(
                                "Write failed: {} (consecutive #{})",
                                e, consecutive_errors
                            );
                        }
                        // Connection error: try to find leader on any node.
                        if let Some(new_leader) = find_leader(&http, &nodes).await {
                            if new_leader != leader {
                                info!("Discovered new leader: {}", new_leader);
                                leader = new_leader;
                                consecutive_errors = 0;
                            }
                        }
                    }
                }

                if consecutive_errors > 300 {
                    error!("Too many consecutive errors ({}), exiting", consecutive_errors);
                    break;
                }

                if args.total_writes > 0 && writes >= args.total_writes {
                    info!("Reached {} writes, stopping", args.total_writes);
                    break;
                }
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 {
        writes as f64 / elapsed
    } else {
        0.0
    };
    info!(
        "Writer done: {} writes, {} errors, {:.1}s elapsed, {:.0} writes/s",
        writes, errors, elapsed, rate
    );

    // Verify data integrity on all nodes.
    let verify_nodes_str = args
        .verify_nodes
        .as_deref()
        .unwrap_or(&args.nodes);
    let verify_nodes: Vec<&str> = verify_nodes_str.split(',').map(|s| s.trim()).collect();

    info!("Waiting 3s for replication to catch up before verification...");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let mut all_ok = true;

    for node in &verify_nodes {
        let verify_url = format!("{}/verify", node);
        match http.get(&verify_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                let ok = body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                let count = body.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
                let gaps = body.get("gap_count").and_then(|v| v.as_i64()).unwrap_or(0);

                if ok {
                    info!("VERIFY {}: OK ({} rows, 0 gaps)", node, count);
                } else {
                    error!(
                        "VERIFY {}: FAILED ({} rows, {} gaps)",
                        node, count, gaps
                    );
                    all_ok = false;
                }
            }
            Ok(resp) => {
                error!("VERIFY {}: HTTP {}", node, resp.status());
                all_ok = false;
            }
            Err(e) => {
                error!("VERIFY {}: connection error: {}", node, e);
                all_ok = false;
            }
        }

        let metrics_url = format!("{}/metrics", node);
        if let Ok(resp) = http.get(&metrics_url).send().await {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                info!("METRICS {}: {}", node, body);
            }
        }
    }

    if all_ok {
        info!("ALL NODES VERIFIED OK");
    } else {
        error!("VERIFICATION FAILED — data integrity issue detected");
        std::process::exit(1);
    }

    // Benchmark summary.
    if args.bench {
        println!("# interval_ms\twrites\terrors\telapsed_s\twrites_per_s");
        println!(
            "{}\t{}\t{}\t{:.1}\t{:.0}",
            args.interval_ms, writes, errors, elapsed, rate
        );
    }

    Ok(())
}
