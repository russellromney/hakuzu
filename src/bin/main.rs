//! hakuzu CLI -- HA Kuzu/LadybugDB server and management tools.
//!
//! ```text
//! hakuzu serve    # Run HA graph server
//! hakuzu explain  # Show resolved config
//! ```

use std::process::ExitCode;

use anyhow::Result;
use async_trait::async_trait;

use hadb_cli::{CliBackend, SharedConfig};
use hakuzu::cli_config::HakuzuConfig;

struct HakuzuBackend;

#[async_trait]
impl CliBackend for HakuzuBackend {
    type Config = HakuzuConfig;

    fn product_name(&self) -> &'static str {
        "hakuzu"
    }

    fn config_filename(&self) -> &'static str {
        "hakuzu.toml"
    }

    async fn serve(&self, shared: &SharedConfig, product: &HakuzuConfig) -> Result<()> {
        let serve_config = product
            .serve
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing [serve] section in config"))?;
        hakuzu::serve::run(shared, serve_config).await
    }

    async fn explain(&self, shared: &SharedConfig, product: &HakuzuConfig) -> Result<()> {
        println!("=== hakuzu Configuration ===\n");
        println!("S3:");
        println!("  Bucket:    {}", shared.s3.bucket);
        println!(
            "  Endpoint:  {}",
            shared.s3.endpoint.as_deref().unwrap_or("(default)")
        );
        println!("\nLease:");
        println!("  TTL:       {}s", shared.lease.ttl_secs);
        println!("  Renew:     {}ms", shared.lease.renew_interval_ms);
        println!("  Poll:      {}ms", shared.lease.poll_interval_ms);
        println!("\nRetention:");
        println!("  Keep:      {} snapshots", shared.retention.keep);
        if let Some(serve) = &product.serve {
            println!("\nServe:");
            println!("  DB path:          {}", serve.db_path.display());
            println!("  Port:             {}", serve.port);
            println!("  Forwarding port:  {}", serve.forwarding_port);
            println!("  Prefix:           {}", serve.prefix);
            println!("  Mode:             {}", serve.mode);
            println!("  Durability:       {}", serve.durability);
            println!("  Sync interval:    {}ms", serve.sync_interval_ms);
            println!("  Follower pull:    {}ms", serve.follower_pull_ms);
            println!("  Snapshot interval: {}s", serve.snapshot_interval_secs);
            println!("  Snapshot every N:  {}", serve.snapshot_every_n);
            if serve.secret.is_some() {
                println!("  Secret:           (set)");
            }
        } else {
            println!("\nServe: (not configured)");
        }
        Ok(())
    }
}

fn main() -> ExitCode {
    hadb_cli::run_cli(HakuzuBackend)
}
