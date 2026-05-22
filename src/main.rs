mod dedup;
mod ws_worker;

use clap::Parser;
use dedup::Dedup;
use std::io::{self, Write};
use std::sync::Arc;
use url::Url;
use ws_worker::{HeartbeatConfig, run_worker};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// WebSocket URL to connect to
    #[arg(short, long)]
    url: String,

    /// Number of concurrent connections to open
    #[arg(short, long, default_value_t = 10)]
    count: usize,

    /// Optional subscription payload to send upon connection
    #[arg(short, long)]
    subscribe_payload: Option<String>,

    /// Text heartbeat payload to send periodically, e.g. PING
    #[arg(long)]
    heartbeat_text: Option<String>,

    /// Heartbeat interval in seconds
    #[arg(long, default_value_t = 10)]
    heartbeat_secs: u64,

    /// Dedup cache size as a power of two (e.g. 16 means 65,536 slots)
    #[arg(long, default_value_t = 16)]
    dedup_power: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let target_url: Url = args.url.parse()?;
    if target_url.scheme() != "ws" && target_url.scheme() != "wss" {
        return Err("URL scheme must be ws or wss".into());
    }

    let heartbeat = args.heartbeat_text.map(|text| HeartbeatConfig {
        text,
        interval: std::time::Duration::from_secs(args.heartbeat_secs),
    });

    let dedup = Arc::new(Dedup::new(args.dedup_power));
    let (tx, rx) = flume::unbounded::<Vec<u8>>();

    tokio::spawn(async move {
        while let Ok(msg) = rx.recv_async().await {
            let mut stdout = io::stdout().lock();
            // Write to stdout (or could be extended to a Unix socket)
            if let Ok(text) = std::str::from_utf8(&msg) {
                let _ = writeln!(stdout, "{}", text);
            } else {
                let _ = writeln!(stdout, "<binary payload {} bytes>", msg.len());
            }
        }
    });

    // Spawn workers
    for i in 0..args.count {
        let dedup = dedup.clone();
        let tx = tx.clone();
        let url = target_url.clone();
        let sub = args.subscribe_payload.clone();
        let heartbeat = heartbeat.clone();
        tokio::spawn(async move {
            run_worker(i, url, sub, heartbeat, dedup, tx).await;
        });
    }

    // Keep main thread alive
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    Ok(())
}
