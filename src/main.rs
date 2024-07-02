use anyhow::anyhow;
use clap::Parser;
use payments::Engine;
use storage::{AccountsDal, InMemoryAccountLedger, InMemoryTxLedger};
use tokio::fs::File;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub mod account;
pub mod error;
pub mod payments;
pub mod storage;

#[derive(Parser, Debug)]
pub struct Args {
    pub input: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let file = File::open(args.input)
        .await
        .map_err(|err| anyhow!("Error while opening file: {err}"))?;
    let mut engine = Engine::new(
        InMemoryAccountLedger::default(),
        InMemoryTxLedger::default(),
    );
    engine.handle_txs(file).await?;

    println!("client,available,held,total,locked");
    for account in engine.accounts().await.values() {
        let inner = account.lock().await;
        println!(
            "{},{},{},{},{}",
            inner.client_id(),
            inner.available() as f64 / 10000f64,
            inner.held() as f64 / 10000f64,
            inner.total() as f64 / 10000f64,
            inner.is_locked()
        );
    }

    Ok(())
}
