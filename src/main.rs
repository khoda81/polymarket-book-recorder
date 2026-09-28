use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::Parser;
use polymarket_book_recorder::{
    api, fees::FeeResolver, migration_v7, recorder, store::RecorderStore,
};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "polymarket-book-recorder",
    version,
    about = "Continuously record Polymarket order-book pressure history"
)]
struct Args {
    /// SQLite recorder database.
    #[arg(long, default_value = ".data/age-recorder.sqlite")]
    database: PathBuf,

    /// HTTP API port.
    #[arg(long, default_value_t = 3001)]
    port: u16,

    /// Maximum concurrent Polymarket REST requests during v7 -> v8 migration.
    #[arg(
        long,
        default_value_t = migration_v7::DEFAULT_MIGRATION_CONCURRENCY
    )]
    migration_concurrency: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    // tokio-tungstenite intentionally leaves rustls' crypto provider
    // unselected. Install one explicitly before any TLS client is built so
    // provider choice cannot depend on transitive Cargo feature resolution.
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("rustls CryptoProvider was already installed"))?;

    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("polymarket_book_recorder=info")),
        )
        .init();

    let mut fees = FeeResolver::new();
    migration_v7::migrate_database_v7_to_v8(&args.database, &mut fees, args.migration_concurrency)
        .await?;

    let store = Arc::new(RecorderStore::open(&args.database)?);
    for market in store.load_market_fees()? {
        fees.seed(market);
    }

    let runtime = recorder::start(store, fees).await?;
    let app = api::router(runtime.handle());
    let listener = TcpListener::bind(("0.0.0.0", args.port)).await?;

    info!(port = args.port, database = %args.database.display(), "age recorder listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    runtime.shutdown().await
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate()).expect("installing SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    info!("shutdown requested");
}
