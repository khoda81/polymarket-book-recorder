use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::Parser;
use polymarket_book_recorder::{api, recorder, store::RecorderStore};
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("polymarket_book_recorder=info")),
        )
        .init();

    let store = Arc::new(RecorderStore::open(&args.database)?);

    let runtime = recorder::start(store).await?;
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
