use std::{env, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use polymarket_book_recorder::{api, recorder, store::RecorderStore};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("polymarket_book_recorder=info")),
        )
        .init();

    let port = env::var("RECORDER_PORT")
        .ok()
        .map(|value| value.parse::<u16>())
        .transpose()
        .context("RECORDER_PORT must be a valid TCP port")?
        .unwrap_or(3001);

    let database_path = env::var_os("RECORDER_DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".data/age-recorder.sqlite"));

    let store = Arc::new(RecorderStore::open(&database_path)?);
    let runtime = recorder::start(store).await?;
    let app = api::router(runtime.handle());
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;

    info!(port, database = %database_path.display(), "age recorder listening");

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
