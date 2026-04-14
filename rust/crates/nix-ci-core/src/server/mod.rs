//! HTTP server: axum router, handlers, SSE. One AppState shared.

pub mod app;
pub mod claim;
pub mod complete;
pub mod events;
pub mod heartbeat;
pub mod ingest;
pub mod ingest_batch;
pub(crate) mod ingest_common;
pub mod jobs;
pub mod ops;
pub mod router;

pub use app::AppState;
pub use router::build_router;

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

use crate::config::ServerConfig;
use crate::dispatch::Dispatcher;
use crate::durable::{self, CoordinatorLock};
use crate::error::Result;
use crate::observability::metrics::Metrics;

/// Run the coordinator until shutdown is signalled.
pub async fn run(cfg: ServerConfig) -> Result<()> {
    let pool = durable::connect_and_migrate(&cfg.database_url).await?;

    // Single-writer: block until we hold the advisory lock. Uses a
    // dedicated connection so Drop reliably releases the lock.
    let _lock = CoordinatorLock::acquire(&cfg.database_url, cfg.lock_key).await?;

    // Reset in-flight state from prior lifetime.
    durable::rehydrate::clear_busy(&pool).await?;

    // Build dispatcher and rehydrate graph.
    let metrics = Metrics::new();
    let dispatcher = Dispatcher::new(metrics.clone());
    durable::rehydrate::rehydrate(&pool, &dispatcher).await?;

    let state = AppState {
        pool: pool.clone(),
        dispatcher: dispatcher.clone(),
        metrics: metrics.clone(),
        cfg: Arc::new(cfg.clone()),
    };

    // Background loops
    tokio::spawn(durable::reaper::run(
        pool.clone(),
        dispatcher.clone(),
        Duration::from_secs(cfg.reaper_interval_secs),
        Duration::from_secs(cfg.job_heartbeat_timeout_secs),
    ));
    tokio::spawn(durable::cleanup::run(
        pool.clone(),
        Duration::from_secs(cfg.cleanup_interval_secs),
        cfg.retention_days,
    ));

    let app = build_router(state);
    let listener = TcpListener::bind(cfg.listen).await?;
    tracing::info!(addr = %cfg.listen, "nix-ci coordinator listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| crate::Error::Internal(format!("axum serve: {e}")))?;

    // Releasing _lock at end of scope drops its connection, which
    // releases the pg advisory lock implicitly.
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        signal::ctrl_c().await.expect("ctrl-c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => { tracing::info!("shutdown: ctrl-c"); }
        () = terminate => { tracing::info!("shutdown: SIGTERM"); }
    }
}
