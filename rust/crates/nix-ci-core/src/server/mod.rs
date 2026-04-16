//! HTTP server: axum router, handlers, SSE. One AppState shared.

pub mod app;
pub mod build_logs;
pub mod claim;
pub mod complete;
pub mod events;
pub mod heartbeat;
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
use tokio::sync::watch;

use crate::config::ServerConfig;
use crate::dispatch::Dispatcher;
use crate::durable::logs::PgLogStore;
use crate::durable::{self, CoordinatorLock};
use crate::error::Result;
use crate::observability::metrics::Metrics;

/// Run the coordinator until shutdown is signalled.
pub async fn run(cfg: ServerConfig) -> Result<()> {
    let pool = durable::connect_and_migrate(&cfg.database_url).await?;

    // Single-writer: block until we hold the advisory lock. Uses a
    // dedicated connection so Drop reliably releases the lock.
    let _lock = CoordinatorLock::acquire(&cfg.database_url, cfg.lock_key).await?;

    // Cancel any non-terminal jobs from the previous lifetime. There is
    // no graph to rebuild — the dispatcher is purely in-memory.
    durable::clear_busy(&pool).await?;

    let metrics = Metrics::new();
    let dispatcher = Dispatcher::new(metrics.clone());
    let log_store: Arc<dyn crate::durable::logs::LogStore> =
        Arc::new(PgLogStore::new(pool.clone()));

    let state = AppState {
        pool: pool.clone(),
        dispatcher: dispatcher.clone(),
        metrics: metrics.clone(),
        cfg: Arc::new(cfg.clone()),
        log_store: log_store.clone(),
    };

    // Shared shutdown signal for background loops. axum has its own
    // graceful_shutdown integration; the reaper and cleanup loops use
    // this watch to stop their tickers promptly and avoid a dangling
    // query against a pool that's about to close.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let reaper_task = tokio::spawn(durable::reaper::run(
        pool.clone(),
        dispatcher.clone(),
        Duration::from_secs(cfg.reaper_interval_secs),
        Duration::from_secs(cfg.job_heartbeat_timeout_secs),
        shutdown_rx.clone(),
    ));
    let cleanup_task = tokio::spawn(durable::cleanup::run(
        pool.clone(),
        Duration::from_secs(cfg.cleanup_interval_secs),
        cfg.retention_days,
        cfg.build_log_retention_days,
        log_store.clone(),
        metrics.clone(),
        shutdown_rx.clone(),
    ));

    let app = build_router(state);
    let listener = TcpListener::bind(cfg.listen).await?;
    tracing::info!(addr = %cfg.listen, "nix-ci coordinator listening");

    let shutdown_fut = {
        let tx = shutdown_tx.clone();
        async move {
            wait_for_signal().await;
            // Flip the watch so background loops wind down concurrently
            // with axum's graceful drain.
            let _ = tx.send(true);
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_fut)
        .await
        .map_err(|e| crate::Error::Internal(format!("axum serve: {e}")))?;

    drain_background_tasks(
        [("reaper", reaper_task), ("cleanup", cleanup_task)],
        Duration::from_secs(cfg.graceful_shutdown_secs),
    )
    .await;
    pool.close().await;

    // Releasing _lock at end of scope drops its connection, which
    // releases the pg advisory lock implicitly.
    Ok(())
}

/// Drain a set of background JoinHandles with a per-handle timeout.
/// Any handle that overruns is aborted so the stuck task drops any
/// pool connection it holds before `pool.close()` is called — without
/// this, close hangs indefinitely. Exposed for tests.
pub async fn drain_background_tasks<I, N>(handles: I, per_task_timeout: Duration)
where
    I: IntoIterator<Item = (N, tokio::task::JoinHandle<()>)>,
    N: std::fmt::Display,
{
    for (name, handle) in handles {
        let abort = handle.abort_handle();
        if tokio::time::timeout(per_task_timeout, handle)
            .await
            .is_err()
        {
            tracing::warn!(task = %name, "shutdown: task overran drain; aborting");
            abort.abort();
        }
    }
}

async fn wait_for_signal() {
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

#[cfg(test)]
mod drain_tests {
    use super::drain_background_tasks;
    use std::time::Duration;

    #[tokio::test]
    async fn graceful_exit_handle_completes_without_abort() {
        let h = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
        });
        let start = std::time::Instant::now();
        drain_background_tasks([("ok", h)], Duration::from_secs(5)).await;
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn stuck_task_is_aborted_after_timeout() {
        // `std::future::pending` never resolves — simulates a wedged
        // background task. Drain must bound its wait at the timeout
        // and abort.
        let h = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let abort_flag = h.abort_handle();
        assert!(!abort_flag.is_finished());
        let start = std::time::Instant::now();
        drain_background_tasks([("stuck", h)], Duration::from_millis(100)).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "drain must return promptly after timeout, took {elapsed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            abort_flag.is_finished(),
            "stuck task must have been aborted"
        );
    }
}
