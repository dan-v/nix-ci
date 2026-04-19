//! HTTP server: axum router, handlers, SSE. One AppState shared.

pub mod app;
pub mod build_logs;
pub mod claim;
pub mod complete;
pub mod events;
pub mod heartbeat;
pub mod ingest_batch;
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
    let pool = durable::connect_and_migrate(&cfg.database_url, cfg.pg_statement_timeout_ms).await?;

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
        draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        fenced_workers: Arc::new(parking_lot::RwLock::new(std::collections::HashSet::new())),
        worker_health: crate::dispatch::WorkerHealth::new(),
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
    // Terminal-writeback retry sweep: a `/complete` that crossed the
    // last-drv-of-job boundary while PG was unreachable leaves the
    // submission with all members finished but `terminal=false` and no
    // `jobs.result` row. The runner's SSE would otherwise wait forever.
    // Ticks at the reaper interval (same cadence as the other recovery
    // sweeps) and re-invokes `check_and_publish_terminal` on any wedged
    // submission. Idempotent against concurrent live `/complete` calls
    // via `mark_terminal` CAS and `done_at IS NULL` guard.
    let terminal_retry_task = tokio::spawn(run_terminal_writeback_retry(
        state.clone(),
        Duration::from_secs(cfg.reaper_interval_secs),
        shutdown_rx.clone(),
    ));

    let app = build_router(state);
    let listener = TcpListener::bind(cfg.listen).await?;
    tracing::info!(addr = %cfg.listen, "nix-ci coordinator listening");

    // Serve with an OS-signal-driven bounded drain:
    //   1. axum serves until shutdown_rx flips true.
    //   2. `wait_for_signal` awaits SIGTERM/Ctrl-C and flips the watch.
    //   3. After that, axum starts its own graceful drain — which is
    //      unbounded by default. SSE subscribers on non-terminalizing
    //      jobs and claim long-polls can pin it indefinitely.
    //   4. We bound that drain at `graceful_shutdown_secs`; if it
    //      overruns we abort the serve task so the process can exit
    //      and release the advisory lock to a standby.
    let serve_task = {
        let mut serve_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    // Resolve as soon as the watch holds `true`.
                    while !*serve_shutdown.borrow() {
                        if serve_shutdown.changed().await.is_err() {
                            return;
                        }
                    }
                })
                .await
        })
    };

    wait_for_signal().await;
    // Kick the watch so bg tasks + the serve task's graceful_shutdown
    // fire concurrently.
    let _ = shutdown_tx.send(true);

    let drain_budget = Duration::from_secs(cfg.graceful_shutdown_secs);
    let serve_abort = serve_task.abort_handle();
    match tokio::time::timeout(drain_budget, serve_task).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => {
            return Err(crate::Error::Internal(format!("axum serve: {e}")));
        }
        Ok(Err(join_err)) => {
            tracing::warn!(error = %join_err, "axum serve task joined with error");
        }
        Err(_) => {
            tracing::warn!(
                timeout_secs = drain_budget.as_secs(),
                "axum graceful drain exceeded limit; aborting in-flight connections"
            );
            serve_abort.abort();
        }
    }

    drain_background_tasks(
        [
            ("reaper", reaper_task),
            ("cleanup", cleanup_task),
            ("terminal_writeback_retry", terminal_retry_task),
        ],
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

/// Periodic sweep that finds sealed submissions whose terminal writeback
/// failed mid-`/complete` (e.g. transient PG outage) and retries.
/// Without this loop a submission with all members finished but a
/// failed terminal-persist stays in memory forever — workers and the
/// runner's SSE are stuck waiting for a `JobDone` event that never
/// fires, and the CI run eventually times out externally, flipping a
/// successful build to `cancelled`.
async fn run_terminal_writeback_retry(
    state: AppState,
    tick: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // first fire is immediate — skip
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => {
                tracing::info!("terminal_writeback_retry: shutdown");
                return;
            }
        }
        let finalized =
            crate::server::complete::retry_pending_terminal_writebacks(&state).await;
        if finalized > 0 {
            tracing::warn!(
                finalized,
                "terminal_writeback_retry: recovered submissions whose terminal writeback had previously failed"
            );
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
        let start = tokio::time::Instant::now();
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
        let start = tokio::time::Instant::now();
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

/// Integration tests for the bounded-shutdown pattern used by `run()`.
///
/// The contract under test: when SIGTERM flips the shutdown watch,
/// `axum::serve(...).with_graceful_shutdown(...)` enters its own
/// drain, but that drain is unbounded by default — an SSE subscriber
/// on a non-terminalizing job or a client holding a never-ending
/// response body will pin the serve task indefinitely. Without the
/// surrounding `tokio::time::timeout`, the process cannot exit,
/// systemd eventually SIGKILLs it, and the Postgres advisory lock
/// dangles until the connection dies.
///
/// These tests mirror the exact pattern in `run()` (spawn serve +
/// abort_handle + timeout-bounded join) on a minimal axum app — if
/// the pattern works here, it works in production.
#[cfg(test)]
mod bounded_serve_tests {
    use axum::routing::get;
    use axum::Router;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::watch;

    /// Serve with a handler that never responds; a client connects and
    /// pins the connection. Shutdown watch flips → axum starts graceful
    /// drain → the pinned connection would otherwise block forever.
    /// Our bounded timeout around the serve task must abort within the
    /// budget.
    #[tokio::test]
    async fn pinned_connection_is_aborted_within_drain_budget() {
        async fn forever() -> &'static str {
            std::future::pending::<()>().await;
            unreachable!()
        }

        let app = Router::new().route("/stuck", get(forever));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Exact pattern from run().
        let serve_task = {
            let mut rx = shutdown_rx.clone();
            tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        while !*rx.borrow() {
                            if rx.changed().await.is_err() {
                                return;
                            }
                        }
                    })
                    .await
            })
        };

        // Pin a client connection to the stuck handler. We fire a GET
        // and don't await its body — just having the TCP connection
        // open is enough to stall axum's graceful drain, which is the
        // regression we're guarding against.
        let pinned = tokio::spawn(async move {
            // reqwest is already a workspace dep (client crate); reuse
            // to avoid pulling another dep just for this test.
            let _ = reqwest::Client::new()
                .get(format!("http://{addr}/stuck"))
                .timeout(Duration::from_secs(10))
                .send()
                .await;
        });

        // Give the connect handshake a moment to land so axum knows
        // there's an in-flight request when we trigger shutdown.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Trigger shutdown.
        let _ = shutdown_tx.send(true);

        let drain_budget = Duration::from_millis(300);
        let serve_abort = serve_task.abort_handle();
        let start = tokio::time::Instant::now();
        match tokio::time::timeout(drain_budget, serve_task).await {
            Ok(_) => panic!(
                "serve should not drain cleanly with a pinned connection; \
                 the unbounded drain would hang forever without our timeout"
            ),
            Err(_) => {
                serve_abort.abort();
            }
        }
        let elapsed = start.elapsed();

        // Budget + small overshoot. The point is: bounded, not unbounded.
        assert!(
            elapsed < drain_budget + Duration::from_millis(500),
            "bounded drain took {elapsed:?}; expected close to {drain_budget:?}"
        );

        pinned.abort();
    }

    /// Positive case: when there's no pinned connection, the bounded
    /// drain completes cleanly well under budget. Guards against a
    /// bug where the timeout always fires (e.g. we forgot to pass
    /// the shutdown signal through to axum, so it never starts
    /// draining).
    #[tokio::test]
    async fn clean_drain_completes_fast_when_no_pinned_connection() {
        async fn ok() -> &'static str {
            "ok"
        }
        let app = Router::new().route("/ok", get(ok));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let serve_task = {
            let mut rx = shutdown_rx.clone();
            tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        while !*rx.borrow() {
                            if rx.changed().await.is_err() {
                                return;
                            }
                        }
                    })
                    .await
            })
        };

        // No client connected. Trigger shutdown immediately.
        let _ = shutdown_tx.send(true);
        let drain_budget = Duration::from_secs(5);
        let start = tokio::time::Instant::now();
        let result = tokio::time::timeout(drain_budget, serve_task).await;
        let elapsed = start.elapsed();
        assert!(result.is_ok(), "clean drain must not hit the timeout");
        assert!(
            elapsed < Duration::from_millis(500),
            "clean drain should complete near-instantly; took {elapsed:?}"
        );
    }
}
