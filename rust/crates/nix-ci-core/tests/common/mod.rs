//! Shared test helpers. Spins up the axum server in-process against a
//! `sqlx::test`-provided Postgres database, returning the base URL and
//! a shutdown handle.

use std::net::SocketAddr;
use std::sync::Arc;

use nix_ci_core::config::ServerConfig;
use nix_ci_core::dispatch::Dispatcher;
use nix_ci_core::durable;
use nix_ci_core::observability::metrics::Metrics;
use nix_ci_core::server::{build_router, AppState};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Handle returned by `spawn_server`. Drop it to shut the server down.
#[allow(dead_code)]
pub struct ServerHandle {
    pub base_url: String,
    pub pool: PgPool,
    pub dispatcher: Dispatcher,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

pub async fn spawn_server(pool: PgPool) -> ServerHandle {
    spawn_server_with_cfg(pool, |_| {}).await
}

/// Variant that lets a test tweak ServerConfig before the server starts
/// (e.g., shrink `job_heartbeat_timeout_secs` so a reaper test doesn't
/// have to wait 30s).
#[allow(dead_code)]
pub async fn spawn_server_with_cfg(
    pool: PgPool,
    mutate_cfg: impl FnOnce(&mut ServerConfig),
) -> ServerHandle {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migrations");

    // Reset in-flight state (clean slate per test).
    durable::rehydrate::clear_busy(&pool)
        .await
        .expect("clear_busy");

    let metrics = Metrics::new();
    let dispatcher = Dispatcher::new(metrics.clone());
    durable::rehydrate::rehydrate(&pool, &dispatcher)
        .await
        .expect("rehydrate");

    let mut cfg = ServerConfig {
        database_url: String::new(),
        listen: "127.0.0.1:0".parse().unwrap(),
        ..ServerConfig::default()
    };
    mutate_cfg(&mut cfg);
    let state = AppState {
        pool: pool.clone(),
        dispatcher: dispatcher.clone(),
        metrics,
        cfg: Arc::new(cfg),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let (tx, rx) = oneshot::channel::<()>();
    let router = build_router(state);
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });

    // Give axum a moment to bind
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    ServerHandle {
        base_url,
        pool,
        dispatcher,
        shutdown: Some(tx),
        task: Some(task),
    }
}

#[allow(dead_code)]
pub fn drv_path(hash: &str, name: &str) -> String {
    format!("/nix/store/{hash}-{name}.drv")
}
