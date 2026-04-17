//! Shared test helpers. Spins up the axum server in-process against a
//! `sqlx::test`-provided Postgres database, returning the base URL and
//! a shutdown handle.

use std::net::SocketAddr;
use std::sync::Arc;

use nix_ci_core::config::ServerConfig;
use nix_ci_core::dispatch::Dispatcher;
use nix_ci_core::durable;
use nix_ci_core::durable::logs::PgLogStore;
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
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

#[allow(dead_code)]
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

    // Clean slate per test: cancel anything non-terminal left over
    // from a prior sqlx::test run on this pool.
    durable::clear_busy(&pool).await.expect("clear_busy");

    let metrics = Metrics::new();
    let dispatcher = Dispatcher::new(metrics.clone());

    let mut cfg = ServerConfig {
        database_url: String::new(),
        listen: "127.0.0.1:0".parse().unwrap(),
        ..ServerConfig::default()
    };
    mutate_cfg(&mut cfg);
    let log_store: Arc<dyn nix_ci_core::durable::logs::LogStore> =
        Arc::new(PgLogStore::new(pool.clone()));
    let state = AppState {
        pool: pool.clone(),
        dispatcher: dispatcher.clone(),
        metrics,
        cfg: Arc::new(cfg),
        log_store,
        draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        fenced_workers: Arc::new(parking_lot::RwLock::new(std::collections::HashSet::new())),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let (tx, rx) = oneshot::channel::<()>();
    let router = build_router(state);
    tokio::spawn(async move {
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
    }
}

#[allow(dead_code)]
pub fn drv_path(hash: &str, name: &str) -> String {
    format!("/nix/store/{hash}-{name}.drv")
}

/// Shared ingest-request builder. Individual test files add their own
/// drv/name/deps; this keeps the boilerplate out of every site.
#[allow(dead_code)]
pub fn ingest(
    drv: &str,
    name: &str,
    deps: &[&str],
    is_root: bool,
) -> nix_ci_core::types::IngestDrvRequest {
    nix_ci_core::types::IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: deps.iter().map(|s| s.to_string()).collect(),
        is_root,
        attr: None,
    }
}
