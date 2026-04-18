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
        worker_health: nix_ci_core::dispatch::WorkerHealth::new(),
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

/// Scrape the coordinator's `/metrics` endpoint and return the value
/// of a specific metric (with optional label-set match). The value
/// comes from the wire format Prometheus would see — not from a
/// direct read of `dispatcher.metrics.inner.*.get()`, which the
/// H15 audits flagged as COMPLIANCE (testing the internal shape
/// instead of the observable contract).
///
/// - `name` is the wire metric name including any `_total` suffix
///   OpenMetrics adds (e.g. `nix_ci_jobs_reaped_total`,
///   `nix_ci_claims_in_flight`).
/// - `labels` is a Vec of `(key, value)` pairs the metric line must
///   match. Empty for unlabeled metrics. Order-insensitive.
///
/// Returns `None` if the metric isn't present (counter at 0 isn't
/// written in OpenMetrics text format until incremented).
#[allow(dead_code)]
pub async fn scrape_metric(base_url: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let body = reqwest::get(format!("{base_url}/metrics"))
        .await
        .expect("scrape /metrics")
        .text()
        .await
        .expect("read /metrics body");
    for line in body.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        // Lines look like:
        //   nix_ci_claims_in_flight 0
        //   nix_ci_builds_completed_total{outcome="success"} 3
        // Split into (metric-with-labels, value).
        let (head, value) = match line.rsplit_once(' ') {
            Some(x) => x,
            None => continue,
        };
        let (metric_name, label_part) = match head.find('{') {
            Some(idx) => {
                let end = head.rfind('}')?;
                (&head[..idx], Some(&head[idx + 1..end]))
            }
            None => (head, None),
        };
        if metric_name != name {
            continue;
        }
        if !labels_match(label_part, labels) {
            continue;
        }
        return value.parse::<f64>().ok();
    }
    None
}

#[allow(dead_code)]
fn labels_match(label_str: Option<&str>, want: &[(&str, &str)]) -> bool {
    let have = label_str.unwrap_or("");
    // Convert "k1=\"v1\",k2=\"v2\"" into a set. For our test use
    // cases, values are always quoted with double quotes and there
    // are no escapes inside.
    let mut parsed: Vec<(&str, &str)> = Vec::new();
    for part in have.split(',').filter(|s| !s.is_empty()) {
        let (k, v) = match part.split_once('=') {
            Some(x) => x,
            None => return false,
        };
        let v = v.trim_matches('"');
        parsed.push((k, v));
    }
    for (k, v) in want {
        if !parsed.iter().any(|(pk, pv)| pk == k && pv == v) {
            return false;
        }
    }
    true
}

/// Same as [`scrape_metric`] but panics with a helpful message when
/// the metric is missing. Use when a zero-valued counter still has
/// to be scraped — you'll get `Some(0.0)` on a labelled family once
/// it's been incremented at least once.
#[allow(dead_code)]
pub async fn scrape_metric_expect(base_url: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    match scrape_metric(base_url, name, labels).await {
        Some(v) => v,
        None => panic!("metric {name}{labels:?} not present in /metrics output"),
    }
}
