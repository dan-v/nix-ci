//! Shared axum state. One `AppState` injected into every handler.

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use parking_lot::RwLock;
use sqlx::PgPool;

use crate::config::ServerConfig;
use crate::dispatch::Dispatcher;
use crate::durable::logs::LogStore;
use crate::observability::metrics::Metrics;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub dispatcher: Dispatcher,
    pub metrics: Metrics,
    pub cfg: Arc<ServerConfig>,
    /// Build log archive. `Arc<dyn LogStore>` so tests can inject an
    /// in-memory backend (and so we can swap to S3 later without
    /// touching every handler).
    pub log_store: Arc<dyn LogStore>,
    /// Operator signal: once set, new job creation and new claims are
    /// rejected, but existing in-flight work continues to completion.
    /// The typical rolling-upgrade flow is: POST /admin/drain, poll
    /// until claims_in_flight is 0, then SIGTERM. See server/ops.rs.
    pub draining: Arc<AtomicBool>,
    /// Operator signal for a targeted worker: claims with
    /// `worker_id` in this set are skipped at pop time, so a stuck
    /// or being-retired host can drain gracefully without new work
    /// while the rest of the fleet keeps running. Claims already
    /// issued to a fenced worker finish normally.
    pub fenced_workers: Arc<RwLock<HashSet<String>>>,
}
