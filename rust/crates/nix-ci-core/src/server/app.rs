//! Shared axum state. One `AppState` injected into every handler.

use std::sync::Arc;

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
}
