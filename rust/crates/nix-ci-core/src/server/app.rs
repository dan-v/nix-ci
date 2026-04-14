//! Shared axum state. One `AppState` injected into every handler.

use std::sync::Arc;

use sqlx::PgPool;

use crate::config::ServerConfig;
use crate::dispatch::Dispatcher;
use crate::observability::metrics::Metrics;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub dispatcher: Dispatcher,
    pub metrics: Metrics,
    pub cfg: Arc<ServerConfig>,
}
