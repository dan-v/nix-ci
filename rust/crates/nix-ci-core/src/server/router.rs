//! Route table.

use axum::routing::{delete, get, post};
use axum::Router;

use super::AppState;
use super::{claim, complete, events, heartbeat, ingest, ingest_batch, jobs, ops};

pub fn build_router(state: AppState) -> Router {
    Router::new()
        // Jobs
        .route("/jobs", post(jobs::create))
        .route("/jobs/{id}", get(jobs::status))
        .route("/jobs/{id}/drvs", post(ingest::submit_drv))
        .route("/jobs/{id}/drvs/batch", post(ingest_batch::submit_batch))
        .route("/jobs/{id}/seal", post(jobs::seal))
        .route("/jobs/{id}/fail", post(jobs::fail))
        .route("/jobs/{id}/cancel", delete(jobs::cancel))
        .route("/jobs/{id}/heartbeat", post(heartbeat::heartbeat))
        .route("/jobs/{id}/claim", get(claim::claim))
        .route(
            "/jobs/{id}/claims/{claim_id}/complete",
            post(complete::complete),
        )
        .route("/jobs/{id}/events", get(events::events))
        // Ops
        .route("/healthz", get(ops::healthz))
        .route("/readyz", get(ops::readyz))
        .route("/metrics", get(ops::metrics))
        .route("/admin/snapshot", get(ops::admin_snapshot))
        .with_state(state)
}
