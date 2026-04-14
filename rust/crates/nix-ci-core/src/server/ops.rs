//! Health, readiness, metrics, snapshot.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;

use super::AppState;
use crate::error::Result;
use crate::types::{AdminSnapshot, DrvState};

pub async fn healthz() -> &'static str {
    "ok"
}

pub async fn readyz(State(state): State<AppState>) -> Result<&'static str> {
    sqlx::query("SELECT 1").fetch_one(&state.pool).await?;
    Ok("ok")
}

pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.metrics.render();
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        "application/openmetrics-text; version=1.0.0; charset=utf-8"
            .parse()
            .unwrap(),
    );
    (StatusCode::OK, headers, body)
}

pub async fn admin_snapshot(State(state): State<AppState>) -> Json<AdminSnapshot> {
    let submissions = state.dispatcher.submissions.len() as u32;
    let steps = state.dispatcher.steps.live();
    let steps_total = steps.len() as u32;
    let mut counts = StateCounts::default();
    for s in &steps {
        match s.observable_state() {
            DrvState::Pending => counts.pending += 1,
            DrvState::Building => counts.building += 1,
            DrvState::Done => counts.done += 1,
            DrvState::Failed => counts.failed += 1,
        }
    }
    Json(AdminSnapshot {
        submissions,
        steps_total,
        steps_pending: counts.pending,
        steps_building: counts.building,
        steps_done: counts.done,
        steps_failed: counts.failed,
        active_claims: state.dispatcher.claims.len() as u32,
    })
}

#[derive(Default)]
struct StateCounts {
    pending: u32,
    building: u32,
    done: u32,
    failed: u32,
}
