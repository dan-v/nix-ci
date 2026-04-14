//! Health, readiness, metrics, snapshot.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

use super::AppState;
use crate::types::{AdminSnapshot, DrvState};

/// Liveness: the process is up and the axum task is scheduling. Used
/// as the systemd / orchestrator restart probe — deliberately cheap
/// (no DB hit) so a slow Postgres doesn't trigger a restart storm.
pub async fn healthz() -> &'static str {
    "ok"
}

/// Readiness: we can take traffic. Verifies a cheap DB round-trip so
/// a coordinator that's still starting up (migrations running,
/// rehydrate loop bootstrapping) or whose pool has saturated returns
/// 503. An orchestrator uses this to decide whether to route traffic
/// to this instance; unlike healthz, a failure here should NOT
/// restart the process — it just parks us out of rotation.
pub async fn readyz(State(state): State<AppState>) -> Response {
    match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sqlx::query("SELECT 1").fetch_one(&state.pool),
    )
    .await
    {
        Ok(Ok(_)) => (StatusCode::OK, "ok").into_response(),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "readyz: DB query failed");
            (StatusCode::SERVICE_UNAVAILABLE, "db error").into_response()
        }
        Err(_) => {
            tracing::warn!("readyz: DB query timed out");
            (StatusCode::SERVICE_UNAVAILABLE, "db timeout").into_response()
        }
    }
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
