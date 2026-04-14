//! Route table.

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Request};
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::Router;

use super::AppState;
use super::{claim, complete, events, heartbeat, ingest_batch, jobs, ops};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

pub fn build_router(state: AppState) -> Router {
    let max_body = state.cfg.max_request_body_bytes;
    Router::new()
        // Jobs
        .route("/jobs", post(jobs::create))
        .route("/jobs/{id}", get(jobs::status))
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
        .layer(DefaultBodyLimit::max(max_body))
        .layer(middleware::from_fn(request_id_layer))
        .with_state(state)
}

/// Read `X-Request-Id` off the request if the client set one; otherwise
/// mint a fresh v4 uuid. The id is attached to a tracing span that
/// wraps the handler, so every log line emitted during this request
/// carries a consistent correlation key. The response echoes the id
/// in the `X-Request-Id` header so a worker that captured the id on
/// its outbound call can cross-reference it in coordinator logs.
async fn request_id_layer(mut req: Request<Body>, next: Next) -> Response {
    let request_id = req
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Make the id available to downstream middleware / handlers via
    // request extensions if they want to read it directly (e.g., to
    // include in an error payload).
    req.extensions_mut().insert(RequestId(request_id.clone()));

    let span = tracing::info_span!(
        "request",
        request_id = %request_id,
        method = %req.method(),
        path = req.uri().path(),
    );
    let mut resp = tracing::Instrument::instrument(next.run(req), span).await;

    if let Ok(header_val) = HeaderValue::from_str(&request_id) {
        resp.headers_mut().insert(REQUEST_ID_HEADER, header_val);
    }
    resp
}

/// Request-scoped correlation id. Available via `Extension<RequestId>`
/// in any handler that wants to read it.
#[derive(Debug, Clone)]
pub struct RequestId(pub String);
