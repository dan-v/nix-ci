//! Route table.

use std::time::Instant;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, MatchedPath, Request};
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::Router;

use super::AppState;
use super::{build_logs, claim, complete, events, heartbeat, ingest_batch, jobs, ops};
use crate::observability::metrics::HttpLabels;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Routes whose latency is dominated by long-poll wait time. We
/// exclude them from the per-endpoint latency histogram so they don't
/// inflate the SLO buckets — alerts on `/complete p99 > 1s` should
/// not be triggered by a 30s long-poll on `/claim`.
fn is_long_poll_route(path: &str) -> bool {
    matches!(path, "/jobs/{id}/claim" | "/claim" | "/jobs/{id}/events")
}

pub fn build_router(state: AppState) -> Router {
    let max_body = state.cfg.max_request_body_bytes;
    let metrics_for_layer = state.metrics.clone();
    Router::new()
        // Jobs
        .route("/jobs", post(jobs::create))
        .route("/jobs", get(jobs::list))
        // Operator lookup by caller-supplied external_ref (e.g. CCI
        // build ID, PR number). Slug-style path so refs with `/` or
        // `:` still route correctly.
        .route(
            "/jobs/by-external-ref/{external_ref}",
            get(jobs::by_external_ref),
        )
        .route("/jobs/{id}", get(jobs::status))
        .route("/jobs/{id}/drvs/batch", post(ingest_batch::submit_batch))
        .route("/jobs/{id}/seal", post(jobs::seal))
        .route("/jobs/{id}/fail", post(jobs::fail))
        .route("/jobs/{id}/cancel", delete(jobs::cancel))
        .route("/jobs/{id}/heartbeat", post(heartbeat::heartbeat))
        .route("/jobs/{id}/claim", get(claim::claim))
        // Fleet claim: scans all live submissions in FIFO order. Used
        // by `nix-ci worker` instances that aren't bound to one job.
        .route("/claim", get(claim::claim_any))
        // Operator: point-in-time list of in-flight claims. Sorted
        // longest-running first so "what's stuck?" is answered by the
        // first row.
        .route("/claims", get(claim::list_claims))
        .route(
            "/jobs/{id}/claims/{claim_id}/complete",
            post(complete::complete),
        )
        // Build log archive.
        .route(
            "/jobs/{id}/claims/{claim_id}/log",
            post(build_logs::upload_log).get(build_logs::fetch_log),
        )
        .route(
            "/jobs/{id}/drvs/{drv_hash}/logs",
            get(build_logs::list_drv_logs),
        )
        .route("/jobs/{id}/events", get(events::events))
        // Ops. `/health` is the same as `/healthz` — kept for proxies
        // (Envoy, GCP load balancers) that look for the unsuffixed
        // path; `/healthz` is the kube convention.
        .route("/healthz", get(ops::healthz))
        .route("/health", get(ops::healthz))
        .route("/readyz", get(ops::readyz))
        .route("/metrics", get(ops::metrics))
        .route("/admin/snapshot", get(ops::admin_snapshot))
        .layer(DefaultBodyLimit::max(max_body))
        .layer(middleware::from_fn_with_state(
            metrics_for_layer,
            http_metrics_layer,
        ))
        .layer(middleware::from_fn(request_id_layer))
        .with_state(state)
}

/// Per-endpoint latency histogram. Records request duration labelled
/// by `(method, route, status_class)` so SREs can answer "did /complete
/// just get slow?" and alert on it without scrolling Grafana.
///
/// Long-poll routes (`/claim`, `/claim_any`, `/events`) are excluded —
/// their latency is dominated by wait time, not work.
async fn http_metrics_layer(
    axum::extract::State(metrics): axum::extract::State<crate::observability::metrics::Metrics>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        // No matched route = 404. Bucket them under a single label so
        // a flood of 404s doesn't blow up cardinality.
        .unwrap_or_else(|| "<unmatched>".to_string());
    let start = Instant::now();
    let resp = next.run(req).await;
    let status = resp.status();

    if !is_long_poll_route(&route) {
        let class = format!("{}xx", status.as_u16() / 100);
        metrics
            .inner
            .http_request_duration_seconds
            .get_or_create(&HttpLabels {
                method: method.as_str().to_string(),
                route,
                status: class,
            })
            .observe(start.elapsed().as_secs_f64());
    }
    resp
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
