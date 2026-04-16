//! Route table.

use std::time::Instant;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, MatchedPath, Request};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::Router;
use opentelemetry::propagation::Extractor;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
///
/// Also extracts a W3C `traceparent` (and `tracestate`) header if the
/// client sent one, and links the request span to the upstream OTel
/// trace context. The result is a single distributed trace covering
/// `nix-ci run` → coordinator HTTP → worker → coordinator complete,
/// visible end-to-end in Jaeger / Tempo / Honeycomb.
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
    // Bridge the upstream OTel trace context (if any) into this span.
    // When OTLP is disabled the global propagator is a no-op and this
    // is a cheap no-op call — no allocation, no header parsing past
    // the missing-header miss.
    let parent_ctx = opentelemetry::global::get_text_map_propagator(|prop| {
        prop.extract(&HeaderMapExtractor(req.headers()))
    });
    span.set_parent(parent_ctx);

    let mut resp = tracing::Instrument::instrument(next.run(req), span).await;

    if let Ok(header_val) = HeaderValue::from_str(&request_id) {
        resp.headers_mut().insert(REQUEST_ID_HEADER, header_val);
    }
    resp
}

/// Adapter for the OTel propagator API, which expects an `Extractor`
/// trait that doesn't match `HeaderMap` directly (header names are
/// case-insensitive in HTTP but `HeaderMap`'s API expects `HeaderName`
/// for typed access).
struct HeaderMapExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Request-scoped correlation id. Available via `Extension<RequestId>`
/// in any handler that wants to read it.
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

#[cfg(test)]
mod extractor_tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn extractor_get_returns_present_header() {
        let mut h = HeaderMap::new();
        h.insert(
            "traceparent",
            HeaderValue::from_static("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"),
        );
        let ex = HeaderMapExtractor(&h);
        assert_eq!(
            ex.get("traceparent"),
            Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
        );
    }

    #[test]
    fn extractor_get_returns_none_for_missing() {
        let h = HeaderMap::new();
        let ex = HeaderMapExtractor(&h);
        assert_eq!(ex.get("traceparent"), None);
    }

    #[test]
    fn extractor_get_is_case_insensitive() {
        // HTTP headers are case-insensitive; reqwest/axum lowercase
        // them on insert so the propagator's mixed-case lookup must
        // still resolve.
        let mut h = HeaderMap::new();
        h.insert("traceparent", HeaderValue::from_static("00-test-test-01"));
        let ex = HeaderMapExtractor(&h);
        assert_eq!(ex.get("Traceparent"), Some("00-test-test-01"));
        assert_eq!(ex.get("TRACEPARENT"), Some("00-test-test-01"));
    }

    #[test]
    fn extractor_keys_lists_all_header_names() {
        let mut h = HeaderMap::new();
        h.insert("traceparent", HeaderValue::from_static("v1"));
        h.insert("tracestate", HeaderValue::from_static("v2"));
        let ex = HeaderMapExtractor(&h);
        let keys = ex.keys();
        assert!(keys.contains(&"traceparent"));
        assert!(keys.contains(&"tracestate"));
    }

    #[test]
    fn long_poll_route_check_matches_known_paths() {
        assert!(is_long_poll_route("/jobs/{id}/claim"));
        assert!(is_long_poll_route("/claim"));
        assert!(is_long_poll_route("/jobs/{id}/events"));
        // Negative cases — these MUST be in the latency histogram.
        assert!(!is_long_poll_route("/jobs/{id}/seal"));
        assert!(!is_long_poll_route("/jobs/{id}/drvs/batch"));
        assert!(!is_long_poll_route("/jobs"));
    }
}
