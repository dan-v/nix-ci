//! Route table.

use std::time::Instant;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, MatchedPath, Request};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
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

/// Routes that skip the bearer-token gate when auth is enabled.
/// Monitoring probes (`/healthz`, `/readyz`, `/metrics`) MUST stay
/// reachable so Prometheus / kubelet don't lock themselves out of the
/// coordinator on auth misconfiguration.
fn is_public_route(path: &str) -> bool {
    // `/version` is safe to expose unauthenticated — it only returns
    // the coordinator's build metadata, and operators need it to
    // confirm rolling-upgrade state without knowing the bearer token.
    matches!(
        path,
        "/healthz" | "/health" | "/readyz" | "/metrics" | "/version"
    )
}

/// Routes whose blast radius (force-cancel, force-fail, force-evict
/// a claim, read the full dispatcher dump) justifies requiring the
/// separate admin bearer when one is configured. A leaked worker
/// token then can't silently kill a production job or inspect live
/// per-step state. When `admin_bearer` is not set, these routes fall
/// back to the worker token like everything else.
///
/// Note: we compare against the MATCHED path (with `{id}` placeholders)
/// not the rendered URL — cardinality is bounded and predictable.
fn is_admin_route(path: &str) -> bool {
    path.starts_with("/admin/")
        || matches!(
            path,
            "/jobs/{id}/fail"
                | "/jobs/{id}/cancel"
                | "/admin/claims/{claim_id}"
                | "/admin/fence"
                | "/admin/drain"
                | "/admin/snapshot"
                | "/admin/debug/dispatcher-dump"
        )
}

pub fn build_router(state: AppState) -> Router {
    let max_body = state.cfg.max_request_body_bytes;
    let metrics_for_layer = state.metrics.clone();
    let auth_tokens = AuthTokens {
        worker: state.cfg.auth_bearer.clone(),
        admin: state.cfg.admin_bearer.clone(),
    };
    let request_timeout = state.cfg.request_timeout_secs;
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
        // Admin: force-evict a specific claim. Intended for the
        // operator-run "this worker is stuck and won't heartbeat"
        // recovery flow. Gated by the same bearer-token middleware as
        // other mutating routes when auth is enabled.
        .route("/admin/claims/{claim_id}", delete(claim::admin_evict_claim))
        .route(
            "/jobs/{id}/claims/{claim_id}/complete",
            post(complete::complete),
        )
        // Claim lease refresh — a worker whose build is running longer
        // than `claim_deadline_secs` calls this periodically to extend
        // the deadline. Without it, long builds get reaped and another
        // worker re-claims the drv, which turns a successful build into
        // `ignored:true` — effectively a silent infra-caused failure.
        .route(
            "/jobs/{id}/claims/{claim_id}/extend",
            post(claim::extend_claim),
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
        .route("/version", get(ops::version))
        .route("/admin/snapshot", get(ops::admin_snapshot))
        .route(
            "/admin/debug/dispatcher-dump",
            get(ops::admin_dispatcher_dump),
        )
        // Drain: stop accepting new jobs / new claims without
        // killing in-flight work. Rolling upgrades poll GET until
        // in_flight_claims hits 0, then SIGTERM.
        .route(
            "/admin/drain",
            post(ops::admin_drain_start).get(ops::admin_drain_status),
        )
        // Fence: per-worker claim block. A retiring host stops
        // receiving new work; its existing claims finish normally.
        .route(
            "/admin/fence",
            post(ops::admin_fence_add)
                .get(ops::admin_fence_list)
                .delete(ops::admin_fence_remove),
        )
        .layer(DefaultBodyLimit::max(max_body))
        .layer(middleware::from_fn_with_state(
            request_timeout,
            request_timeout_layer,
        ))
        .layer(middleware::from_fn_with_state(
            metrics_for_layer,
            http_metrics_layer,
        ))
        .layer(middleware::from_fn_with_state(
            auth_tokens,
            bearer_auth_layer,
        ))
        .layer(middleware::from_fn(request_id_layer))
        .with_state(state)
}

/// Per-handler request timeout. Bounds the wall-clock any single
/// non-long-poll handler can consume — a pathologically slow DB
/// query or an unresponsive downstream won't wedge a tokio task
/// forever (which would break graceful shutdown and slowly starve
/// the pool). Long-poll routes are exempt because they have their
/// own `wait` semantics; pushing them through here would produce
/// spurious 503s at the poll boundary.
///
/// Disabled when `request_timeout_secs == 0` (operators can turn it
/// off for debugging, but the production default is 30s).
async fn request_timeout_layer(
    axum::extract::State(timeout_secs): axum::extract::State<u64>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if timeout_secs == 0 {
        return next.run(req).await;
    }
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_default();
    if is_long_poll_route(&route) {
        return next.run(req).await;
    }
    let fut = next.run(req);
    match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), fut).await {
        Ok(resp) => resp,
        Err(_) => {
            crate::Error::ServiceUnavailable(format!("handler exceeded {timeout_secs}s timeout"))
                .into_response()
        }
    }
}

/// Token pair handed to the auth middleware. `admin` is only honored
/// when `worker` is also set (otherwise auth is disabled for the
/// whole router).
#[derive(Clone)]
struct AuthTokens {
    worker: Option<String>,
    admin: Option<String>,
}

/// Bearer-token gate. When `auth_bearer` is unset, all traffic passes
/// through regardless (the operator has explicitly opted out of auth,
/// typically because there's a mesh / VPN in front). When set, every
/// non-public request must carry `Authorization: Bearer <token>`.
///
/// Admin routes additionally require the `admin_bearer` if it's
/// configured, so a leaked worker token can't escalate to
/// `/jobs/{id}/cancel` or the dispatcher dump. Workers presenting the
/// worker token on an admin route get 403 (not 401) to distinguish
/// "you authenticated, but not for this" from "no credentials."
///
/// Comparison is constant-time so an attacker can't prefix-probe.
async fn bearer_auth_layer(
    axum::extract::State(tokens): axum::extract::State<AuthTokens>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let Some(worker_tok) = tokens.worker.as_deref() else {
        // No auth configured at all: everyone passes.
        return next.run(req).await;
    };
    let raw_path = req.uri().path().to_string();
    if is_public_route(&raw_path) {
        return next.run(req).await;
    }
    // Prefer the matched-route pattern for admin-route detection so
    // `/jobs/abc/fail` (with a real id) checks the same way as the
    // route definition `/jobs/{id}/fail`.
    let route_for_match = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| raw_path.clone());
    let is_admin = is_admin_route(&route_for_match);
    let presented = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    let Some(presented) = presented else {
        return crate::Error::Unauthorized("missing bearer token".into()).into_response();
    };

    let matches_worker = constant_time_eq(presented.as_bytes(), worker_tok.as_bytes());
    let matches_admin = tokens
        .admin
        .as_deref()
        .map(|t| constant_time_eq(presented.as_bytes(), t.as_bytes()))
        .unwrap_or(false);

    if is_admin {
        // Admin routes: accept the admin bearer when configured,
        // otherwise fall back to the worker bearer (backwards-compat
        // for single-token deployments).
        if matches_admin || (tokens.admin.is_none() && matches_worker) {
            return next.run(req).await;
        }
        if matches_worker {
            // Authenticated but not privileged: 403 is more accurate
            // than 401 and tells operators exactly what went wrong.
            return crate::Error::Forbidden("admin bearer required for this endpoint".into())
                .into_response();
        }
        return crate::Error::Unauthorized("invalid bearer token".into()).into_response();
    }

    // Non-admin route: either token works. Accepting the admin token
    // on worker routes is harmless (admin is strictly more-privileged)
    // and avoids accidentally locking out operator tooling.
    if matches_worker || matches_admin {
        next.run(req).await
    } else {
        crate::Error::Unauthorized("invalid bearer token".into()).into_response()
    }
}

/// Byte-equal constant-time compare. Prevents timing oracles on the
/// bearer token prefix. `subtle` crate would be cleaner but avoiding
/// a new dep for ≤20 lines.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
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
