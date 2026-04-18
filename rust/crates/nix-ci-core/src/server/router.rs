//! Route table.

use std::panic::AssertUnwindSafe;

use tokio::time::Instant;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, MatchedPath, Request};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use futures::FutureExt;
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
        // Refute: operator lever for clearing false-positive
        // failed_outputs entries (e.g. a sick worker marked a
        // drv failed before auto-quarantine kicked in, and the
        // drv is still blocked by the TTL cache). Gated by the
        // admin bearer via is_admin_route.
        .route("/admin/refute", post(ops::admin_refute))
        .layer(DefaultBodyLimit::max(max_body))
        // Catch-panic sits innermost so a panic inside any handler or
        // deeper middleware is converted to a 500 response rather than
        // unwinding through the axum task — dropping the connection
        // and leaving the client with no signal. Each of the outer
        // layers (timeout, metrics, auth, request-id) then observes a
        // synthetic 500 and records/emits the same way as any other
        // error. The real panic is logged by the global panic hook
        // (installed at binary startup) which also increments the
        // `nix_ci_process_panics` counter — operators alert on that.
        //
        // Placed inside the body-limit layer so a panic triggered by
        // an unusually large body (a reserved-but-unusual eventuality)
        // still doesn't escape; the body-limit check itself is a
        // `Service`, not a future, and doesn't panic.
        .layer(middleware::from_fn(catch_panic_layer))
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

/// Convert a panic in any downstream handler/middleware into a 500
/// response, keeping the axum task alive and the connection healthy.
///
/// Without this, a panic propagates up through the spawned connection
/// task; axum drops the connection silently (the client sees an EOF,
/// not an error payload) and — more importantly for reliability — the
/// blast radius of a single malformed input is unbounded: any panic
/// the input triggers also skips our metrics/auth layers, so the
/// operator's dashboards don't see the failure cleanly.
///
/// Semantics mirror `Error::Internal`: fixed-shape 500 body, the real
/// panic payload is logged (via the global panic hook + a targeted
/// tracing event here), and the `nix_ci_process_panics` counter
/// increments. Callers cannot distinguish "handler panicked" from
/// "handler returned Error::Internal" — by design; both are our fault
/// and both require the same ops response.
///
/// `AssertUnwindSafe` is required because `Next`'s future captures the
/// request (not `UnwindSafe`). It's sound here because:
/// * we don't reuse the future on the error path — a panicked future
///   is dropped, not resumed;
/// * no observer holds references into the future across the .await;
/// * the caller synthesizes a fresh response from constants.
async fn catch_panic_layer(req: Request<Body>, next: Next) -> Response {
    // Capture the path up front so the tracing line still names the
    // route after the request is consumed by `next.run`.
    let matched = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string());
    let raw_path = req.uri().path().to_string();
    match AssertUnwindSafe(next.run(req)).catch_unwind().await {
        Ok(resp) => resp,
        Err(payload) => {
            let msg = panic_payload_message(&payload);
            let route = matched.unwrap_or(raw_path);
            tracing::error!(
                route = %route,
                panic_payload = %msg,
                "handler panicked; returning 500"
            );
            crate::Error::Internal(format!("handler panic at {route}")).into_response()
        }
    }
}

/// Best-effort extraction of a panic payload's string representation.
/// Panics typically use `&'static str` (from `panic!("msg")`) or
/// `String` (from `panic!("{x}")`); other payload types are rare and
/// don't carry a diagnostic — fall back to a marker string so the log
/// line always has *something* useful.
fn panic_payload_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<non-string panic payload>".to_string()
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

    #[test]
    fn panic_payload_message_downcasts_str_and_string() {
        // `panic!("lit")` payload is `&'static str`; `panic!("{x}")`
        // payload is `String`; other types fall through to the marker.
        // The middleware relies on all three cases in production.
        let p: Box<dyn std::any::Any + Send> = Box::new("hello");
        assert_eq!(panic_payload_message(&p), "hello");

        let p: Box<dyn std::any::Any + Send> = Box::new(String::from("world"));
        assert_eq!(panic_payload_message(&p), "world");

        // Anything else — `Box<u32>` is a plausible-ish "I panicked
        // with a value that isn't a diagnostic string" case.
        let p: Box<dyn std::any::Any + Send> = Box::new(42u32);
        assert_eq!(panic_payload_message(&p), "<non-string panic payload>");
    }
}

#[cfg(test)]
mod catch_panic_tests {
    //! Exercise the catch-panic middleware in isolation. We don't use
    //! the full `spawn_server` harness here — tower's `oneshot` lets us
    //! drive a mini-router directly with no TCP listener, no DB pool,
    //! and no background tasks, so the test runtime stays tight and
    //! the assertions focus entirely on the panic-recovery surface.
    //!
    //! The coverage we need:
    //! * A panic in a handler → 500 response with the sanitized
    //!   error body (not a dropped connection).
    //! * After a panic, the router still serves the next request
    //!   (panics must not poison shared state — catch_unwind is per
    //!   future, so this should hold, but we assert it explicitly).
    //! * The panic body is the same `Error::Internal`-shaped JSON
    //!   clients already handle, so tooling that parses 5xx errors
    //!   doesn't need a second code path.
    //! * Both `&'static str` and formatted-`String` panic payloads
    //!   are handled without the middleware itself panicking.
    //! * A non-panicking handler is completely unaffected (no
    //!   latency added beyond the catch_unwind poll, no body
    //!   rewriting, no status mutation).
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;

    async fn panics_with_str() -> &'static str {
        panic!("intentional: static str payload")
    }

    async fn panics_with_formatted_string() -> &'static str {
        let noun = "world";
        panic!("intentional: formatted {noun}")
    }

    async fn returns_ok() -> &'static str {
        "ok"
    }

    fn test_router() -> Router {
        Router::new()
            .route("/boom_str", get(panics_with_str))
            .route("/boom_string", get(panics_with_formatted_string))
            .route("/ok", get(returns_ok))
            .layer(middleware::from_fn(catch_panic_layer))
    }

    async fn status_and_body(
        app: Router,
        path: &str,
    ) -> (StatusCode, serde_json::Value) {
        let req = Request::builder()
            .uri(path)
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.expect("oneshot");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 8192)
            .await
            .expect("collect body");
        if bytes.is_empty() {
            return (status, serde_json::Value::Null);
        }
        let v = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        (status, v)
    }

    #[tokio::test]
    async fn str_panic_becomes_sanitized_500() {
        let (status, body) = status_and_body(test_router(), "/boom_str").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        // Error body shape must match the regular Error::Internal
        // path so clients have one 5xx parser.
        assert_eq!(body["code"], 500);
        assert_eq!(body["error"], "internal server error");
        // Crucially: the panic payload MUST NOT be echoed into the
        // response body — operators see it in logs, but the network
        // only ever sees the sanitized constant.
        let body_str = body.to_string();
        assert!(
            !body_str.contains("static str payload"),
            "panic payload leaked into response body: {body_str}"
        );
    }

    #[tokio::test]
    async fn formatted_string_panic_becomes_sanitized_500() {
        let (status, body) = status_and_body(test_router(), "/boom_string").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["code"], 500);
        assert_eq!(body["error"], "internal server error");
        assert!(!body.to_string().contains("formatted world"));
    }

    #[tokio::test]
    async fn non_panicking_handler_is_untouched() {
        let (status, body) = status_and_body(test_router(), "/ok").await;
        assert_eq!(status, StatusCode::OK);
        // The handler returned a bare string, not JSON; our
        // extractor collapses to a JSON string in that case.
        assert_eq!(body.as_str().unwrap_or_default(), "ok");
    }

    #[tokio::test]
    async fn router_stays_alive_after_a_panic() {
        // The regression this guards against is an axum / tokio / tower
        // interaction where a panicking future somehow poisons the
        // router (e.g. via a Layer that holds `&mut` state). Our layer
        // is stateless so this should be stable, but an explicit
        // assertion is cheap and documents the contract.
        let router = test_router();
        let (boom_status, _) = status_and_body(router.clone(), "/boom_str").await;
        assert_eq!(boom_status, StatusCode::INTERNAL_SERVER_ERROR);
        let (ok_status, body) = status_and_body(router, "/ok").await;
        assert_eq!(ok_status, StatusCode::OK);
        assert_eq!(body.as_str().unwrap_or_default(), "ok");
    }

    #[tokio::test]
    async fn sequential_panics_all_produce_500() {
        // Two back-to-back panics must both surface clean 500s —
        // the per-future catch_unwind must not pick up state from
        // a previous unwind.
        let router = test_router();
        for _ in 0..3 {
            let (s, _) = status_and_body(router.clone(), "/boom_str").await;
            assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
}
