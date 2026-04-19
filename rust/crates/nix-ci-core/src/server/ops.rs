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

/// Readiness: this instance should receive traffic right now.
///
/// Unlike `/healthz` (liveness — is the process responsive at all,
/// drives systemd / orchestrator restart), `/readyz` drives load
/// balancer routing: 200 means "send me work," 503 means "route
/// elsewhere." A failure here must not restart the process — it just
/// parks us out of rotation until the underlying signal clears.
///
/// Three readiness checks, each short-circuiting with a distinct body:
///   1. `draining`: an operator triggered `/admin/drain` for a
///      rolling-upgrade cutover. New traffic belongs on a standby.
///   2. Overload: `claims_in_flight` has reached `max_claims_in_flight`
///      (the shedding threshold). New claims would 503 at the handler
///      anyway; flipping readiness here lets the LB steer to a less-
///      saturated instance and protect this one from further load.
///   3. PG reachability: a 2s-timed `SELECT 1`. Boot-time migrations,
///      pool saturation, or network partition all surface here.
///
/// Order matters: cheap in-memory checks first so a load balancer
/// polling at 1 Hz doesn't pound PG from every replica during drain.
pub async fn readyz(State(state): State<AppState>) -> Response {
    if state.draining.load(std::sync::atomic::Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response();
    }
    if let Some(cap) = state.cfg.max_claims_in_flight {
        let current = state.metrics.inner.claims_in_flight.get();
        if current >= 0 && (current as u64) >= u64::from(cap) {
            return (StatusCode::SERVICE_UNAVAILABLE, "overloaded").into_response();
        }
    }
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
    // Refresh snapshot gauges on every scrape so Prometheus sees
    // current dispatcher size rather than zero.
    state
        .metrics
        .inner
        .submissions_active
        .set(state.dispatcher.submissions.len() as i64);
    state
        .metrics
        .inner
        .steps_registry_size
        .set(state.dispatcher.steps.len() as i64);
    // H3: PG pool usage — saturation is a leading indicator for
    // coordinator-induced latency spikes.
    state
        .metrics
        .inner
        .pg_pool_size
        .set(state.pool.size() as i64);
    state
        .metrics
        .inner
        .pg_pool_idle
        .set(state.pool.num_idle() as i64);

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
    // Top-5 most recent failed jobs. One DB query, indexed. Lets
    // operators see "X failures in metrics — which jobs?" in a single
    // snapshot call without going to /jobs?status=failed separately.
    let recent_failures = match crate::durable::writeback::list_jobs_by_status(
        &state.pool,
        "failed",
        None,
        5,
    )
    .await
    {
        Ok(rows) => rows
            .into_iter()
            .take(5)
            .map(|r| {
                crate::server::jobs::job_summary_from_row(
                    r.id,
                    r.external_ref,
                    &r.status,
                    r.done_at,
                    r.result.as_ref(),
                )
            })
            .collect(),
        Err(e) => {
            tracing::warn!(error = %e, "snapshot: recent_failures query failed; returning empty");
            Vec::new()
        }
    };
    Json(AdminSnapshot {
        submissions,
        steps_total,
        steps_pending: counts.pending,
        steps_building: counts.building,
        steps_done: counts.done,
        steps_failed: counts.failed,
        active_claims: state.dispatcher.claims.len() as u32,
        recent_failures,
    })
}

#[derive(Default)]
struct StateCounts {
    pending: u32,
    building: u32,
    done: u32,
    failed: u32,
}

/// `GET /version` — build metadata, unauthenticated so operators can
/// confirm which coordinator is running without knowing the bearer
/// token. Surfaced fields are stable-ish; add new ones rather than
/// repurposing existing ones so rolling upgrades don't silently
/// mis-interpret each other.
pub async fn version() -> Json<VersionInfo> {
    Json(VersionInfo {
        version: env!("CARGO_PKG_VERSION"),
        git_revision: option_env!("NIX_CI_GIT_REVISION").unwrap_or("unknown"),
    })
}

#[derive(serde::Serialize)]
pub struct VersionInfo {
    pub version: &'static str,
    pub git_revision: &'static str,
}

/// `GET /admin/debug/dispatcher-dump` — full in-memory snapshot for
/// post-mortem debugging. NOT for dashboards (cardinality blows up);
/// operators pull this when a job is stuck and they need to see the
/// graph. Gated by the bearer-token middleware when auth is enabled.
///
/// Intentionally cheap: one read-lock pass over the dispatcher maps,
/// no DB queries. Safe to call at any scale but the response grows
/// with in-memory state — expect MB for a 500K-drv registry.
pub async fn admin_dispatcher_dump(State(state): State<AppState>) -> Json<DispatcherDump> {
    let submissions: Vec<DispatcherDumpSubmission> = state
        .dispatcher
        .submissions
        .all()
        .into_iter()
        .map(|s| {
            let counts = s.live_counts();
            DispatcherDumpSubmission {
                id: s.id,
                priority: s.priority,
                max_workers: s.max_workers,
                sealed: s.is_sealed(),
                terminal: s.is_terminal(),
                active_claims: s.active_claims.load(std::sync::atomic::Ordering::Acquire),
                reserved_drvs: s.reserved_drvs.load(std::sync::atomic::Ordering::Acquire),
                member_count: s.members.read().len() as u32,
                toplevel_count: s.toplevels.read().len() as u32,
                failure_count: s.failures.read().len() as u32,
                counts,
            }
        })
        .collect();

    let claims: Vec<DispatcherDumpClaim> = state
        .dispatcher
        .claims
        .all()
        .into_iter()
        .map(|c| DispatcherDumpClaim {
            claim_id: c.claim_id,
            job_id: c.job_id,
            drv_hash: c.drv_hash.clone(),
            attempt: c.attempt,
            worker_id: c.worker_id.clone(),
            started_at: c.started_at_wall,
            elapsed_secs: c.started_at.elapsed().as_secs(),
        })
        .collect();

    Json(DispatcherDump {
        submissions,
        claims,
        steps_registry_size: state.dispatcher.steps.len() as u32,
    })
}

#[derive(serde::Serialize)]
pub struct DispatcherDump {
    pub submissions: Vec<DispatcherDumpSubmission>,
    pub claims: Vec<DispatcherDumpClaim>,
    /// Includes both live and yet-to-be-GC'd Weak entries.
    pub steps_registry_size: u32,
}

#[derive(serde::Serialize)]
pub struct DispatcherDumpSubmission {
    pub id: crate::types::JobId,
    pub priority: i32,
    pub max_workers: Option<u32>,
    pub sealed: bool,
    pub terminal: bool,
    pub active_claims: u32,
    pub reserved_drvs: u32,
    pub member_count: u32,
    pub toplevel_count: u32,
    pub failure_count: u32,
    pub counts: crate::types::JobCounts,
}

#[derive(serde::Serialize)]
pub struct DispatcherDumpClaim {
    pub claim_id: crate::types::ClaimId,
    pub job_id: crate::types::JobId,
    pub drv_hash: crate::types::DrvHash,
    pub attempt: i32,
    pub worker_id: Option<String>,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub elapsed_secs: u64,
}

/// `POST /admin/drain` — flip the coordinator into drain mode. New
/// job creation returns 503 and new claims return 204 (nothing
/// claimable); already-running workers finish their current build and
/// POST /complete as usual. The response carries a snapshot of
/// remaining work so operators can decide when to SIGTERM.
///
/// `GET /admin/drain` returns the same snapshot without flipping the
/// flag — useful for polling convergence.
#[tracing::instrument(skip_all)]
pub async fn admin_drain_start(State(state): State<AppState>) -> Json<DrainStatus> {
    state
        .draining
        .store(true, std::sync::atomic::Ordering::Release);
    // Wake long-pollers so they immediately see the drain flag and
    // return 204 rather than sleeping to their deadline.
    state.dispatcher.wake();
    Json(drain_snapshot(&state))
}

#[tracing::instrument(skip_all)]
pub async fn admin_drain_status(State(state): State<AppState>) -> Json<DrainStatus> {
    Json(drain_snapshot(&state))
}

fn drain_snapshot(state: &AppState) -> DrainStatus {
    let submissions = state.dispatcher.submissions.all();
    let in_flight_claims = state.dispatcher.claims.len() as u32;
    let open_submissions = submissions.iter().filter(|s| !s.is_terminal()).count() as u32;
    DrainStatus {
        draining: state.draining.load(std::sync::atomic::Ordering::Acquire),
        open_submissions,
        in_flight_claims,
    }
}

#[derive(serde::Serialize)]
pub struct DrainStatus {
    pub draining: bool,
    /// Submissions that haven't yet transitioned terminal. Continues
    /// ticking down as workers complete their claims.
    pub open_submissions: u32,
    /// Claims outstanding. When both this and `open_submissions` are
    /// zero, it's safe to SIGTERM without losing work.
    pub in_flight_claims: u32,
}

/// `POST /admin/fence?worker_id=X` — prevent new claims going to the
/// named worker. Claims that were already issued to it continue to
/// complete normally. Designed for "this host is being retired; drain
/// its existing work but don't give it more." Idempotent: adding a
/// worker that's already fenced is a no-op.
///
/// `DELETE /admin/fence?worker_id=X` removes the fence. `GET
/// /admin/fence` lists the currently-fenced set.
#[tracing::instrument(skip_all, fields(worker_id = %q.worker_id))]
pub async fn admin_fence_add(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<FenceQuery>,
) -> Result<Json<FenceStatus>, crate::Error> {
    validate_worker_id(&q.worker_id, state.cfg.max_identifier_bytes)?;
    state.fenced_workers.write().insert(q.worker_id);
    state.dispatcher.wake();
    Ok(Json(fence_status(&state)))
}

#[tracing::instrument(skip_all, fields(worker_id = %q.worker_id))]
pub async fn admin_fence_remove(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<FenceQuery>,
) -> Result<Json<FenceStatus>, crate::Error> {
    validate_worker_id(&q.worker_id, state.cfg.max_identifier_bytes)?;
    state.fenced_workers.write().remove(&q.worker_id);
    state.dispatcher.wake();
    Ok(Json(fence_status(&state)))
}

/// Shared shape builder for the three `/admin/fence` handlers.
/// Returns the manual fence list + the live auto-quarantine
/// snapshot so operators see a unified picture of "workers not
/// getting new claims."
fn fence_status(state: &AppState) -> FenceStatus {
    let auto_quarantined = state
        .worker_health
        .snapshot_quarantined()
        .into_iter()
        .map(|(worker_id, release_at)| {
            let remaining = release_at.saturating_duration_since(tokio::time::Instant::now());
            AutoQuarantinedWorker {
                worker_id,
                release_in_secs: remaining.as_secs(),
            }
        })
        .collect();
    FenceStatus {
        fenced: fenced_vec(state),
        auto_quarantined,
    }
}

pub async fn admin_fence_list(State(state): State<AppState>) -> Json<FenceStatus> {
    Json(fence_status(&state))
}

fn fenced_vec(state: &AppState) -> Vec<String> {
    let mut v: Vec<String> = state.fenced_workers.read().iter().cloned().collect();
    v.sort();
    v
}

fn validate_worker_id(worker_id: &str, max_bytes: usize) -> Result<(), crate::Error> {
    if worker_id.is_empty() {
        return Err(crate::Error::BadRequest(
            "worker_id must be non-empty".into(),
        ));
    }
    if worker_id.len() > max_bytes {
        return Err(crate::Error::BadRequest(format!(
            "worker_id exceeds max_identifier_bytes ({} > {max_bytes})",
            worker_id.len()
        )));
    }
    Ok(())
}

#[derive(serde::Deserialize)]
pub struct FenceQuery {
    pub worker_id: String,
}

#[derive(serde::Serialize)]
pub struct FenceStatus {
    pub fenced: Vec<String>,
    /// Workers currently auto-quarantined by the per-worker failure
    /// circuit breaker. Emitted alongside `fenced` so an operator
    /// running `GET /admin/fence` sees the union of "things that
    /// aren't getting new work." Omitted from the response when
    /// serde skips the empty Vec? No — we always emit so clients
    /// can branch on the presence of the key without parsing.
    pub auto_quarantined: Vec<AutoQuarantinedWorker>,
}

#[derive(serde::Serialize)]
pub struct AutoQuarantinedWorker {
    pub worker_id: String,
    /// Seconds until auto-release. Zero means the next claim by
    /// this worker will clear the quarantine lazily.
    pub release_in_secs: u64,
}

// ─── `/admin/refute` — clear false-positive failed_outputs entries ─

/// `POST /admin/refute` — remove one or more entries from the
/// `failed_outputs` TTL cache. Two caller-facing use cases:
///
/// * **False positive on a single drv**: a sick worker reported a
///   drv as `BuildFailure`, poisoning the cache so every future
///   job referencing the same output path skips the build and
///   inherits the failure. Refuting by `drv_hash` clears every
///   output associated with it.
///
/// * **Environment was fixed**: an external dependency (broken
///   substituter, missing sandbox mount) was corrected before the
///   TTL expired. Refuting by `output_paths` lets the next job try
///   rebuilding the drv without waiting out the TTL.
///
/// Returns the number of rows deleted. Zero rows isn't an error —
/// the operator may have passed paths that were already expired,
/// or the cache was queried before the insert raced.
///
/// Gated behind the admin bearer via `is_admin_route` + the
/// router's auth middleware.
#[tracing::instrument(skip_all)]
pub async fn admin_refute(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<RefuteRequest>,
) -> Result<Json<RefuteResponse>, crate::Error> {
    // Input validation: bound the request size so a malformed
    // client can't drive the DELETE into a pathological shape.
    // `max_identifier_bytes` is the right bound for individual
    // paths (they're already constrained by ingest validation to
    // `max_drv_path_bytes`, but we defensively re-check here).
    if req.output_paths.len() > state.cfg.max_input_drvs_per_drv as usize {
        return Err(crate::Error::PayloadTooLarge(format!(
            "output_paths batch of {} exceeds max_input_drvs_per_drv={}",
            req.output_paths.len(),
            state.cfg.max_input_drvs_per_drv
        )));
    }
    for p in &req.output_paths {
        if p.len() > state.cfg.max_drv_path_bytes {
            return Err(crate::Error::BadRequest(format!(
                "output_path length {} exceeds max_drv_path_bytes",
                p.len()
            )));
        }
    }
    if req.output_paths.is_empty() && req.drv_hash.is_none() && req.worker_id.is_none() {
        return Err(crate::Error::BadRequest(
            "specify at least one of: drv_hash, output_paths, worker_id".into(),
        ));
    }
    if let Some(w) = req.worker_id.as_deref() {
        if w.is_empty() {
            return Err(crate::Error::BadRequest(
                "worker_id, when set, must be non-empty".into(),
            ));
        }
        if w.len() > state.cfg.max_identifier_bytes {
            return Err(crate::Error::BadRequest(format!(
                "worker_id length {} exceeds max_identifier_bytes",
                w.len()
            )));
        }
    }

    let drv_hash_ref = req.drv_hash.as_ref();
    let rows_affected = crate::durable::writeback::delete_failed_outputs(
        &state.pool,
        drv_hash_ref,
        &req.output_paths,
        req.worker_id.as_deref(),
    )
    .await?;

    tracing::warn!(
        drv_hash = ?req.drv_hash,
        paths = req.output_paths.len(),
        worker_id = ?req.worker_id,
        rows_affected,
        "admin: refuted failed_outputs entries"
    );
    Ok(Json(RefuteResponse { rows_affected }))
}

#[derive(serde::Deserialize)]
pub struct RefuteRequest {
    /// Remove every cached failed-output entry whose `drv_hash`
    /// matches. Optional; unioned with `output_paths` and `worker_id`.
    #[serde(default)]
    pub drv_hash: Option<crate::types::DrvHash>,
    /// Specific output paths to remove. Empty when refuting by
    /// `drv_hash` or `worker_id` alone.
    #[serde(default)]
    pub output_paths: Vec<String>,
    /// Remove every cached failed-output entry that was reported by
    /// this worker. Primary operator escape hatch when a sick host's
    /// BuildFailure reports poisoned the cache: one call bulk-deletes
    /// every row that worker owns instead of walking them per-drv.
    /// Silently does nothing if no rows match — a worker_id without
    /// recent cache entries is a valid state (healthy worker).
    #[serde(default)]
    pub worker_id: Option<String>,
}

#[derive(serde::Serialize)]
pub struct RefuteResponse {
    /// Number of `failed_outputs` rows deleted. Callers log this
    /// so operators can distinguish "refute hit N entries" from
    /// "refute was a no-op" (typo, already expired, etc.).
    pub rows_affected: u64,
}
