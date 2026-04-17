//! Job lifecycle handlers: create, seal, fail, cancel, status.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use super::AppState;
use crate::dispatch::Submission;
use crate::durable::writeback;
use crate::error::{Error, Result};
use crate::observability::metrics::TerminalLabels;
use crate::types::{
    CreateJobRequest, CreateJobResponse, DrvFailure, ErrorCategory, FailJobRequest, JobCounts,
    JobEvent, JobId, JobStatus, JobStatusResponse, JobSummary, JobsListResponse, SealJobResponse,
};

#[tracing::instrument(skip_all, fields(
    external_ref = req.external_ref.as_deref(),
    priority = req.priority,
    max_workers = req.max_workers,
))]
pub async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> Result<Json<CreateJobResponse>> {
    // Bounded identifier: external_ref propagates into DB text columns,
    // log lines, and JSONB snapshots. An unbounded value from a broken
    // or hostile client would bloat every row and line it touches.
    if let Some(ext) = req.external_ref.as_deref() {
        if ext.len() > state.cfg.max_identifier_bytes {
            return Err(Error::BadRequest(format!(
                "external_ref exceeds max_identifier_bytes ({} > {})",
                ext.len(),
                state.cfg.max_identifier_bytes
            )));
        }
    }
    // Idempotent on external_ref: a retry with the same ref resolves
    // to the existing id (whatever its status — client decides what to
    // do with a cancelled / failed result). On hit, the existing
    // submission keeps its original priority / max_workers — we don't
    // retroactively mutate scheduling params on an in-flight job.
    if let Some(ext) = req.external_ref.as_deref() {
        if let Some(existing) = writeback::find_job_by_external_ref(&state.pool, ext).await? {
            let _ = state.dispatcher.submissions.get_or_insert_with_options(
                existing,
                state.cfg.submission_event_capacity,
                req.priority,
                req.max_workers,
                req.claim_deadline_secs,
            );
            tracing::debug!(%existing, external_ref = ext, "job create: external_ref hit");
            return Ok(Json(CreateJobResponse { id: existing }));
        }
    }
    let job_id = JobId::new();
    writeback::upsert_job(&state.pool, job_id, req.external_ref.as_deref()).await?;
    let _ = state.dispatcher.submissions.get_or_insert_with_options(
        job_id,
        state.cfg.submission_event_capacity,
        req.priority,
        req.max_workers,
        req.claim_deadline_secs,
    );
    state.metrics.inner.jobs_created.inc();
    tracing::info!(%job_id, "job created");
    Ok(Json(CreateJobResponse { id: job_id }))
}

#[tracing::instrument(skip_all, fields(job_id = %id))]
pub async fn seal(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
) -> Result<Json<SealJobResponse>> {
    let Some(sub) = state.dispatcher.submissions.get(id) else {
        return Err(Error::NotFound(format!("job {id}")));
    };
    sub.seal();
    if !writeback::seal_job(&state.pool, id).await? {
        return Err(Error::NotFound(format!("job {id}")));
    }

    // Cycle detection (C2). The graph is immutable from here on
    // (ingest rejects post-seal via `reject_if_terminal`) so one pass
    // is authoritative. A cycle means no progress will ever be made —
    // every step in the cycle sits runnable=false forever — so we fail
    // the job with a clear cause rather than silently stalling until
    // the heartbeat reaper cancels it.
    if let Some(cycle) = sub.detect_cycle() {
        let preview: Vec<String> = cycle
            .iter()
            .take(5)
            .map(|h| h.as_str().to_string())
            .collect();
        let reason = format!(
            "dep_cycle: {} drv(s) on the cycle (showing up to 5): [{}]",
            cycle.len(),
            preview.join(", ")
        );
        tracing::warn!(job_id = %id, cycle_len = cycle.len(), "seal rejected: dep cycle");
        auto_fail_on_seal(&state, id, &reason).await?;
        return Ok(Json(SealJobResponse {
            status: JobStatus::Failed,
        }));
    }

    // Sealing a submission whose toplevels are already terminal —
    // including the empty-toplevels case — must transition to Done
    // immediately or the client waits forever.
    crate::server::complete::check_and_publish_terminal(&state, &sub).await?;
    Ok(Json(SealJobResponse {
        status: sub.live_status(),
    }))
}

/// Shared auto-fail path used by seal-time cycle detection (and any
/// future seal-time validation failure). Persists a terminal snapshot
/// with `eval_error` set, removes the submission, and publishes
/// `JobDone` so any subscribers (SSE, long-poll claims) wake up.
async fn auto_fail_on_seal(state: &AppState, id: JobId, reason: &str) -> Result<()> {
    let eval_errors = state
        .dispatcher
        .submissions
        .get(id)
        .map(|sub| sub.eval_errors.read().clone())
        .unwrap_or_default();
    let snapshot = JobStatusResponse {
        id,
        status: JobStatus::Failed,
        sealed: true,
        counts: JobCounts::default(),
        failures: Vec::new(),
        eval_error: Some(reason.to_string()),
        eval_errors,
    };
    let snapshot_json = serde_json::to_value(&snapshot)
        .map_err(|e| Error::Internal(format!("serialize seal-fail snapshot: {e}")))?;
    let _ = writeback::transition_job_terminal(
        &state.pool,
        id,
        JobStatus::Failed.as_str(),
        &snapshot_json,
    )
    .await?;
    finish_in_memory(state, id, JobStatus::Failed, Vec::new());
    Ok(())
}

pub async fn fail(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
    Json(req): Json<FailJobRequest>,
) -> Result<Json<SealJobResponse>> {
    let snapshot =
        build_terminal_snapshot(&state, id, JobStatus::Failed, Some(req.message.clone()));
    let snapshot_json = serde_json::to_value(&snapshot)
        .map_err(|e| Error::Internal(format!("serialize fail result: {e}")))?;
    let _ = writeback::transition_job_terminal(
        &state.pool,
        id,
        JobStatus::Failed.as_str(),
        &snapshot_json,
    )
    .await?;
    finish_in_memory(&state, id, JobStatus::Failed, Vec::new());
    Ok(Json(SealJobResponse {
        status: JobStatus::Failed,
    }))
}

pub async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
) -> Result<Json<SealJobResponse>> {
    let snapshot = build_terminal_snapshot(&state, id, JobStatus::Cancelled, None);
    let snapshot_json = serde_json::to_value(&snapshot)
        .map_err(|e| Error::Internal(format!("serialize cancel result: {e}")))?;
    let _ = writeback::transition_job_terminal(
        &state.pool,
        id,
        JobStatus::Cancelled.as_str(),
        &snapshot_json,
    )
    .await?;
    finish_in_memory(&state, id, JobStatus::Cancelled, Vec::new());
    Ok(Json(SealJobResponse {
        status: JobStatus::Cancelled,
    }))
}

/// Build a JobStatusResponse snapshot from the live submission (if
/// present) for persistence as `jobs.result`. Used by cancel/fail which
/// force a terminal state before the dispatcher's usual path runs.
fn build_terminal_snapshot(
    state: &AppState,
    id: JobId,
    status: JobStatus,
    eval_error: Option<String>,
) -> JobStatusResponse {
    let (counts, failures, sealed, eval_errors) = match state.dispatcher.submissions.get(id) {
        Some(sub) => (
            sub.live_counts(),
            crate::server::complete::cap_failures(
                sub.failures.read().clone(),
                state.cfg.max_failures_in_result,
            ),
            sub.is_sealed(),
            sub.eval_errors.read().clone(),
        ),
        None => (JobCounts::default(), Vec::new(), false, Vec::new()),
    };
    JobStatusResponse {
        id,
        status,
        sealed,
        counts,
        failures,
        eval_error,
        eval_errors,
    }
}

/// In-memory cleanup for cancel/fail: remove the submission, CAS
/// terminal, publish JobDone, bump metrics. Durable write has already
/// happened.
///
/// We always wake the dispatcher afterward so any in-flight long-poll
/// `/claim` on this job notices the terminal state and returns 410
/// instead of sleeping until its wait deadline expires.
fn finish_in_memory(state: &AppState, id: JobId, status: JobStatus, failures: Vec<DrvFailure>) {
    let Some(sub) = state.dispatcher.submissions.remove(id) else {
        return;
    };
    // Cancel/fail can race with in-flight workers — drop their claims
    // synchronously so the gauge balances and we don't pin the step's
    // drv_hash until the claim's deadline expires.
    state.dispatcher.evict_claims_for(id);
    if sub.mark_terminal() {
        sub.publish(JobEvent::JobDone { status, failures });
    }
    state
        .metrics
        .inner
        .jobs_terminal
        .get_or_create(&TerminalLabels {
            status: status.as_str().into(),
        })
        .inc();
    // Per-job size at terminal (mirror of the path in
    // `complete::check_and_publish_terminal`). Captured here too so
    // cancel/fail-driven terminations show up in `drvs_per_job`.
    state
        .metrics
        .inner
        .drvs_per_job
        .observe(sub.members.read().len() as f64);
    state.dispatcher.wake();
}

pub async fn status(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
) -> Result<Json<JobStatusResponse>> {
    // Live job: build response from the in-memory submission — cheapest
    // path and freshest data.
    if let Some(sub) = state.dispatcher.submissions.get(id) {
        return Ok(Json(response_from_live(&sub)));
    }
    // Terminal job: read the JSONB snapshot persisted at terminal time.
    let row: Option<(serde_json::Value,)> = sqlx::query_as("SELECT result FROM jobs WHERE id = $1")
        .bind(id.0)
        .fetch_optional(&state.pool)
        .await?;
    match row {
        Some((result,)) => {
            let snap: JobStatusResponse = serde_json::from_value(result)
                .map_err(|e| Error::Internal(format!("parse jobs.result: {e}")))?;
            Ok(Json(snap))
        }
        None => Err(Error::NotFound(format!("job {id}"))),
    }
}

fn response_from_live(sub: &Arc<Submission>) -> JobStatusResponse {
    JobStatusResponse {
        id: sub.id,
        status: sub.live_status(),
        sealed: sub.is_sealed(),
        counts: sub.live_counts(),
        failures: sub.failures.read().clone(),
        eval_error: None,
        eval_errors: sub.eval_errors.read().clone(),
    }
}

/// Default `?limit=` for `GET /jobs`. Capped server-side regardless.
const DEFAULT_LIST_LIMIT: i64 = 50;
/// Hard server-side cap on `?limit=`. Prevents accidentally pulling
/// thousands of rows.
const MAX_LIST_LIMIT: i64 = 200;

#[derive(Debug, serde::Deserialize)]
pub struct JobsListQuery {
    /// Required. Today only `failed`/`done`/`cancelled`/`pending`/`building`
    /// are meaningful — anything else returns an empty list.
    pub status: String,
    /// Cursor: return rows with `done_at` strictly before this. Pass
    /// the previous response's `next_cursor` for the following page.
    #[serde(default)]
    pub cursor: Option<chrono::DateTime<chrono::Utc>>,
    /// Earliest `done_at` to consider. Mutually compatible with
    /// `cursor` (both narrow the upper bound; `since` narrows the
    /// lower).
    #[serde(default)]
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /jobs?status=...&since=...&cursor=...&limit=N` — operator-
/// facing list of recent jobs in a status. Cursor pagination on
/// `done_at DESC`. Each row is a `JobSummary` populated from the
/// terminal-snapshot JSONB stored in `jobs.result`.
pub async fn list(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<JobsListQuery>,
) -> Result<Json<JobsListResponse>> {
    let limit = q
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let mut rows = writeback::list_jobs_by_status(&state.pool, &q.status, q.cursor, limit).await?;
    // Apply `since` filter in-memory — the SQL only does upper-bound
    // pagination because we need cursor + since combined.
    if let Some(since) = q.since {
        rows.retain(|r| r.done_at.map(|d| d >= since).unwrap_or(false));
    }
    // We over-fetched by 1 to detect "more available". If we did,
    // drop the extra row AND set the cursor to the LAST RETURNED
    // row's done_at — the next page must use `done_at < cursor` so
    // the row that was the cursor isn't returned again. Setting the
    // cursor to the dropped row's done_at would silently SKIP that
    // row on the next page.
    let next_cursor = if rows.len() > limit as usize {
        rows.truncate(limit as usize);
        rows.last().and_then(|r| r.done_at)
    } else {
        None
    };
    let jobs: Vec<JobSummary> = rows
        .into_iter()
        .map(|r| {
            job_summary_from_row(
                r.id,
                r.external_ref,
                &r.status,
                r.done_at,
                r.result.as_ref(),
            )
        })
        .collect();
    Ok(Json(JobsListResponse { jobs, next_cursor }))
}

#[derive(Debug, serde::Deserialize)]
pub struct ByExternalRefPath {
    pub external_ref: String,
}

/// `GET /jobs/by-external-ref/{ref}` — gateway from "I have a CCI
/// build ID / PR number / opaque ref" to the job's status snapshot.
/// 404 if no job has that ref. Used by `nix-ci show <ref>`.
pub async fn by_external_ref(
    State(state): State<AppState>,
    Path(ByExternalRefPath { external_ref }): Path<ByExternalRefPath>,
) -> Result<Json<JobStatusResponse>> {
    let lookup = writeback::lookup_job_by_external_ref(&state.pool, &external_ref)
        .await?
        .ok_or_else(|| Error::NotFound(format!("job ext_ref={external_ref}")))?;
    // Live-job path: prefer the in-memory snapshot for freshness.
    if let Some(sub) = state.dispatcher.submissions.get(lookup.id) {
        return Ok(Json(response_from_live(&sub)));
    }
    // Terminal job: use the stored result JSONB.
    let snap = lookup
        .result
        .ok_or_else(|| Error::NotFound(format!("job {} has no terminal snapshot", lookup.id)))?;
    let snap: JobStatusResponse = serde_json::from_value(snap)
        .map_err(|e| Error::Internal(format!("parse jobs.result: {e}")))?;
    Ok(Json(snap))
}

/// Build a `JobSummary` from a stored row + its (possibly absent)
/// terminal snapshot. Picks out originating failures (capped at 3) +
/// propagated count for the operator overview.
pub(crate) fn job_summary_from_row(
    id: JobId,
    external_ref: Option<String>,
    status: &str,
    done_at: Option<chrono::DateTime<chrono::Utc>>,
    result: Option<&serde_json::Value>,
) -> JobSummary {
    let mut originating: Vec<String> = Vec::new();
    let mut originating_total: u32 = 0;
    let mut propagated: u32 = 0;
    if let Some(snap) = result {
        if let Ok(snap) = serde_json::from_value::<JobStatusResponse>(snap.clone()) {
            for f in &snap.failures {
                if matches!(f.error_category, ErrorCategory::PropagatedFailure) {
                    propagated += 1;
                } else {
                    originating_total += 1;
                    if originating.len() < 3 {
                        originating.push(f.drv_name.clone());
                    }
                }
            }
        }
    }
    JobSummary {
        id,
        external_ref,
        status: parse_status(status).unwrap_or(JobStatus::Pending),
        done_at,
        originating_failures: originating,
        originating_failures_total: originating_total,
        propagated_failures: propagated,
    }
}

fn parse_status(s: &str) -> Option<JobStatus> {
    match s {
        "pending" => Some(JobStatus::Pending),
        "building" => Some(JobStatus::Building),
        "done" => Some(JobStatus::Done),
        "failed" => Some(JobStatus::Failed),
        "cancelled" => Some(JobStatus::Cancelled),
        _ => None,
    }
}
