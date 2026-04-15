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
    CreateJobRequest, CreateJobResponse, DrvFailure, FailJobRequest, JobCounts, JobEvent, JobId,
    JobStatus, JobStatusResponse, SealJobResponse,
};

pub async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> Result<Json<CreateJobResponse>> {
    // Idempotent on external_ref: a retry with the same ref resolves
    // to the existing id (whatever its status — client decides what to
    // do with a cancelled / failed result).
    if let Some(ext) = req.external_ref.as_deref() {
        if let Some(existing) = writeback::find_job_by_external_ref(&state.pool, ext).await? {
            let _ = state
                .dispatcher
                .submissions
                .get_or_insert(existing, state.cfg.submission_event_capacity);
            tracing::debug!(%existing, external_ref = ext, "job create: external_ref hit");
            return Ok(Json(CreateJobResponse { id: existing }));
        }
    }
    let job_id = JobId::new();
    writeback::upsert_job(&state.pool, job_id, req.external_ref.as_deref()).await?;
    let _ = state
        .dispatcher
        .submissions
        .get_or_insert(job_id, state.cfg.submission_event_capacity);
    state.metrics.inner.jobs_created.inc();
    tracing::info!(%job_id, "job created");
    Ok(Json(CreateJobResponse { id: job_id }))
}

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
    // Sealing a submission whose toplevels are already terminal —
    // including the empty-toplevels case — must transition to Done
    // immediately or the client waits forever.
    crate::server::complete::check_and_publish_terminal(&state, &sub).await?;
    Ok(Json(SealJobResponse {
        status: sub.live_status(),
    }))
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
    let (counts, failures, sealed) = match state.dispatcher.submissions.get(id) {
        Some(sub) => (
            sub.live_counts(),
            crate::server::complete::cap_failures(
                sub.failures.read().clone(),
                state.cfg.max_failures_in_result,
            ),
            sub.is_sealed(),
        ),
        None => (JobCounts::default(), Vec::new(), false),
    };
    JobStatusResponse {
        id,
        status,
        sealed,
        counts,
        failures,
        eval_error,
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
    }
}
