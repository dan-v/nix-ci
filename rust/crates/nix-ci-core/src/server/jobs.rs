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
    CreateJobRequest, CreateJobResponse, DrvFailure, DrvState, FailJobRequest, JobCounts, JobEvent,
    JobId, JobStatus, JobStatusResponse, SealJobResponse,
};

pub async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> Result<Json<CreateJobResponse>> {
    // Idempotent on external_ref: if the caller provided one and a job
    // with that ref already exists, return its id instead of minting
    // a new one. Protects against duplicate jobs when a POST /jobs
    // response is lost mid-flight and the caller retries.
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
    // Pre-create the in-memory submission so concurrent ingests can
    // attach without racing.
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
    writeback::seal_job(&state.pool, id).await?;
    // A sealed submission whose toplevels are already terminal
    // (including the empty-toplevels case) must transition to Done
    // immediately — otherwise the client waits forever for a
    // completion that will never come.
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
    writeback::fail_job_eval(&state.pool, id, &req.message).await?;
    transition_to(&state, id, JobStatus::Failed, Vec::new());
    Ok(Json(SealJobResponse {
        status: JobStatus::Failed,
    }))
}

pub async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
) -> Result<Json<SealJobResponse>> {
    writeback::transition_job_terminal(&state.pool, id, "cancelled").await?;
    transition_to(&state, id, JobStatus::Cancelled, Vec::new());
    Ok(Json(SealJobResponse {
        status: JobStatus::Cancelled,
    }))
}

/// Consolidated terminal-transition path for seal/fail/cancel. Does:
///   1. `mark_terminal` CAS on the in-memory submission
///   2. Publish `JobDone` event (only the winning CAS does this)
///   3. Increment the terminal metric
///   4. Remove the submission from the map (releases the graph)
///
/// Callers are responsible for the prior Postgres write so that a
/// crash between DB write and this call only orphans an in-memory
/// submission (harmless — the next coordinator restart will observe
/// the terminal row in PG and not rehydrate).
fn transition_to(state: &AppState, id: JobId, status: JobStatus, failures: Vec<DrvFailure>) {
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
}

pub async fn status(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
) -> Result<Json<JobStatusResponse>> {
    let row: Option<(String, bool, Option<String>)> =
        sqlx::query_as("SELECT status, sealed, eval_error FROM jobs WHERE id = $1")
            .bind(id.0)
            .fetch_optional(&state.pool)
            .await?;
    let (status_str, sealed, eval_error) =
        row.ok_or_else(|| Error::NotFound(format!("job {id}")))?;
    let status = parse_status(&status_str)?;

    let (counts, failures) = if let Some(sub) = state.dispatcher.submissions.get(id) {
        (sub.live_counts(), failures_from_submission(&sub))
    } else {
        (
            counts_from_db(&state.pool, id).await?,
            failures_from_db(&state.pool, id).await?,
        )
    };

    Ok(Json(JobStatusResponse {
        id,
        status,
        sealed,
        counts,
        failures,
        eval_error,
    }))
}

fn parse_status(s: &str) -> Result<JobStatus> {
    Ok(match s {
        "pending" => JobStatus::Pending,
        "building" => JobStatus::Building,
        "done" => JobStatus::Done,
        "failed" => JobStatus::Failed,
        "cancelled" => JobStatus::Cancelled,
        _ => return Err(Error::Internal(format!("unknown status: {s}"))),
    })
}

fn failures_from_submission(sub: &Arc<Submission>) -> Vec<DrvFailure> {
    sub.members
        .read()
        .values()
        .filter(|s| matches!(s.observable_state(), DrvState::Failed))
        .map(|s| DrvFailure {
            drv_hash: s.drv_hash().clone(),
            drv_name: s.drv_name().to_string(),
            error_category: crate::types::ErrorCategory::BuildFailure,
            error_message: None,
            log_tail: None,
            propagated_from: None,
        })
        .collect()
}

async fn counts_from_db(pool: &sqlx::PgPool, id: JobId) -> Result<JobCounts> {
    let row: (i64, i64, i64, i64, i64) = sqlx::query_as(
        r#"
        WITH RECURSIVE closure AS (
            SELECT drv_hash FROM job_roots WHERE job_id = $1
            UNION
            SELECT d.dep_hash FROM deps d JOIN closure c ON c.drv_hash = d.drv_hash
        )
        SELECT
            COUNT(*)                                   AS total,
            COUNT(*) FILTER (WHERE state = 'pending')  AS pending,
            COUNT(*) FILTER (WHERE state = 'building') AS building,
            COUNT(*) FILTER (WHERE state = 'done')     AS done,
            COUNT(*) FILTER (WHERE state = 'failed')   AS failed
        FROM derivations
        WHERE drv_hash IN (SELECT drv_hash FROM closure)
        "#,
    )
    .bind(id.0)
    .fetch_one(pool)
    .await?;
    Ok(JobCounts {
        total: row.0 as u32,
        pending: row.1 as u32,
        building: row.2 as u32,
        done: row.3 as u32,
        failed: row.4 as u32,
    })
}

type FailureRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

async fn failures_from_db(pool: &sqlx::PgPool, id: JobId) -> Result<Vec<DrvFailure>> {
    let rows: Vec<FailureRow> = sqlx::query_as(
        r#"
        WITH RECURSIVE closure AS (
            SELECT drv_hash FROM job_roots WHERE job_id = $1
            UNION
            SELECT d.dep_hash FROM deps d JOIN closure c ON c.drv_hash = d.drv_hash
        )
        SELECT drv_hash, drv_name, error_category, error_message, log_tail, propagated_from
        FROM derivations
        WHERE drv_hash IN (SELECT drv_hash FROM closure)
          AND state = 'failed'
        "#,
    )
    .bind(id.0)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(drv_hash, drv_name, cat, msg, log, prop)| DrvFailure {
            drv_hash: crate::types::DrvHash::new(drv_hash),
            drv_name,
            error_category: cat
                .as_deref()
                .and_then(parse_cat)
                .unwrap_or(crate::types::ErrorCategory::BuildFailure),
            error_message: msg,
            log_tail: log,
            propagated_from: prop.map(crate::types::DrvHash::new),
        })
        .collect())
}

fn parse_cat(s: &str) -> Option<crate::types::ErrorCategory> {
    use crate::types::ErrorCategory::*;
    Some(match s {
        "build_failure" => BuildFailure,
        "transient" => Transient,
        "disk_full" => DiskFull,
        "propagated_failure" => PropagatedFailure,
        _ => return None,
    })
}
