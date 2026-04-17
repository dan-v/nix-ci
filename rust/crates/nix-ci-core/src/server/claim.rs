//! Two claim endpoints:
//!
//! * `GET /jobs/{id}/claim?wait=Ns&system=X&features=...` — per-job
//!   long-poll. Used by `nix-ci run`-style workers tied to a single
//!   job; returns 410 Gone when the job terminates.
//!
//! * `GET /claim?wait=Ns&system=X&features=...` — fleet long-poll.
//!   Scans every live submission in FIFO order (oldest job first)
//!   and returns the first claimable drv. Used by persistent
//!   `nix-ci worker` instances that are not tied to any one job.
//!   Returns 204 when nothing is claimable within the wait window;
//!   never 410, because there's no single job to be Gone.
//!
//! Pure in-memory: the dispatcher's `Submissions::pop_runnable` is the
//! only source of truth for what's claimable, and the claim itself is
//! recorded only in the in-memory `Claims` map. No DB writes on the
//! hot path.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::Utc;

use super::AppState;
use crate::dispatch::claim::ActiveClaim;
use crate::error::{Error, Result};
use crate::types::{
    ActiveClaimSummary, ClaimId, ClaimQuery, ClaimResponse, ClaimsListResponse, JobEvent, JobId,
};

#[tracing::instrument(skip_all, fields(job_id = %id, system = %q.system, worker = q.worker.as_deref()))]
pub async fn claim(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
    Query(mut q): Query<ClaimQuery>,
) -> Result<Response> {
    q.wait = q.wait.min(state.cfg.max_claim_wait_secs);

    let Some(sub) = state.dispatcher.submissions.get(id) else {
        // Submission absent: job is terminal, never existed, or the
        // coordinator just restarted and dropped in-flight state.
        // Workers interpret 410 as "stop polling."
        return Err(Error::Gone(format!("job {id} is terminal or unknown")));
    };

    let start = Instant::now();
    let deadline = start + Duration::from_secs(q.wait);
    let features_vec = q.features_vec();

    loop {
        // Check submission terminal status: a concurrent cancel / fail
        // / reap wakes the dispatcher, the notify below fires, and on
        // the next iteration we see the terminal flag and return 410
        // rather than sleeping to the wait deadline.
        if sub.is_terminal() {
            return Err(Error::Gone(format!("job {id} is terminal")));
        }

        let now_ms = Utc::now().timestamp_millis();
        if let Some(step) = sub.pop_runnable(&q.system, &features_vec, now_ms) {
            return issue_claim(&state, &sub, id, step, start, q.worker.clone());
        }

        if Instant::now() >= deadline {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }

        let wait = tokio::time::sleep_until(deadline.into());
        let notified = state.dispatcher.notify.notified();
        tokio::select! {
            () = notified => continue,
            () = wait => {
                if sub.is_terminal() {
                    return Err(Error::Gone(format!("job {id} is terminal")));
                }
                // A step may have become runnable at the exact tick the
                // timer fired. Retry once before the 204 to avoid an
                // immediate 30s-round-trip re-poll from the worker.
                let now_ms = Utc::now().timestamp_millis();
                if let Some(step) = sub.pop_runnable(&q.system, &features_vec, now_ms) {
                    return issue_claim(&state, &sub, id, step, start, q.worker.clone());
                }
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
        }
    }
}

/// Fleet claim: scan every live submission in FIFO order and return
/// the first runnable drv that matches `system` + `features`. Long-
/// polls via `dispatcher.notify` when nothing is claimable.
///
/// Fairness: oldest submission first (FIFO by `Submission.created_at`).
/// If the oldest job has more workers than runnable drvs (saturated),
/// subsequent workers naturally fall through to the next job — no
/// starvation in practice. A future `priority` field on
/// `CreateJobRequest` would extend the sort to `(-priority, created_at)`.
#[tracing::instrument(skip_all, fields(system = %q.system, worker = q.worker.as_deref()))]
pub async fn claim_any(
    State(state): State<AppState>,
    Query(mut q): Query<crate::types::ClaimQuery>,
) -> Result<Response> {
    q.wait = q.wait.min(state.cfg.max_claim_wait_secs);
    let start = Instant::now();
    let deadline = start + Duration::from_secs(q.wait);
    let features_vec = q.features_vec();

    loop {
        let now_ms = Utc::now().timestamp_millis();
        // Priority-first, then FIFO across submissions — dual-indexed
        // Submissions returns pre-sorted by (Reverse(priority),
        // created_at, id); no per-call O(N log N) sort, critical under
        // 1000+ workers racing on a single `notify_waiters()` edge.
        let subs = state.dispatcher.submissions.sorted_by_created_at();
        for sub in &subs {
            // Skip submissions already moving toward terminal — their
            // ready queues may still hold weak refs but their workers
            // are exiting.
            if sub.is_terminal() {
                continue;
            }
            // Per-job concurrency cap: already at the caller-configured
            // limit, let other submissions have this worker slot.
            if sub.at_worker_cap() {
                continue;
            }
            if let Some(step) = sub.pop_runnable(&q.system, &features_vec, now_ms) {
                let job_id = sub.id;
                return issue_claim(&state, sub, job_id, step, start, q.worker.clone());
            }
        }
        drop(subs);

        if Instant::now() >= deadline {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }

        let wait = tokio::time::sleep_until(deadline.into());
        let notified = state.dispatcher.notify.notified();
        tokio::select! {
            () = notified => continue,
            () = wait => {
                // One last sweep before 204 — a step may have just
                // become runnable.
                let now_ms = Utc::now().timestamp_millis();
                let subs = state.dispatcher.submissions.sorted_by_created_at();
                for sub in &subs {
                    if sub.is_terminal() {
                        continue;
                    }
                    if sub.at_worker_cap() {
                        continue;
                    }
                    if let Some(step) = sub.pop_runnable(&q.system, &features_vec, now_ms) {
                        let job_id = sub.id;
                        return issue_claim(&state, sub, job_id, step, start, q.worker.clone());
                    }
                }
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
        }
    }
}

/// `GET /claims` — point-in-time list of every active claim, sorted
/// longest-running first. Operator's main "what's stuck?" lever.
/// Cheap: snapshots the in-memory `Claims` map and returns. No DB.
pub async fn list_claims(State(state): State<AppState>) -> Result<axum::Json<ClaimsListResponse>> {
    let now = Instant::now();
    let mut summaries: Vec<ActiveClaimSummary> = state
        .dispatcher
        .claims
        .all()
        .into_iter()
        .map(|c| {
            let elapsed = now
                .saturating_duration_since(c.started_at)
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
            // Wall-clock deadline: if the deadline already passed,
            // the reaper hasn't run yet — Utc::now() + 0 is honest.
            let remaining = c.deadline.saturating_duration_since(now);
            let wall_deadline = Utc::now()
                + chrono::Duration::from_std(remaining).unwrap_or(chrono::Duration::zero());
            ActiveClaimSummary {
                claim_id: c.claim_id,
                job_id: c.job_id,
                drv_hash: c.drv_hash.clone(),
                attempt: c.attempt,
                worker_id: c.worker_id.clone(),
                claimed_at: c.started_at_wall,
                elapsed_ms: elapsed,
                deadline: wall_deadline,
            }
        })
        .collect();
    // Longest-running first — what an operator actually wants when
    // the question is "what's hung?"
    summaries.sort_by(|a, b| b.elapsed_ms.cmp(&a.elapsed_ms));
    Ok(axum::Json(ClaimsListResponse { claims: summaries }))
}

fn issue_claim(
    state: &AppState,
    sub: &Arc<crate::dispatch::Submission>,
    job_id: JobId,
    step: Arc<crate::dispatch::Step>,
    started_at: Instant,
    worker_id: Option<String>,
) -> Result<Response> {
    let attempt = step.tries.fetch_add(1, Ordering::AcqRel) + 1;
    let claim_id = ClaimId::new();
    // Per-job deadline override (C5): the caller can tighten the
    // claim deadline for a single job (e.g., "this job only builds
    // small leaf packages, cap at 10 min"). Falls back to the server
    // default when unset.
    let deadline_secs = sub
        .claim_deadline_secs
        .unwrap_or(state.cfg.claim_deadline_secs);
    let deadline_duration = Duration::from_secs(deadline_secs);
    let wall_deadline =
        Utc::now() + chrono::Duration::from_std(deadline_duration).unwrap_or_default();

    let now = Instant::now();
    let now_wall = Utc::now();
    state.dispatcher.claims.insert(Arc::new(ActiveClaim {
        claim_id,
        job_id,
        drv_hash: step.drv_hash().clone(),
        attempt,
        deadline: now + deadline_duration,
        started_at: now,
        started_at_wall: now_wall,
        worker_id,
    }));
    // Per-submission in-flight counter for max_workers cap. Decremented
    // in `complete` and in the claim-deadline reaper. `evict_claims_for`
    // doesn't decrement because its callers have just removed the
    // submission; the counter drops with the Arc.
    sub.active_claims.fetch_add(1, Ordering::AcqRel);

    sub.publish(JobEvent::DrvStarted {
        drv_hash: step.drv_hash().clone(),
        drv_name: step.drv_name().to_string(),
        claim_id,
        attempt,
    });

    state.metrics.inner.claims_issued.inc();
    state.metrics.inner.claims_in_flight.inc();
    state
        .metrics
        .inner
        .dispatch_wait_seconds
        .observe(started_at.elapsed().as_secs_f64());

    Ok(Json(ClaimResponse {
        job_id,
        claim_id,
        drv_hash: step.drv_hash().clone(),
        drv_path: step.drv_path().to_string(),
        attempt,
        deadline: wall_deadline,
    })
    .into_response())
}
