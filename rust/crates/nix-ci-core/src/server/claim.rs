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

use super::complete::{check_and_publish_terminal, collect_submissions, propagate_failure_inmem};
use super::AppState;
use crate::dispatch::claim::ActiveClaim;
use crate::dispatch::Step;
use crate::error::{Error, Result};
use crate::observability::metrics::OutcomeLabels;
use crate::types::{
    ActiveClaimSummary, ClaimId, ClaimQuery, ClaimResponse, ClaimsListResponse, DrvFailure,
    ErrorCategory, JobEvent, JobId,
};

#[tracing::instrument(skip_all, fields(job_id = %id, system = %q.system, worker = q.worker.as_deref()))]
pub async fn claim(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
    Query(mut q): Query<ClaimQuery>,
) -> Result<Response> {
    q.wait = q.wait.min(state.cfg.max_claim_wait_secs);
    validate_worker_id(&q.worker, state.cfg.max_identifier_bytes)?;
    // Drain / fence short-circuit: return 204 immediately so the
    // worker exits its poll loop cleanly rather than holding the
    // connection open until the wait deadline. Long-polling
    // workers during a drain would otherwise leave claim_in_flight
    // RSS pinned for up to max_claim_wait_secs.
    if state.draining.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    if worker_is_fenced(&state, q.worker.as_deref()) {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

    let Some(sub) = state.dispatcher.submissions.get(id) else {
        // Submission absent: job is terminal, never existed, or the
        // coordinator just restarted and dropped in-flight state.
        // Workers interpret 410 as "stop polling."
        return Err(Error::Gone(format!("job {id} is terminal or unknown")));
    };

    let start = Instant::now();
    let deadline = start + Duration::from_secs(q.wait);
    let features_vec = q.features_vec();
    let systems_vec = q.systems_vec();
    if systems_vec.is_empty() {
        return Err(Error::BadRequest(
            "system query param must be non-empty".into(),
        ));
    }

    loop {
        // Check submission terminal status: a concurrent cancel / fail
        // / reap wakes the dispatcher, the notify below fires, and on
        // the next iteration we see the terminal flag and return 410
        // rather than sleeping to the wait deadline.
        if sub.is_terminal() {
            return Err(Error::Gone(format!("job {id} is terminal")));
        }

        let now_ms = Utc::now().timestamp_millis();
        if let Some(step) = sub.pop_runnable(&systems_vec, &features_vec, now_ms) {
            if step_exhausted(&step) {
                terminal_fail_exhausted(&state, &step).await?;
                continue;
            }
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
                if let Some(step) = sub.pop_runnable(&systems_vec, &features_vec, now_ms) {
                    if step_exhausted(&step) {
                        terminal_fail_exhausted(&state, &step).await?;
                        return Ok(StatusCode::NO_CONTENT.into_response());
                    }
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
    validate_worker_id(&q.worker, state.cfg.max_identifier_bytes)?;
    if state.draining.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    if worker_is_fenced(&state, q.worker.as_deref()) {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    let start = Instant::now();
    let deadline = start + Duration::from_secs(q.wait);
    let features_vec = q.features_vec();
    let systems_vec = q.systems_vec();
    if systems_vec.is_empty() {
        return Err(Error::BadRequest(
            "system query param must be non-empty".into(),
        ));
    }

    'outer: loop {
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
            if let Some(step) = sub.pop_runnable(&systems_vec, &features_vec, now_ms) {
                if step_exhausted(&step) {
                    drop(subs);
                    terminal_fail_exhausted(&state, &step).await?;
                    // Re-scan from the top: terminalizing this step
                    // may have propagated failures to other submissions
                    // and we want a consistent view before the next
                    // pop.
                    continue 'outer;
                }
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
                    if let Some(step) = sub.pop_runnable(&systems_vec, &features_vec, now_ms) {
                        if step_exhausted(&step) {
                            drop(subs);
                            terminal_fail_exhausted(&state, &step).await?;
                            return Ok(StatusCode::NO_CONTENT.into_response());
                        }
                        let job_id = sub.id;
                        return issue_claim(&state, sub, job_id, step, start, q.worker.clone());
                    }
                }
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
        }
    }
}

/// `DELETE /admin/claims/{claim_id}` — operator lever for unsticking
/// a worker that hasn't crossed the claim deadline but clearly isn't
/// making progress. Removes the claim synchronously (not via the
/// reaper so eviction is immediate, even if reaper ticks are long or
/// paused), decrements active_claims, and re-arms the step so another
/// worker can pick it up. Returns 404 if the claim doesn't exist.
#[tracing::instrument(skip_all, fields(claim_id = %id))]
pub async fn admin_evict_claim(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<ClaimId>,
) -> Result<Response> {
    let Some(claim) = state.dispatcher.claims.take(id) else {
        return Err(Error::NotFound(format!("claim {id}")));
    };
    state.metrics.inner.claims_in_flight.dec();
    state
        .metrics
        .inner
        .claim_age_seconds
        .observe(claim.started_at.elapsed().as_secs_f64());
    // Mirror the active_claims decrement on the owning submission so
    // the fleet-cap semantics don't drift.
    if let Some(sub) = state.dispatcher.submissions.get(claim.job_id) {
        sub.decrement_active_claim();
    }
    // Re-arm the step unless it's already finished (race with complete
    // / failure propagation). Use the same guarded pattern as the
    // deadline reaper: set runnable=true, re-check finished, undo if
    // the check flipped to true to preserve dispatcher invariant 4.
    if let Some(step) = state.dispatcher.steps.get(&claim.drv_hash) {
        use std::sync::atomic::Ordering;
        if !step.finished.load(Ordering::Acquire) {
            step.runnable.store(true, Ordering::Release);
            if step.finished.load(Ordering::Acquire) {
                step.runnable.store(false, Ordering::Release);
            } else {
                crate::dispatch::rdep::enqueue_for_all_submissions(&step);
            }
        }
    }
    state.dispatcher.wake();
    tracing::warn!(%id, "admin: claim force-evicted");
    Ok(StatusCode::NO_CONTENT.into_response())
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
            let remaining = c.deadline.lock().saturating_duration_since(now);
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

/// True when the worker identified by `worker_id` is in the admin
/// fence set. Missing (None) worker_ids are never fenced — the fence
/// is an explicit opt-in for known hosts. Read-only snapshot via the
/// RwLock; no `.await` under the guard.
fn worker_is_fenced(state: &AppState, worker_id: Option<&str>) -> bool {
    let Some(w) = worker_id else { return false };
    state.fenced_workers.read().contains(w)
}

/// Returns true when a popped step has already consumed its full
/// `max_tries` budget — the next claim would be attempt `max_tries + 1`,
/// violating the retry policy. Checked before `issue_claim` so a step
/// that's been re-armed by the reaper after repeated worker crashes
/// doesn't silently get another retry beyond the cap.
fn step_exhausted(step: &Arc<Step>) -> bool {
    step.tries.load(Ordering::Acquire) >= step.max_tries()
}

/// Terminal-fail a step whose retry budget is exhausted. Runs the same
/// in-memory propagation path as a worker-reported BuildFailure would,
/// minus the `failed_outputs` TTL cache insert: exhaustion after
/// transient failures / claim expiries isn't strong enough evidence
/// that the drv itself is bad (a flaky network or a repeatedly-crashing
/// worker looks identical), so we don't poison the dedup cache.
///
/// The step may already be finished by a concurrent failure
/// propagation path — guard with `finished` before doing any work so
/// this helper is idempotent.
async fn terminal_fail_exhausted(state: &AppState, step: &Arc<Step>) -> Result<()> {
    if step.finished.load(Ordering::Acquire) {
        return Ok(());
    }
    step.previous_failure.store(true, Ordering::Release);
    step.finished.store(true, Ordering::Release);

    let tries = step.tries.load(Ordering::Acquire);
    let max = step.max_tries();
    let msg = format!(
        "max_retries_exceeded: {tries} attempts completed (cap {max}); \
         last claim expired before reporting"
    );

    state
        .metrics
        .inner
        .builds_completed
        .get_or_create(&OutcomeLabels {
            outcome: "max_retries_exceeded".into(),
        })
        .inc();

    let subs = collect_submissions(step);
    let failure = DrvFailure {
        drv_hash: step.drv_hash().clone(),
        drv_name: step.drv_name().to_string(),
        // Transient is the honest classification: we can't tell whether
        // the drv is actually unbuildable or the builds kept hitting
        // infra issues. The message makes the distinction clear.
        error_category: ErrorCategory::Transient,
        error_message: Some(msg.clone()),
        log_tail: None,
        propagated_from: None,
    };
    for sub in &subs {
        sub.record_failure(failure.clone());
        sub.publish(JobEvent::DrvFailed {
            drv_hash: step.drv_hash().clone(),
            drv_name: step.drv_name().to_string(),
            error_category: ErrorCategory::Transient,
            error_message: Some(msg.clone()),
            log_tail: None,
            attempt: tries,
            will_retry: false,
            used_by_attrs: Vec::new(),
        });
    }

    let propagated = propagate_failure_inmem(step, step.drv_hash());
    state
        .metrics
        .inner
        .propagated_failures
        .inc_by(propagated);

    for sub in &subs {
        check_and_publish_terminal(state, sub).await?;
    }
    state.dispatcher.wake();
    tracing::warn!(
        drv = %step.drv_hash(),
        tries,
        max,
        "terminal-failed step after max_retries_exceeded"
    );
    Ok(())
}

/// `POST /jobs/{id}/claims/{claim_id}/extend` — lease refresh for a
/// worker whose build is taking longer than the initial claim deadline.
/// Extends by the full `deadline_window` stored on the claim so behavior
/// is stable across server-config drift.
///
/// Returns 410 Gone when the claim is unknown (already completed,
/// evicted, or reaped). The worker uses that as the signal to stop
/// refreshing: at that point another worker may already have re-claimed
/// the drv and there's nothing useful for us to extend.
#[tracing::instrument(skip_all, fields(job_id = %id, claim_id = %claim_id))]
pub async fn extend_claim(
    State(state): State<AppState>,
    Path((id, claim_id)): Path<(JobId, ClaimId)>,
) -> Result<Json<crate::types::ExtendClaimResponse>> {
    // job_id in the path is for symmetry with the other claim endpoints
    // and to let log aggregators group by job; the claim itself carries
    // its own job_id so we don't use the path argument for lookup.
    let _ = id;
    let now = Instant::now();
    let Some(new_deadline) = state.dispatcher.claims.extend(claim_id, now) else {
        return Err(Error::Gone(format!(
            "claim {claim_id} is no longer active; stop refreshing"
        )));
    };
    state.metrics.inner.claim_lease_extensions.inc();
    let remaining = new_deadline.saturating_duration_since(now);
    let wall_deadline = Utc::now()
        + chrono::Duration::from_std(remaining).unwrap_or(chrono::Duration::zero());
    Ok(Json(crate::types::ExtendClaimResponse {
        deadline: wall_deadline,
    }))
}

/// Reject an over-long `worker_id` at the claim entry point so the
/// value never reaches tracing spans, the ActiveClaim record, or
/// `/claims` output. An unbounded string from a broken client would
/// bloat logs and serialized claim snapshots. No-op when `None`.
fn validate_worker_id(worker_id: &Option<String>, max_bytes: usize) -> Result<()> {
    if let Some(w) = worker_id.as_deref() {
        if w.len() > max_bytes {
            return Err(Error::BadRequest(format!(
                "worker_id exceeds max_identifier_bytes ({} > {max_bytes})",
                w.len()
            )));
        }
    }
    Ok(())
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
        deadline: parking_lot::Mutex::new(now + deadline_duration),
        deadline_window: deadline_duration,
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
