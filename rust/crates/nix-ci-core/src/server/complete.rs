//! `POST /jobs/{id}/claims/{claim_id}/complete`
//!
//! Pure in-memory state transitions on the drv graph. The only durable
//! writes are:
//!   * `failed_outputs` insert when a drv terminal-fails (TTL cache so
//!     concurrent / subsequent jobs can short-circuit a known-bad drv).
//!   * `jobs.status/done_at/result` when the submission itself reaches
//!     a terminal state.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use super::AppState;
use crate::dispatch::rdep::make_rdeps_runnable;
use crate::dispatch::Submission;
use crate::durable::writeback;
use crate::error::{Error, Result};
use crate::observability::metrics::{OutcomeLabels, TerminalLabels};
use crate::types::{
    ClaimId, CompleteRequest, CompleteResponse, DrvFailure, DrvHash, ErrorCategory, JobEvent,
    JobId, JobStatus, JobStatusResponse, MAX_LOG_TAIL_BYTES,
};

pub async fn complete(
    State(state): State<AppState>,
    Path((job_id, claim_id)): Path<(JobId, ClaimId)>,
    Json(mut req): Json<CompleteRequest>,
) -> Result<Json<CompleteResponse>> {
    let Some(claim) = state.dispatcher.claims.take(claim_id) else {
        return Ok(Json(CompleteResponse {
            ignored: true,
        }));
    };
    state.metrics.inner.claims_in_flight.dec();

    if claim.job_id != job_id {
        return Err(Error::BadRequest(
            "claim_id doesn't belong to this job".into(),
        ));
    }

    let Some(step) = state.dispatcher.steps.get(&claim.drv_hash) else {
        return Ok(Json(CompleteResponse {
            ignored: true,
        }));
    };

    // Guard against the failure-propagation race: if propagation already
    // marked this step finished, skip processing so we don't double-
    // count metrics or re-fire events.
    if step.finished.load(Ordering::Acquire) {
        return Ok(Json(CompleteResponse {
            ignored: true,
        }));
    }

    truncate_log(&mut req.log_tail);

    state
        .metrics
        .inner
        .build_duration
        .observe((req.duration_ms as f64) / 1000.0);

    if req.success {
        handle_success(&state, &step).await?;
    } else {
        handle_failure(&state, &claim, &step, req).await?;
    }
    Ok(Json(CompleteResponse {
        ignored: false,
    }))
}

fn truncate_log(log: &mut Option<String>) {
    let Some(s) = log else { return };
    if s.len() <= MAX_LOG_TAIL_BYTES {
        return;
    }
    let target = s.len() - MAX_LOG_TAIL_BYTES;
    let mut cut = target;
    while cut < s.len() && !s.is_char_boundary(cut) {
        cut += 1;
    }
    *s = s[cut..].to_string();
}

async fn handle_success(
    state: &AppState,
    step: &Arc<crate::dispatch::Step>,
) -> Result<()> {
    step.finished.store(true, Ordering::Release);

    state
        .metrics
        .inner
        .builds_completed
        .get_or_create(&OutcomeLabels {
            outcome: "success".into(),
        })
        .inc();

    let subs = collect_submissions(step);
    for sub in &subs {
        sub.publish(JobEvent::DrvCompleted {
            drv_hash: step.drv_hash().clone(),
            drv_name: step.drv_name().to_string(),
            duration_ms: 0,
        });
    }

    make_rdeps_runnable(step);

    for sub in &subs {
        check_and_publish_terminal(state, sub).await?;
    }
    state.dispatcher.wake();
    Ok(())
}

async fn handle_failure(
    state: &AppState,
    claim: &crate::dispatch::claim::ActiveClaim,
    step: &Arc<crate::dispatch::Step>,
    req: CompleteRequest,
) -> Result<()> {
    let category = req.error_category.unwrap_or(ErrorCategory::BuildFailure);
    let attempt = claim.attempt;
    let can_retry = category.is_retryable() && attempt < step.max_tries();

    if can_retry {
        return handle_flaky_retry(state, step, req, category, attempt).await;
    }

    step.previous_failure.store(true, Ordering::Release);
    step.finished.store(true, Ordering::Release);

    // Best-effort: cache the output path (drv_path with .drv stripped)
    // so concurrent jobs short-circuit on the next ingest. Not fatal on
    // error.
    let output_path = step.drv_path().trim_end_matches(".drv").to_string();
    if !output_path.is_empty() && output_path != step.drv_path() {
        if let Err(e) =
            writeback::insert_failed_outputs(&state.pool, step.drv_hash(), &[output_path]).await
        {
            tracing::warn!(error = %e, "failed_outputs insert failed; continuing");
        }
    }

    state
        .metrics
        .inner
        .builds_completed
        .get_or_create(&OutcomeLabels {
            outcome: "failure".into(),
        })
        .inc();

    let subs = collect_submissions(step);

    // Record the originating failure on every submission that owns this
    // step. The DrvFailure is what the terminal `jobs.result` snapshot
    // and `GET /jobs/{id}` will return for this drv.
    let failure = DrvFailure {
        drv_hash: step.drv_hash().clone(),
        drv_name: step.drv_name().to_string(),
        error_category: category,
        error_message: req.error_message.clone(),
        log_tail: req.log_tail.clone(),
        propagated_from: None,
    };
    for sub in &subs {
        sub.record_failure(failure.clone());
    }
    publish_drv_failed(&subs, step, category, &req, attempt, false);

    let propagated_count = propagate_failure_inmem(step, step.drv_hash(), &subs);
    if propagated_count > 0 {
        state
            .metrics
            .inner
            .propagated_failures
            .inc_by(propagated_count);
    }

    for sub in subs {
        check_and_publish_terminal(state, &sub).await?;
    }
    state.dispatcher.wake();
    Ok(())
}

async fn handle_flaky_retry(
    state: &AppState,
    step: &Arc<crate::dispatch::Step>,
    req: CompleteRequest,
    category: ErrorCategory,
    attempt: i32,
) -> Result<()> {
    let backoff_ms = state.cfg.flaky_retry_backoff_step_ms * i64::from(attempt);
    step.next_attempt_at.store(
        chrono::Utc::now().timestamp_millis() + backoff_ms,
        Ordering::Release,
    );
    if step
        .runnable
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        crate::dispatch::rdep::enqueue_for_all_submissions(step);
    }
    let subs = collect_submissions(step);
    publish_drv_failed(&subs, step, category, &req, attempt, true);
    state
        .metrics
        .inner
        .builds_completed
        .get_or_create(&OutcomeLabels {
            outcome: "flaky_retry".into(),
        })
        .inc();
    state.dispatcher.wake();
    Ok(())
}

fn collect_submissions(step: &Arc<crate::dispatch::Step>) -> Vec<Arc<Submission>> {
    let st = step.state.read();
    st.submissions.iter().filter_map(|w| w.upgrade()).collect()
}

fn publish_drv_failed(
    subs: &[Arc<Submission>],
    step: &Arc<crate::dispatch::Step>,
    category: ErrorCategory,
    req: &CompleteRequest,
    attempt: i32,
    will_retry: bool,
) {
    for sub in subs {
        sub.publish(JobEvent::DrvFailed {
            drv_hash: step.drv_hash().clone(),
            drv_name: step.drv_name().to_string(),
            error_category: category,
            error_message: req.error_message.clone(),
            log_tail: req.log_tail.clone(),
            attempt,
            will_retry,
        });
    }
}

/// Flip every transitive rdep of `root` to terminal-failed, recording a
/// propagated `DrvFailure` on every submission that owns it. Returns
/// the number of drvs newly failed so callers can bump metrics.
fn propagate_failure_inmem(
    root: &Arc<crate::dispatch::Step>,
    origin: &DrvHash,
    origin_subs: &[Arc<Submission>],
) -> u64 {
    use std::collections::VecDeque;
    let mut frontier: VecDeque<Arc<crate::dispatch::Step>> = VecDeque::new();
    {
        let st = root.state.read();
        for w in &st.rdeps {
            if let Some(r) = w.upgrade() {
                frontier.push_back(r);
            }
        }
    }
    let mut count = 0u64;
    while let Some(step) = frontier.pop_front() {
        if step.finished.load(Ordering::Acquire) {
            continue;
        }
        step.previous_failure.store(true, Ordering::Release);
        step.finished.store(true, Ordering::Release);
        count += 1;

        let drv_subs = collect_submissions(&step);
        // Union of origin-subs and this drv's subs — record the
        // propagated failure on every submission that can see this drv.
        for sub in origin_subs.iter().chain(drv_subs.iter()) {
            sub.record_failure(DrvFailure {
                drv_hash: step.drv_hash().clone(),
                drv_name: step.drv_name().to_string(),
                error_category: ErrorCategory::PropagatedFailure,
                error_message: Some(format!("dep failed: {origin}")),
                log_tail: None,
                propagated_from: Some(origin.clone()),
            });
        }

        let next: Vec<Arc<crate::dispatch::Step>> = {
            let st = step.state.read();
            st.rdeps.iter().filter_map(|w| w.upgrade()).collect()
        };
        frontier.extend(next);
    }
    count
}

/// Transition the submission to terminal if its toplevels are all
/// finished and it's sealed. Writes the final `JobStatusResponse`
/// snapshot to `jobs.result` and publishes `JobDone`.
pub(super) async fn check_and_publish_terminal(
    state: &AppState,
    sub: &Arc<Submission>,
) -> Result<()> {
    if !sub.is_sealed() {
        return Ok(());
    }
    if sub.terminal.load(Ordering::Acquire) {
        return Ok(());
    }
    let tops = sub.toplevels.read().clone();
    let all_finished = tops.iter().all(|t| t.finished.load(Ordering::Acquire));
    if !all_finished {
        return Ok(());
    }
    let any_failed = tops
        .iter()
        .any(|t| t.previous_failure.load(Ordering::Acquire));
    let final_status = if any_failed {
        JobStatus::Failed
    } else {
        JobStatus::Done
    };

    let counts = sub.live_counts();
    let failures = sub.failures.read().clone();
    let snapshot = JobStatusResponse {
        id: sub.id,
        status: final_status,
        sealed: true,
        counts,
        failures: failures.clone(),
        eval_error: None,
    };
    let snapshot_json = serde_json::to_value(&snapshot)
        .map_err(|e| Error::Internal(format!("serialize terminal result: {e}")))?;
    let _ = writeback::transition_job_terminal(
        &state.pool,
        sub.id,
        final_status.as_str(),
        &snapshot_json,
    )
    .await?;

    if !sub.mark_terminal() {
        return Ok(());
    }

    sub.publish(JobEvent::JobDone {
        status: final_status,
        failures,
    });
    state
        .metrics
        .inner
        .jobs_terminal
        .get_or_create(&TerminalLabels {
            status: final_status.as_str().into(),
        })
        .inc();

    // Drop the in-memory submission. Existing SSE subscribers keep
    // their receiver until the broadcast Sender is dropped with the
    // last Arc<Submission>. New subscribers get 404 and should poll
    // `/jobs/{id}` for the terminal result instead.
    state.dispatcher.submissions.remove(sub.id);
    Ok(())
}
