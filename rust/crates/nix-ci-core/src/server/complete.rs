//! `POST /jobs/{id}/claims/{claim_id}/complete`

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use super::AppState;
use crate::dispatch::rdep::make_rdeps_runnable;
use crate::dispatch::Submission;
use crate::durable::writeback::{self, MarkFailed};
use crate::error::{Error, Result};
use crate::observability::metrics::{OutcomeLabels, TerminalLabels};
use crate::types::{
    ClaimId, CompleteRequest, CompleteResponse, DrvFailure, ErrorCategory, JobEvent, JobId,
    JobStatus, MAX_LOG_TAIL_BYTES,
};

pub async fn complete(
    State(state): State<AppState>,
    Path((job_id, claim_id)): Path<(JobId, ClaimId)>,
    Json(mut req): Json<CompleteRequest>,
) -> Result<Json<CompleteResponse>> {
    // Retrieve the active claim; if it's gone, the completion is stale.
    let Some(claim) = state.dispatcher.claims.take(claim_id) else {
        return Ok(Json(CompleteResponse {
            ignored: true,
            next_build: None,
        }));
    };
    // Decrement gauge for every taken claim, including the paths below
    // where we early-return on a GC'd step or a claim mismatch.
    state.metrics.inner.claims_in_flight.dec();

    if claim.job_id != job_id {
        return Err(Error::BadRequest(
            "claim_id doesn't belong to this job".into(),
        ));
    }

    let Some(step) = state.dispatcher.steps.get(&claim.drv_hash) else {
        // The step's Arc has been GC'd — the submission that owned it
        // is gone. Treat as stale; caller drops its result.
        return Ok(Json(CompleteResponse {
            ignored: true,
            next_build: None,
        }));
    };

    // Guard against the race where failure propagation already marked
    // this step finished in-memory while the worker's complete was in
    // flight. Double-processing would cause duplicate PG writes and
    // incorrect metrics.
    if step.finished.load(Ordering::Acquire) {
        return Ok(Json(CompleteResponse {
            ignored: true,
            next_build: None,
        }));
    }

    truncate_log(&mut req.log_tail);

    state
        .metrics
        .inner
        .build_duration
        .observe((req.duration_ms as f64) / 1000.0);

    if req.success {
        handle_success(&state, &claim, &step).await?;
    } else {
        handle_failure(&state, &claim, &step, req).await?;
    }
    // No opportunistic `next_build`: it would mark a drv `building`
    // with an outstanding claim_id before we know the caller will
    // consume it; if the response is dropped, the drv is stuck until
    // the reaper catches it. Worker loops wake on notify in microseconds.
    Ok(Json(CompleteResponse {
        ignored: false,
        next_build: None,
    }))
}

/// Char-boundary-safe tail truncation. Keeps at most
/// `MAX_LOG_TAIL_BYTES` bytes; if the byte cut falls mid-UTF-8, we
/// step forward to the next valid boundary to avoid a `split_at`
/// panic on multi-byte characters.
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
    claim: &crate::dispatch::claim::ActiveClaim,
    step: &Arc<crate::dispatch::Step>,
) -> Result<()> {
    step.finished.store(true, Ordering::Release);

    let _ = writeback::mark_done(&state.pool, step.drv_hash(), claim.claim_id).await?;
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
        return handle_flaky_retry(state, claim, step, req, category, attempt).await;
    }

    step.previous_failure.store(true, Ordering::Release);
    step.finished.store(true, Ordering::Release);

    let propagated = writeback::mark_terminal_failure(
        &state.pool,
        MarkFailed {
            drv_hash: step.drv_hash(),
            claim_id: Some(claim.claim_id),
            category,
            message: req.error_message.as_deref(),
            exit_code: req.exit_code,
            log_tail: req.log_tail.as_deref(),
        },
    )
    .await?;
    state
        .metrics
        .inner
        .builds_completed
        .get_or_create(&OutcomeLabels {
            outcome: "failure".into(),
        })
        .inc();
    if !propagated.is_empty() {
        state
            .metrics
            .inner
            .propagated_failures
            .inc_by(propagated.len() as u64);
    }

    propagate_failure_inmem(step);

    let subs = collect_submissions(step);
    publish_drv_failed(&subs, step, category, &req, attempt, false);
    for sub in subs {
        check_and_publish_terminal(state, &sub).await?;
    }
    state.dispatcher.wake();
    Ok(())
}

async fn handle_flaky_retry(
    state: &AppState,
    claim: &crate::dispatch::claim::ActiveClaim,
    step: &Arc<crate::dispatch::Step>,
    req: CompleteRequest,
    category: ErrorCategory,
    attempt: i32,
) -> Result<()> {
    let backoff_ms = state.cfg.flaky_retry_backoff_step_ms * i64::from(attempt);
    let updated = writeback::mark_flaky_retry(
        &state.pool,
        MarkFailed {
            drv_hash: step.drv_hash(),
            claim_id: Some(claim.claim_id),
            category,
            message: req.error_message.as_deref(),
            exit_code: req.exit_code,
            log_tail: req.log_tail.as_deref(),
        },
        backoff_ms,
    )
    .await?;
    if updated {
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

fn propagate_failure_inmem(root: &Arc<crate::dispatch::Step>) {
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
    while let Some(step) = frontier.pop_front() {
        if step.finished.load(Ordering::Acquire) {
            continue;
        }
        step.previous_failure.store(true, Ordering::Release);
        step.finished.store(true, Ordering::Release);
        let next: Vec<Arc<crate::dispatch::Step>> = {
            let st = step.state.read();
            st.rdeps.iter().filter_map(|w| w.upgrade()).collect()
        };
        frontier.extend(next);
    }
}

/// Transition a submission to a terminal state if it qualifies.
///
/// Ordering is load-bearing: we **write to Postgres first**, then do
/// the in-memory CAS that gates event publication. If the DB write
/// fails, the in-memory state is not dirtied, so the next completion
/// or the reaper can retry cleanly. If the DB write succeeds and the
/// CAS-race loses to another completion, the event publish is skipped
/// (another thread already did it) — the double-DB-write is idempotent
/// because `transition_job_terminal` has a `done_at IS NULL` guard.
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

    // `transition_job_terminal` is idempotent on `done_at IS NULL`;
    // a zero-rows result means another path already concluded the job
    // — still safe to proceed, the in-memory CAS below is the
    // authoritative gate for event publication.
    let _transitioned =
        writeback::transition_job_terminal(&state.pool, sub.id, final_status.as_str()).await?;

    if !sub.mark_terminal() {
        return Ok(());
    }

    let failures: Vec<DrvFailure> = tops
        .iter()
        .filter(|t| t.previous_failure.load(Ordering::Acquire))
        .map(|t| DrvFailure {
            drv_hash: t.drv_hash().clone(),
            drv_name: t.drv_name().to_string(),
            error_category: ErrorCategory::BuildFailure,
            error_message: None,
            log_tail: None,
            propagated_from: None,
        })
        .collect();

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

    // Release the submission from the in-memory map. The caller still
    // holds the `&Arc<Submission>` so outstanding SSE subscribers'
    // broadcast queues see JobDone, then the stream closes cleanly as
    // the Sender is dropped (happens when this Arc's last holder does).
    // New `/events` subscribers and new ingest calls after this point
    // get a 404 — they should poll `/jobs/{id}` status instead, which
    // serves from Postgres for terminal jobs. Matches the cleanup
    // pattern in `jobs::transition_to` and `reaper::reap_stale_jobs`.
    state.dispatcher.submissions.remove(sub.id);
    Ok(())
}
