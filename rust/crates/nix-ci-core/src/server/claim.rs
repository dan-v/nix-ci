//! `GET /jobs/{id}/claim?wait=Ns&system=X&features=...` — long-poll.
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
use crate::types::{ClaimId, ClaimQuery, ClaimResponse, JobEvent, JobId};

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
            return issue_claim(&state, &sub, id, step, start);
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
                    return issue_claim(&state, &sub, id, step, start);
                }
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
        }
    }
}

fn issue_claim(
    state: &AppState,
    sub: &Arc<crate::dispatch::Submission>,
    job_id: JobId,
    step: Arc<crate::dispatch::Step>,
    started_at: Instant,
) -> Result<Response> {
    let attempt = step.tries.fetch_add(1, Ordering::AcqRel) + 1;
    let claim_id = ClaimId::new();
    let deadline_duration = Duration::from_secs(state.cfg.claim_deadline_secs);
    let wall_deadline =
        Utc::now() + chrono::Duration::from_std(deadline_duration).unwrap_or_default();

    state.dispatcher.claims.insert(Arc::new(ActiveClaim {
        claim_id,
        job_id,
        drv_hash: step.drv_hash().clone(),
        attempt,
        deadline: Instant::now() + deadline_duration,
    }));

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
        claim_id,
        drv_hash: step.drv_hash().clone(),
        drv_path: step.drv_path().to_string(),
        attempt,
        deadline: wall_deadline,
    })
    .into_response())
}
