//! `GET /jobs/{id}/claim?wait=Ns&system=X&features=...`
//! Long-poll claim endpoint.

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
use crate::durable::writeback;
use crate::error::{Error, Result};
use crate::types::{ClaimId, ClaimQuery, ClaimResponse, JobEvent, JobId};

pub async fn claim(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
    Query(mut q): Query<ClaimQuery>,
) -> Result<Response> {
    q.wait = q.wait.min(state.cfg.max_claim_wait_secs);

    let Some(sub) = state.dispatcher.submissions.get(id) else {
        // Submission is absent from the dispatcher map. Three cases:
        //   1. Job went terminal (completion / cancel / heartbeat-reap)
        //      — the terminal paths remove from the map.
        //   2. Coordinator just restarted and rehydrate hasn't wired
        //      up submissions yet — transient.
        //   3. Job never existed — caller bug.
        // In all three, the correct worker behavior is to stop
        // polling. Returning 410 Gone makes worker.rs exit its loop
        // via its explicit Gone match arm.
        return Err(Error::Gone(format!("job {id} is terminal or unknown")));
    };
    // No explicit terminal guard beyond the map presence check above:
    // `pop_runnable` returns None for a submission whose drvs have
    // all terminated, and the `complete` handler short-circuits a
    // stale claim via the finished flag.

    let start = Instant::now();
    let deadline = start + Duration::from_secs(q.wait);
    let features_vec = q.features_vec();

    loop {
        let now_ms = Utc::now().timestamp_millis();
        if let Some(step) = sub.pop_runnable(&q.system, &features_vec, now_ms) {
            match issue_claim(&state, &sub, id, step, start).await {
                Ok(resp) => return Ok(resp),
                Err(e) => return Err(e),
            }
        }

        if Instant::now() >= deadline {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }

        let wait = tokio::time::sleep_until(deadline.into());
        let notified = state.dispatcher.notify.notified();
        tokio::select! {
            () = notified => continue,
            () = wait => return Ok(StatusCode::NO_CONTENT.into_response()),
        }
    }
}

/// Record the claim atomically: first install the in-memory entry (so
/// the dispatcher knows it exists), then write-through to Postgres. If
/// the DB write fails, unwind the in-memory claim and put `runnable`
/// back for another worker. The reverse ordering would leave Postgres
/// marked `building` with no in-memory record, which the reaper would
/// only clean up after heartbeat timeout.
async fn issue_claim(
    state: &AppState,
    sub: &Arc<crate::dispatch::Submission>,
    job_id: JobId,
    step: Arc<crate::dispatch::Step>,
    started_at: Instant,
) -> Result<Response> {
    let attempt = step.tries.fetch_add(1, Ordering::AcqRel) + 1;
    let claim_id = ClaimId::new();
    let claim_deadline_duration = Duration::from_secs(state.cfg.claim_deadline_secs);
    let claim_instant_deadline = Instant::now() + claim_deadline_duration;
    let wall_deadline =
        Utc::now() + chrono::Duration::from_std(claim_deadline_duration).unwrap_or_default();

    state.dispatcher.claims.insert(Arc::new(ActiveClaim {
        claim_id,
        job_id,
        drv_hash: step.drv_hash().clone(),
        attempt,
        deadline: claim_instant_deadline,
    }));

    if let Err(e) = writeback::mark_building(&state.pool, step.drv_hash(), claim_id, attempt).await
    {
        // Unwind: drop in-memory claim, re-arm step so another worker
        // can pick it up.
        state.dispatcher.claims.take(claim_id);
        step.runnable.store(true, Ordering::Release);
        crate::dispatch::rdep::enqueue_for_all_submissions(&step);
        return Err(e);
    }

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
