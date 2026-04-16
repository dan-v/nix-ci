//! Reaper: periodically sweep stale state. Runs on the single-writer
//! coordinator.
//!
//! Two sweeps:
//! 1. Jobs whose `last_heartbeat` is too old — transition to cancelled
//!    with a sentinel result; their in-memory submission is dropped.
//! 2. Claims whose deadline has passed — re-arm the in-memory step so
//!    another worker picks it up. A stale `/complete` from the original
//!    worker no longer matches any active claim and is ignored.
//!
//! No "terminal sweep" of the submissions map: every terminal path
//! (cancel / fail / check_and_publish_terminal / reap_stale_jobs)
//! removes its own submission; a safety-net sweep only masked bugs.

use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::watch;
use tokio::time::interval;

use crate::dispatch::Dispatcher;
use crate::error::Result;
use crate::types::{JobId, JobStatus};

pub async fn run(
    pool: PgPool,
    dispatcher: Dispatcher,
    tick: Duration,
    job_heartbeat_timeout: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => {
                tracing::info!("reaper: shutdown");
                return;
            }
        }
        if let Err(e) = reap_stale_jobs(&pool, &dispatcher, job_heartbeat_timeout).await {
            tracing::warn!(error = %e, "reap_stale_jobs failed");
        }
        reap_expired_claims(&dispatcher);
    }
}

/// Find jobs whose heartbeat is older than the timeout, flip them to
/// `cancelled` in Postgres with a sentinel result, drop their
/// in-memory submissions (so next `/claim` returns 410), and discard
/// any in-memory claims tied to those jobs.
pub async fn reap_stale_jobs(
    pool: &PgPool,
    dispatcher: &Dispatcher,
    timeout: Duration,
) -> Result<()> {
    let secs = timeout.as_secs() as i64;
    let stale_ids: Vec<(sqlx::types::Uuid,)> = sqlx::query_as(
        r#"
        SELECT id FROM jobs
        WHERE status = 'pending'
          AND last_heartbeat < now() - make_interval(secs => $1::bigint)
        "#,
    )
    .bind(secs)
    .fetch_all(pool)
    .await?;

    if stale_ids.is_empty() {
        return Ok(());
    }
    tracing::warn!(n = stale_ids.len(), "reaping stale jobs");
    dispatcher
        .metrics
        .inner
        .jobs_reaped
        .inc_by(stale_ids.len() as u64);

    for (uuid,) in &stale_ids {
        let job_id = JobId(*uuid);
        let sentinel = serde_json::json!({
            "id": job_id.0.to_string(),
            "status": "cancelled",
            "sealed": false,
            "counts": { "total": 0, "pending": 0, "building": 0, "done": 0, "failed": 0 },
            "failures": [],
            "eval_error": "heartbeat timeout; job reaped"
        });
        if let Err(e) = sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'cancelled', done_at = now(), result = $2
            WHERE id = $1 AND done_at IS NULL
            "#,
        )
        .bind(job_id.0)
        .bind(&sentinel)
        .execute(pool)
        .await
        {
            tracing::warn!(error = %e, %job_id, "reap: transition failed");
            continue;
        }

        if let Some(sub) = dispatcher.submissions.remove(job_id) {
            // Evict in-flight claims for the reaped job in the same
            // step so the gauge balances and we don't pin the step's
            // drv_hash until the claim's deadline.
            dispatcher.evict_claims_for(job_id);
            if sub.mark_terminal() {
                sub.publish(crate::types::JobEvent::JobDone {
                    status: JobStatus::Cancelled,
                    failures: vec![],
                });
            }
        }
    }

    dispatcher.wake();
    Ok(())
}

/// Deadline-reap claims. Claims live only in memory — no Postgres
/// write is needed. Re-arms the step so another worker can pick it up.
pub fn reap_expired_claims(dispatcher: &Dispatcher) {
    let now = Instant::now();
    let expired = dispatcher.claims.expired_ids(now);
    if expired.is_empty() {
        return;
    }
    tracing::warn!(n = expired.len(), "reaping expired claims");
    dispatcher
        .metrics
        .inner
        .claims_expired
        .inc_by(expired.len() as u64);
    for claim_id in expired {
        let Some(claim) = dispatcher.claims.take(claim_id) else {
            continue;
        };
        dispatcher.metrics.inner.claims_in_flight.dec();
        if let Some(step) = dispatcher.steps.get(&claim.drv_hash) {
            if !step.finished.load(std::sync::atomic::Ordering::Acquire) {
                step.runnable
                    .store(true, std::sync::atomic::Ordering::Release);
                // Race: between the load above and the store, a
                // concurrent failure-propagation could have set
                // finished=true. Re-check and undo so we never leave
                // a step with `finished=true && runnable=true` (which
                // would orphan a queue entry and violate dispatcher
                // invariant 4).
                if step.finished.load(std::sync::atomic::Ordering::Acquire) {
                    step.runnable
                        .store(false, std::sync::atomic::Ordering::Release);
                } else {
                    crate::dispatch::rdep::enqueue_for_all_submissions(&step);
                }
            }
        }
    }
    dispatcher.wake();
}
