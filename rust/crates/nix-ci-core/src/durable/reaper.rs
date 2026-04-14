//! Reaper: periodically sweep stale state. Runs on the single-writer
//! coordinator.
//!
//! Two sweeps:
//! 1. Jobs whose `last_heartbeat` is too old — transition to cancelled
//!    with a sentinel result; their in-memory submission is dropped.
//! 2. Claims whose deadline has passed — re-arm the in-memory step so
//!    another worker picks it up. A stale `/complete` from the original
//!    worker no longer matches any active claim and is ignored.

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
        if let Err(e) = reap_expired_claims(&pool, &dispatcher).await {
            tracing::warn!(error = %e, "reap_expired_claims failed");
        }
        sweep_terminal_submissions(&dispatcher);
    }
}

fn sweep_terminal_submissions(dispatcher: &Dispatcher) {
    let all = dispatcher.submissions.all();
    let mut removed = 0u32;
    for sub in all {
        if sub.is_terminal() && dispatcher.submissions.remove(sub.id).is_some() {
            removed += 1;
        }
    }
    if removed > 0 {
        tracing::debug!(removed, "swept terminal submissions from in-memory map");
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
            if sub.mark_terminal() {
                sub.publish(crate::types::JobEvent::JobDone {
                    status: JobStatus::Cancelled,
                    failures: vec![],
                });
            }
        }
    }

    // Drop any in-memory claims whose job was just reaped. Without this
    // they'd linger until the claim_deadline (hours) inflating the
    // `claims_in_flight` gauge and holding a reference to the drv.
    let reaped: std::collections::HashSet<JobId> =
        stale_ids.iter().map(|(u,)| JobId(*u)).collect();
    let expired: Vec<_> = dispatcher
        .claims
        .all()
        .into_iter()
        .filter(|c| reaped.contains(&c.job_id))
        .map(|c| c.claim_id)
        .collect();
    for claim_id in expired {
        if dispatcher.claims.take(claim_id).is_some() {
            dispatcher.metrics.inner.claims_in_flight.dec();
        }
    }

    dispatcher.wake();
    Ok(())
}

/// Deadline-reap claims. Unlike `reap_stale_jobs` this does not need to
/// touch Postgres — claims exist only in memory — it just re-arms the
/// step so another worker can pick it up.
pub async fn reap_expired_claims(_pool: &PgPool, dispatcher: &Dispatcher) -> Result<()> {
    let now = Instant::now();
    let expired = dispatcher.claims.expired_ids(now);
    if expired.is_empty() {
        return Ok(());
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
                crate::dispatch::rdep::enqueue_for_all_submissions(&step);
            }
        }
    }
    dispatcher.wake();
    Ok(())
}
