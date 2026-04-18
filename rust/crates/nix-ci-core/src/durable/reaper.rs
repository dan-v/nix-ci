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

use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::watch;
// `tokio::time::Instant` for the claim-deadline compare — matches
// the scheduling-relevant clock `ActiveClaim.deadline` is written
// against. See `dispatch/claim.rs` for the rule.
use tokio::time::{interval, Instant};

use crate::dispatch::Dispatcher;
use crate::error::Result;
use crate::types::{JobId, JobStatus};

/// Upper bound on how long the stale-jobs DB query may run before the
/// reaper gives up on a tick and moves on. Without this, a stuck
/// Postgres server would block the reaper indefinitely — stale jobs
/// pile up in memory and in the pool's acquire queue, even though the
/// claim-deadline sweep (which has no DB dependency) could still
/// serve in-flight workers. Keep it well below the reaper interval so
/// a flaky PG doesn't compound across ticks.
const REAPER_DB_TIMEOUT: Duration = Duration::from_secs(10);

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
        match tokio::time::timeout(
            REAPER_DB_TIMEOUT,
            reap_stale_jobs(&pool, &dispatcher, job_heartbeat_timeout),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "reap_stale_jobs failed");
            }
            Err(_) => {
                tracing::warn!(
                    timeout_secs = REAPER_DB_TIMEOUT.as_secs(),
                    "reap_stale_jobs timed out; will retry on next tick"
                );
            }
        }
        // Claim-deadline sweep is pure in-memory; always run it so
        // workers don't get stranded when Postgres is slow.
        reap_expired_claims(&dispatcher);
    }
}

/// Find jobs whose heartbeat is older than the timeout, flip them to
/// `cancelled` in Postgres with a sentinel result, drop their
/// in-memory submissions (so next `/claim` returns 410), and discard
/// any in-memory claims tied to those jobs.
///
/// One `UPDATE ... RETURNING id` does the whole transition atomically
/// and tells us exactly which jobs moved — avoiding the SELECT+loop
/// pattern's failure mode where a crash mid-loop left some rows
/// cancelled in PG but still pinned in memory until restart.
/// Sentinel JSON is built inside Postgres via `jsonb_build_object` so
/// we don't round-trip it per row.
pub async fn reap_stale_jobs(
    pool: &PgPool,
    dispatcher: &Dispatcher,
    timeout: Duration,
) -> Result<()> {
    let secs = timeout.as_secs() as i64;
    let reaped: Vec<(sqlx::types::Uuid,)> = sqlx::query_as(
        r#"
        UPDATE jobs
        SET status = 'cancelled',
            done_at = now(),
            result = jsonb_build_object(
                'id', id::text,
                'status', 'cancelled',
                'sealed', false,
                'counts', jsonb_build_object(
                    'total', 0, 'pending', 0, 'building', 0,
                    'done', 0, 'failed', 0
                ),
                'failures', '[]'::jsonb,
                'eval_error', 'heartbeat timeout; job reaped',
                'eval_errors', '[]'::jsonb
            )
        WHERE status = 'pending'
          AND last_heartbeat < now() - make_interval(secs => $1::bigint)
          AND done_at IS NULL
        RETURNING id
        "#,
    )
    .bind(secs)
    .fetch_all(pool)
    .await?;

    if reaped.is_empty() {
        return Ok(());
    }
    tracing::warn!(n = reaped.len(), "reaped stale jobs (atomic)");
    dispatcher
        .metrics
        .inner
        .jobs_reaped
        .inc_by(reaped.len() as u64);

    // Postgres already transitioned every row; now mirror the state
    // in memory. Each eviction is idempotent (submissions.remove is
    // a no-op when the id is absent, and claims.take / mark_terminal
    // are one-shot), so partial failure here is safe: the next
    // reaper tick won't re-attempt (the PG WHERE filters those rows
    // out), but claim-time 410s and admin eviction handle any
    // surviving orphans. A deliberate trade: we'd rather have
    // consistent durable state than risk a bulk UPDATE/bulk-evict
    // split transaction.
    for (uuid,) in &reaped {
        let job_id = JobId(*uuid);
        if let Some(sub) = dispatcher.submissions.remove(job_id) {
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
        // H3: observe claim age on expire so the histogram captures
        // both fast (complete) and slow (expire) tails.
        dispatcher
            .metrics
            .inner
            .claim_age_seconds
            .observe(claim.started_at.elapsed().as_secs_f64());
        // Mirror active_claims decrement on the owning submission so
        // the fleet scheduler's per-job cap clears as claims expire.
        if let Some(sub) = dispatcher.submissions.get(claim.job_id) {
            sub.decrement_active_claim();
        }
        if let Some(step) = dispatcher.steps.get(&claim.drv_hash) {
            crate::dispatch::rdep::rearm_step_if_live(&step);
        }
    }
    dispatcher.wake();
}
