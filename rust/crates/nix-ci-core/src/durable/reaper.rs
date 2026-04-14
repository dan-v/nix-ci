//! Reaper: periodically sweep stale state. Runs on the single-writer
//! coordinator.
//!
//! Three sweeps:
//! 1. Jobs whose `last_heartbeat` is too old — their in-flight drvs
//!    get released to pending, and the job goes to cancelled.
//! 2. Claims whose deadline has passed — drv terminal-fails with
//!    `timeout`.
//! 3. Submissions that already reached terminal (via any path —
//!    completion, explicit cancel, heartbeat-reap, rehydrate-with-
//!    eval_error) but are still in the in-memory map. Belt-and-
//!    suspenders cleanup so the map can't grow monotonically even
//!    if some terminal path forgets to remove.

use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::time::interval;

use crate::dispatch::Dispatcher;
use crate::error::Result;
use crate::types::JobId;

pub async fn run(
    pool: PgPool,
    dispatcher: Dispatcher,
    tick: Duration,
    job_heartbeat_timeout: Duration,
) {
    let mut ticker = interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(e) = reap_stale_jobs(&pool, &dispatcher, job_heartbeat_timeout).await {
            tracing::warn!(error = %e, "reap_stale_jobs failed");
        }
        if let Err(e) = reap_expired_claims(&pool, &dispatcher).await {
            tracing::warn!(error = %e, "reap_expired_claims failed");
        }
        sweep_terminal_submissions(&dispatcher);
    }
}

/// Drop in-memory submissions that have already reached terminal
/// state. The primary terminal paths (`jobs::transition_to`,
/// `complete::check_and_publish_terminal`, `reap_stale_jobs`) all
/// remove the submission themselves, but this sweep closes any hole
/// left by a new terminal path that forgets — or by the older
/// `check_and_publish_terminal` behavior before it learned to remove.
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

/// Find jobs whose heartbeat is older than the timeout. Release their
/// in-flight drvs back to pending in Postgres; our in-memory state
/// will sync on next claim attempt (CAS on runnable will fail
/// otherwise). The job goes to cancelled.
async fn reap_stale_jobs(pool: &PgPool, dispatcher: &Dispatcher, timeout: Duration) -> Result<()> {
    let secs = timeout.as_secs() as i64;
    let stale_ids: Vec<(sqlx::types::Uuid,)> = sqlx::query_as(
        r#"
        SELECT id FROM jobs
        WHERE status IN ('pending','building')
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

    let mut tx = pool.begin().await?;
    let uuids: Vec<sqlx::types::Uuid> = stale_ids.iter().map(|(u,)| *u).collect();

    // Release assigned drvs belonging to these jobs — including
    // transitive, non-root derivations. The previous query only
    // touched `job_roots`, leaving any building child drv wedged
    // until its 2h claim deadline expired. For a large DAG with
    // thousands of build nodes, that was most of the drvs.
    sqlx::query(
        r#"
        WITH RECURSIVE closure AS (
            SELECT drv_hash FROM job_roots WHERE job_id = ANY($1::uuid[])
            UNION
            SELECT d.dep_hash FROM deps d JOIN closure c ON c.drv_hash = d.drv_hash
        )
        UPDATE derivations
        SET state = 'pending', assigned_claim_id = NULL
        WHERE state = 'building'
          AND drv_hash IN (SELECT drv_hash FROM closure)
        "#,
    )
    .bind(&uuids)
    .execute(&mut *tx)
    .await?;

    // Mark the jobs cancelled
    sqlx::query(
        r#"
        UPDATE jobs SET status = 'cancelled', done_at = now()
        WHERE id = ANY($1::uuid[])
        "#,
    )
    .bind(&uuids)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    // In-memory: publish a JobDone event so SSE subscribers see the
    // cancellation, mark terminal (idempotent CAS), then drop. The
    // worker's next heartbeat returns 410, the worker exits.
    for (uuid,) in &stale_ids {
        let job_id = JobId(*uuid);
        if let Some(sub) = dispatcher.submissions.remove(job_id) {
            if sub.mark_terminal() {
                sub.publish(crate::types::JobEvent::JobDone {
                    status: crate::types::JobStatus::Cancelled,
                    failures: vec![],
                });
            }
        }
    }
    dispatcher.wake();
    Ok(())
}

async fn reap_expired_claims(pool: &PgPool, dispatcher: &Dispatcher) -> Result<()> {
    let now = Instant::now();
    let expired = dispatcher.claims.expired_ids(now);
    if expired.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        n = expired.len(),
        "reaping expired claims (deadline exceeded)"
    );
    for claim_id in expired {
        let Some(claim) = dispatcher.claims.take(claim_id) else {
            continue;
        };
        // Durable state: flip drv back to pending so another worker
        // can claim it. A stale `complete` call from the original
        // worker will no longer match `assigned_claim_id` in PG and
        // will return `ignored: true` on its way through the HTTP
        // complete handler.
        if let Err(e) = sqlx::query(
            r#"
            UPDATE derivations
            SET state = 'pending', assigned_claim_id = NULL
            WHERE drv_hash = $1 AND assigned_claim_id = $2
            "#,
        )
        .bind(claim.drv_hash.as_str())
        .bind(claim.claim_id.0)
        .execute(pool)
        .await
        {
            tracing::warn!(error = %e, drv = %claim.drv_hash, "reap: DB reset failed");
            continue;
        }
        // In-memory: re-arm runnable so the dispatcher can serve it.
        if let Some(step) = dispatcher.steps.get(&claim.drv_hash) {
            step.runnable
                .store(true, std::sync::atomic::Ordering::Release);
            crate::dispatch::rdep::enqueue_for_all_submissions(&step);
        }
        dispatcher.metrics.inner.claims_in_flight.dec();
    }
    dispatcher.wake();
    Ok(())
}
