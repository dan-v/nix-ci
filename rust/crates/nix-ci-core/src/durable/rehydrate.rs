//! Startup rehydration: rebuild the in-memory dispatcher graph from
//! Postgres. Runs once, after the advisory lock is held and after
//! `clear_busy` has swept stale claims.

use std::sync::Arc;

use sqlx::PgPool;

use crate::dispatch::rdep::attach_dep;
use crate::dispatch::{Dispatcher, Step, Submission};
use crate::error::Result;
use crate::types::{DrvHash, JobId};

/// Reset any in-flight state from a previous coordinator's lifetime.
/// Called immediately after the advisory lock is acquired.
pub async fn clear_busy(pool: &PgPool) -> Result<()> {
    // Move any 'building' drvs back to 'pending'; their claims are gone.
    sqlx::query(
        r#"
        UPDATE derivations
        SET state = 'pending',
            assigned_claim_id = NULL
        WHERE state = 'building'
        "#,
    )
    .execute(pool)
    .await?;

    // Move any 'building' jobs back to 'pending'.
    sqlx::query(
        r#"
        UPDATE jobs
        SET status = 'pending'
        WHERE status = 'building'
        "#,
    )
    .execute(pool)
    .await?;

    // Expire failed_outputs whose TTL has passed.
    let deleted = sqlx::query("DELETE FROM failed_outputs WHERE expires_at < now()")
        .execute(pool)
        .await?;
    tracing::info!(
        expired_failed_outputs = deleted.rows_affected(),
        "clear_busy complete"
    );
    Ok(())
}

struct DrvRow {
    drv_hash: String,
    drv_path: String,
    drv_name: String,
    system: String,
    state: String,
    attempt: i32,
    max_attempts: i32,
    required_features: Vec<String>,
    previous_failure: bool,
}

/// Rebuild `Dispatcher.submissions` and `Dispatcher.steps` from the
/// durable tables. Called once at startup after `clear_busy`.
pub async fn rehydrate(pool: &PgPool, dispatcher: &Dispatcher) -> Result<()> {
    // Load all non-terminal jobs
    let jobs: Vec<(sqlx::types::Uuid, String, bool, Option<String>)> = sqlx::query_as(
        r#"
        SELECT id, status, sealed, eval_error
        FROM jobs
        WHERE status IN ('pending','building')
        "#,
    )
    .fetch_all(pool)
    .await?;

    tracing::info!(jobs = jobs.len(), "rehydrating jobs");

    for (id, _status, sealed, eval_error) in &jobs {
        let job_id = JobId(*id);
        let sub = dispatcher.submissions.get_or_insert(job_id, 4096);
        if *sealed {
            sub.seal();
        }
        // If the pre-crash coordinator stored an eval_error on this
        // job, we already know it's going to terminate as Failed. Mark
        // terminal now so status polls + SSE reflect the truth
        // immediately, and subsequent worker claims against this job
        // see an empty ready set.
        if let Some(err) = eval_error {
            if sub.mark_terminal() {
                sub.publish(crate::types::JobEvent::JobDone {
                    status: crate::types::JobStatus::Failed,
                    failures: vec![],
                });
            }
            tracing::warn!(%job_id, eval_error = %err, "rehydrated job has eval_error; marked terminal");
        }
    }

    // Load all drvs referenced by any non-terminal job. We do this via
    // a join on job_roots + a recursive walk over deps; cheaper to
    // just load ALL derivations and all deps and filter in memory at
    // the scale we're targeting (≤10⁵).
    let drvs: Vec<DrvRow> = sqlx::query_as::<_, DrvRow>(
        r#"
        SELECT d.drv_hash,
               d.drv_path,
               d.drv_name,
               d.system,
               d.state,
               d.attempt,
               d.max_attempts,
               d.required_features,
               (d.propagated_from IS NOT NULL) AS previous_failure
        FROM derivations d
        WHERE EXISTS (
            SELECT 1 FROM job_roots jr
            JOIN jobs j ON j.id = jr.job_id
            WHERE j.status IN ('pending','building')
              AND jr.drv_hash = d.drv_hash
        )
        OR d.state IN ('pending','building')
        "#,
    )
    .fetch_all(pool)
    .await?;

    tracing::info!(drvs = drvs.len(), "rehydrating derivations");

    // Hold strong Arcs until submission attachment — otherwise each
    // `get_or_create`'s returned outcome would be dropped, the Weak
    // in the registry would die immediately, and subsequent lookups
    // (deps wiring, root attachment) would find nothing.
    let mut step_holders: Vec<Arc<Step>> = Vec::with_capacity(drvs.len());

    for row in &drvs {
        let step = Step::new(
            DrvHash::new(row.drv_hash.clone()),
            row.drv_path.clone(),
            row.drv_name.clone(),
            row.system.clone(),
            row.required_features.clone(),
            row.max_attempts,
        );
        step.tries
            .store(row.attempt, std::sync::atomic::Ordering::Release);
        match row.state.as_str() {
            "done" => {
                step.created
                    .store(true, std::sync::atomic::Ordering::Release);
                step.finished
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            "failed" => {
                step.created
                    .store(true, std::sync::atomic::Ordering::Release);
                step.finished
                    .store(true, std::sync::atomic::Ordering::Release);
                step.previous_failure
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            _ => {
                // pending: leave created=false for now; we'll flip it
                // once deps are wired up.
            }
        }
        if row.previous_failure {
            step.previous_failure
                .store(true, std::sync::atomic::Ordering::Release);
        }
        let live = dispatcher
            .steps
            .get_or_create(&DrvHash::new(row.drv_hash.clone()), || step)
            .into_step();
        step_holders.push(live);
    }

    // Wire up deps. Scope the query to edges whose parent is a drv we
    // actually loaded — at nixpkgs-scale, fetching every `deps` row
    // costs ~50 MB memory per boot, most of which isn't relevant.
    let loaded_hashes: Vec<String> = drvs.iter().map(|r| r.drv_hash.clone()).collect();
    let edges: Vec<(String, String)> =
        sqlx::query_as("SELECT drv_hash, dep_hash FROM deps WHERE drv_hash = ANY($1::text[])")
            .bind(&loaded_hashes)
            .fetch_all(pool)
            .await?;
    for (drv_hash, dep_hash) in &edges {
        let parent = dispatcher.steps.get(&DrvHash::new(drv_hash.clone()));
        let dep = dispatcher.steps.get(&DrvHash::new(dep_hash.clone()));
        if let (Some(parent), Some(dep)) = (parent, dep) {
            // Only attach the dep if it's not yet finished — if dep is
            // done, it would never make parent's deps set go empty by
            // removal, but we rely on `created=true` AND empty-deps
            // for runnability. So skip finished deps here and rely on
            // the runnable-sweep below.
            if !dep.finished.load(std::sync::atomic::Ordering::Acquire) {
                attach_dep(&parent, &dep);
            }
        }
    }

    // Wire up submissions → steps via job_roots; also backfill membership
    let roots: Vec<(sqlx::types::Uuid, String)> =
        sqlx::query_as("SELECT job_id, drv_hash FROM job_roots")
            .fetch_all(pool)
            .await?;
    for (job_id, drv_hash) in &roots {
        let job_id = JobId(*job_id);
        let Some(sub) = dispatcher.submissions.get(job_id) else {
            continue;
        };
        let Some(step) = dispatcher.steps.get(&DrvHash::new(drv_hash.clone())) else {
            continue;
        };
        sub.add_root(step.clone());
        walk_and_attach_submission(&sub, &step, dispatcher).await;
    }

    // Arm: for every pending step REFERENCED BY AT LEAST ONE live
    // submission, set created. For steps with no submissions, skip —
    // they belong to an old job that was pruned or rehydrating would
    // create orphaned dispatchable drvs.
    for step in dispatcher.steps.live() {
        if step.finished.load(std::sync::atomic::Ordering::Acquire) {
            continue;
        }
        let has_sub = {
            let s = step.state.read();
            s.submissions.iter().any(|w| w.strong_count() > 0)
        };
        if !has_sub {
            continue;
        }
        step.created
            .store(true, std::sync::atomic::Ordering::Release);
        if step.state.read().deps.is_empty() {
            step.runnable
                .store(true, std::sync::atomic::Ordering::Release);
            crate::dispatch::rdep::enqueue_for_all_submissions(&step);
        }
    }

    dispatcher.wake();
    tracing::info!(
        steps = dispatcher.steps.len(),
        submissions = dispatcher.submissions.len(),
        "rehydration complete"
    );

    Ok(())
}

/// Walk a submission's root's deps transitively, adding every
/// transitively-reachable step to the submission's membership.
async fn walk_and_attach_submission(
    sub: &Arc<Submission>,
    root: &Arc<Step>,
    dispatcher: &Dispatcher,
) {
    use std::collections::HashSet;
    let mut stack = vec![root.clone()];
    let mut seen: HashSet<DrvHash> = HashSet::new();
    while let Some(step) = stack.pop() {
        if !seen.insert(step.drv_hash().clone()) {
            continue;
        }
        sub.add_member(&step);
        {
            let state = step.state.read();
            let deps_snapshot: Vec<Arc<Step>> = state.deps.iter().map(|h| h.0.clone()).collect();
            for dep in deps_snapshot {
                stack.push(dep);
            }
        }
        // Attach submission weak to step (idempotent; deduped by id).
        step.state.write().attach_submission(sub);
    }
    let _ = dispatcher; // parameter reserved for future growth
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for DrvRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            drv_hash: row.try_get("drv_hash")?,
            drv_path: row.try_get("drv_path")?,
            drv_name: row.try_get("drv_name")?,
            system: row.try_get("system")?,
            state: row.try_get("state")?,
            attempt: row.try_get("attempt")?,
            max_attempts: row.try_get("max_attempts")?,
            required_features: row.try_get("required_features")?,
            previous_failure: row.try_get("previous_failure")?,
        })
    }
}
