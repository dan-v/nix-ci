//! Cleanup loop: bulk deletes and TTL sweeps. Runs every few minutes.

use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::watch;
use tokio::time::interval;

use crate::error::Result;

pub async fn run(
    pool: PgPool,
    tick: Duration,
    retention_days: u32,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // skip first tick — we ran cleanup at startup
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => {
                tracing::info!("cleanup: shutdown");
                return;
            }
        }
        if let Err(e) = sweep(&pool, retention_days).await {
            tracing::warn!(error = %e, "cleanup sweep failed");
        }
    }
}

async fn sweep(pool: &PgPool, retention_days: u32) -> Result<()> {
    // Expired failed_outputs
    let r = sqlx::query("DELETE FROM failed_outputs WHERE expires_at < now()")
        .execute(pool)
        .await?;
    if r.rows_affected() > 0 {
        tracing::debug!(n = r.rows_affected(), "expired failed_outputs");
    }

    // Old terminal jobs (and their roots cascade)
    let interval_arg = format!("{} days", retention_days);
    let r = sqlx::query(
        r#"
        DELETE FROM jobs
        WHERE status IN ('done','failed','cancelled')
          AND done_at < now() - $1::interval
        "#,
    )
    .bind(&interval_arg)
    .execute(pool)
    .await?;
    if r.rows_affected() > 0 {
        tracing::info!(n = r.rows_affected(), "pruned old terminal jobs");
    }

    // Old derivations not referenced by any live job. We use a
    // transitive-closure walk to identify "needed" drvs.
    let r = sqlx::query(
        r#"
        WITH RECURSIVE needed AS (
            SELECT jr.drv_hash
            FROM job_roots jr
            JOIN jobs j ON j.id = jr.job_id
            WHERE j.status IN ('pending','building')
            UNION
            SELECT d.dep_hash FROM deps d
            JOIN needed n ON n.drv_hash = d.drv_hash
        )
        DELETE FROM derivations
        WHERE state IN ('done','failed')
          AND completed_at < now() - $1::interval
          AND drv_hash NOT IN (SELECT drv_hash FROM needed)
        "#,
    )
    .bind(&interval_arg)
    .execute(pool)
    .await?;
    if r.rows_affected() > 0 {
        tracing::info!(n = r.rows_affected(), "pruned old derivations");
    }

    Ok(())
}
