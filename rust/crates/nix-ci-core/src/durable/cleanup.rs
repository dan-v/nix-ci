//! Cleanup loop: TTL eviction and retention pruning.

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
    ticker.tick().await; // skip first: we ran cleanup at startup
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
    let r = sqlx::query("DELETE FROM failed_outputs WHERE expires_at < now()")
        .execute(pool)
        .await?;
    if r.rows_affected() > 0 {
        tracing::debug!(n = r.rows_affected(), "expired failed_outputs");
    }

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
    Ok(())
}
