//! Cleanup loop: TTL eviction and retention pruning.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::watch;
use tokio::time::interval;

use crate::durable::logs::LogStore;
use crate::error::Result;
use crate::observability::metrics::Metrics;

pub async fn run(
    pool: PgPool,
    tick: Duration,
    retention_days: u32,
    build_log_retention_days: u32,
    log_store: Arc<dyn LogStore>,
    metrics: Metrics,
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
        if let Err(e) = sweep(
            &pool,
            retention_days,
            build_log_retention_days,
            log_store.as_ref(),
            &metrics,
        )
        .await
        {
            tracing::warn!(error = %e, "cleanup sweep failed");
        }
    }
}

/// TTL + retention prune. Public for integration testing; callers in
/// production use `run` instead.
#[doc(hidden)]
pub async fn sweep(
    pool: &PgPool,
    retention_days: u32,
    build_log_retention_days: u32,
    log_store: &dyn LogStore,
    metrics: &Metrics,
) -> Result<()> {
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

    // Build log retention: independent of job retention because logs
    // are the bulkiest bytes per row. A coordinator can keep job
    // metadata (jobs.result, ~few KB each) for months while keeping
    // log archives (~hundreds of KB each) for only a couple of weeks.
    let cutoff = chrono::Utc::now() - chrono::Duration::days(build_log_retention_days as i64);
    let pruned = log_store.prune_older_than(cutoff).await?;
    if pruned > 0 {
        tracing::info!(n = pruned, "pruned old build logs");
    }

    // Refresh capacity gauges. Cheap (planner stats + pg_total_relation_size).
    if let Ok(Some(bytes)) = log_store.total_bytes().await {
        metrics.inner.build_logs_bytes_total.set(bytes as i64);
    }
    if let Ok(rows) = log_store.row_count().await {
        metrics.inner.build_logs_rows_total.set(rows as i64);
    }
    Ok(())
}
