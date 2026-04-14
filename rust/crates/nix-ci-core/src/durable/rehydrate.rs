//! Boot-time state repair.
//!
//! There is no graph to rehydrate — the dispatcher is purely in-memory
//! and in-flight state is not persisted. On coordinator restart every
//! non-terminal job is flipped to cancelled with a sentinel result, so
//! its workers' next poll returns 410 and the caller retries with a
//! fresh job. Matches Hydra v2's `clear_busy` primitive and the general
//! "durable envelope, ephemeral scheduling" pattern across the space.

use sqlx::PgPool;

use crate::error::Result;

/// Mark every in-flight job cancelled with an explanatory result blob.
/// Idempotent: subsequent calls are no-ops. Also evicts expired entries
/// from the `failed_outputs` TTL cache so the background cleanup loop
/// doesn't pick up stale ones on its first tick.
pub async fn clear_busy(pool: &PgPool) -> Result<()> {
    let sentinel = serde_json::json!({
        "id": null,
        "status": "cancelled",
        "sealed": false,
        "counts": { "total": 0, "pending": 0, "building": 0, "done": 0, "failed": 0 },
        "failures": [],
        "eval_error": "coordinator restarted; job aborted"
    });

    let res = sqlx::query(
        r#"
        UPDATE jobs
        SET status = 'cancelled',
            done_at = now(),
            result = $1
        WHERE status = 'pending' AND done_at IS NULL
        "#,
    )
    .bind(&sentinel)
    .execute(pool)
    .await?;

    let expired = sqlx::query("DELETE FROM failed_outputs WHERE expires_at < now()")
        .execute(pool)
        .await?;

    tracing::info!(
        cancelled_jobs = res.rows_affected(),
        expired_failed_outputs = expired.rows_affected(),
        "clear_busy complete"
    );
    Ok(())
}
