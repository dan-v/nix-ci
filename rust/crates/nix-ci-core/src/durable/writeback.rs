//! Durable writes. The in-memory dispatcher is authoritative for in-
//! flight state; Postgres only carries the job envelope + terminal
//! results snapshot + failed-output TTL cache.
//!
//! Every function here touches at most one row of `jobs` or bulk-
//! inserts into `failed_outputs`. No cross-table joins, no recursive
//! CTEs.

use sqlx::PgPool;

use crate::error::Result;
use crate::types::{DrvHash, JobId};

/// Insert the job row. Idempotent on `id`: concurrent retries of a
/// lost POST /jobs response will no-op rather than double-insert.
pub async fn upsert_job(pool: &PgPool, job_id: JobId, external_ref: Option<&str>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO jobs (id, status, external_ref)
        VALUES ($1, 'pending', $2)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(job_id.0)
    .bind(external_ref)
    .execute(pool)
    .await?;
    Ok(())
}

/// Look up an existing job by `external_ref`. The POST /jobs handler
/// uses this to make create idempotent: a client retry after a lost
/// response must return the *same* id, not mint a new one.
pub async fn find_job_by_external_ref(pool: &PgPool, external_ref: &str) -> Result<Option<JobId>> {
    let row: Option<(sqlx::types::Uuid,)> =
        sqlx::query_as("SELECT id FROM jobs WHERE external_ref = $1 LIMIT 1")
            .bind(external_ref)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(id,)| JobId(id)))
}

/// Bump `last_heartbeat`. Returns false if the job is terminal (the
/// reaper already cancelled it, or a client already sealed/failed it).
pub async fn heartbeat_job(pool: &PgPool, job_id: JobId) -> Result<bool> {
    let updated = sqlx::query(
        r#"
        UPDATE jobs SET last_heartbeat = now()
        WHERE id = $1 AND status = 'pending'
        "#,
    )
    .bind(job_id.0)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected() > 0)
}

/// Mark the job sealed. Returns false if the job row does not exist.
/// A re-seal of an already-sealed job is a success (idempotent).
pub async fn seal_job(pool: &PgPool, job_id: JobId) -> Result<bool> {
    let res = sqlx::query("UPDATE jobs SET sealed = TRUE WHERE id = $1")
        .bind(job_id.0)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Transition a job to a terminal state and persist the final
/// `JobStatusResponse` snapshot in `result`. Returns `true` if this
/// call actually transitioned the row (non-terminal → terminal) and
/// `false` if the job was already terminal or does not exist.
///
/// Idempotent via the `done_at IS NULL` guard; a losing race skips the
/// write entirely — the caller uses the return value to decide whether
/// to publish `JobEvent::JobDone`.
pub async fn transition_job_terminal(
    pool: &PgPool,
    job_id: JobId,
    status: &str,
    result: &serde_json::Value,
) -> Result<bool> {
    let res = sqlx::query(
        r#"
        UPDATE jobs
        SET status = $2,
            done_at = now(),
            result = $3
        WHERE id = $1 AND done_at IS NULL
        "#,
    )
    .bind(job_id.0)
    .bind(status)
    .bind(result)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Insert a batch of failed output paths into the TTL cache. Called
/// once per terminal-failed drv so future ingests of the same output
/// path can short-circuit without dispatching a worker.
pub async fn insert_failed_outputs(
    pool: &PgPool,
    drv_hash: &DrvHash,
    output_paths: &[String],
) -> Result<()> {
    if output_paths.is_empty() {
        return Ok(());
    }
    let drv_hash_refs: Vec<&str> =
        std::iter::repeat_n(drv_hash.as_str(), output_paths.len()).collect();
    let path_refs: Vec<&str> = output_paths.iter().map(String::as_str).collect();
    sqlx::query(
        r#"
        INSERT INTO failed_outputs (output_path, drv_hash)
        SELECT * FROM UNNEST($1::text[], $2::text[]) AS t(output_path, drv_hash)
        ON CONFLICT (output_path) DO NOTHING
        "#,
    )
    .bind(&path_refs)
    .bind(&drv_hash_refs)
    .execute(pool)
    .await?;
    Ok(())
}

/// Bulk lookup of failed output paths. Returns the subset of input paths
/// whose cache entry is still valid. A DB error here is *not* fatal:
/// ingest proceeds without the optimization. Callers log + continue.
pub async fn failed_output_hits(
    pool: &PgPool,
    drv_paths: &[&str],
) -> std::collections::HashSet<String> {
    use std::collections::HashSet;
    if drv_paths.is_empty() {
        return HashSet::new();
    }
    let paths: Vec<&str> = drv_paths.to_vec();
    let rows: std::result::Result<Vec<(String,)>, sqlx::Error> = sqlx::query_as(
        r#"
        SELECT output_path FROM failed_outputs
        WHERE output_path = ANY($1::text[])
          AND expires_at > now()
        "#,
    )
    .bind(&paths)
    .fetch_all(pool)
    .await;
    match rows {
        Ok(r) => r.into_iter().map(|(p,)| p).collect(),
        Err(e) => {
            tracing::warn!(error = %e, "failed_outputs lookup failed; continuing without cache");
            HashSet::new()
        }
    }
}
