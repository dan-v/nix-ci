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

/// Look up a job by its caller-supplied `external_ref`. Returns the
/// terminal-snapshot result JSONB (if the job has reached terminal)
/// alongside identity fields. Used by `GET /jobs/by-external-ref/{ref}`
/// and by `nix-ci show <ref>`.
pub async fn lookup_job_by_external_ref(
    pool: &PgPool,
    external_ref: &str,
) -> Result<Option<JobLookup>> {
    type LookupRow = (
        sqlx::types::Uuid,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
    );
    let row: Option<LookupRow> = sqlx::query_as(
        r#"
        SELECT id, status, done_at, result
        FROM jobs
        WHERE external_ref = $1
        ORDER BY done_at DESC NULLS FIRST
        LIMIT 1
        "#,
    )
    .bind(external_ref)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id, status, done_at, result)| JobLookup {
        id: JobId(id),
        status,
        done_at,
        result,
    }))
}

/// Look up a job by its UUID. Mirrors `lookup_job_by_external_ref` for
/// the `nix-ci show <uuid>` path.
pub async fn lookup_job_by_id(pool: &PgPool, id: JobId) -> Result<Option<JobLookup>> {
    let row: Option<(
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
    )> = sqlx::query_as("SELECT status, done_at, result FROM jobs WHERE id = $1")
        .bind(id.0)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(status, done_at, result)| JobLookup {
        id,
        status,
        done_at,
        result,
    }))
}

/// Filter rows for `GET /jobs`. Status is required (we don't have a
/// real use case for unfiltered listing yet). `since` is exclusive
/// upper-bound on `done_at` for cursor pagination. `limit` is capped
/// server-side; the response includes a `next_cursor` if more rows
/// exist past the last returned `done_at`.
pub async fn list_jobs_by_status(
    pool: &PgPool,
    status: &str,
    cursor_before: Option<chrono::DateTime<chrono::Utc>>,
    limit: i64,
) -> Result<Vec<JobRow>> {
    // We deliberately query one row past `limit` so the caller can
    // tell whether there's a next page without a separate count.
    type ListRow = (
        sqlx::types::Uuid,
        Option<String>,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
    );
    let rows: Vec<ListRow> = match cursor_before {
        Some(before) => {
            sqlx::query_as(
                r#"
            SELECT id, external_ref, status, done_at, result
            FROM jobs
            WHERE status = $1 AND done_at IS NOT NULL AND done_at < $2
            ORDER BY done_at DESC
            LIMIT $3
            "#,
            )
            .bind(status)
            .bind(before)
            .bind(limit + 1)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as(
                r#"
            SELECT id, external_ref, status, done_at, result
            FROM jobs
            WHERE status = $1 AND done_at IS NOT NULL
            ORDER BY done_at DESC
            LIMIT $2
            "#,
            )
            .bind(status)
            .bind(limit + 1)
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows
        .into_iter()
        .map(|(id, external_ref, status, done_at, result)| JobRow {
            id: JobId(id),
            external_ref,
            status,
            done_at,
            result,
        })
        .collect())
}

/// Returned from per-job lookups (`by-external-ref`, `by-id`).
pub struct JobLookup {
    pub id: JobId,
    pub status: String,
    pub done_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Terminal snapshot JSONB; absent for non-terminal jobs.
    pub result: Option<serde_json::Value>,
}

/// One row from `list_jobs_by_status`.
pub struct JobRow {
    pub id: JobId,
    pub external_ref: Option<String>,
    pub status: String,
    pub done_at: Option<chrono::DateTime<chrono::Utc>>,
    pub result: Option<serde_json::Value>,
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
