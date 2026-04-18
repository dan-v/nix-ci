//! Durable writes. The in-memory dispatcher is authoritative for in-
//! flight state; Postgres only carries the job envelope + terminal
//! results snapshot + failed-output TTL cache.
//!
//! Every function here touches at most one row of `jobs` or bulk-
//! inserts into `failed_outputs`. No cross-table joins, no recursive
//! CTEs.

use sqlx::PgPool;
use tokio::time::Instant;

use crate::error::{Error, Result};
use crate::observability::metrics::Metrics;
use crate::types::{DrvHash, JobId, JobStatusResponse};

/// Acquire a pooled connection while recording the wait time into the
/// `pg_pool_acquire_duration_seconds` histogram. Callers on the hot
/// complete / ingest paths use this variant; tests and cold-path
/// lookups can call `pool.acquire()` directly.
///
/// The observation is taken on BOTH success and error — an `acquire`
/// that hits `acquire_timeout` (10s default) still tells us the pool
/// is starving, and the histogram should reflect that.
pub async fn timed_acquire(
    pool: &PgPool,
    metrics: &Metrics,
) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let t0 = Instant::now();
    let out = pool.acquire().await;
    metrics
        .inner
        .pg_pool_acquire_duration_seconds
        .observe(t0.elapsed().as_secs_f64());
    Ok(out?)
}

/// Insert the job row and return the winning id. Idempotent under
/// concurrent retries on EITHER unique constraint:
///
/// * On `id` — a retry of a lost `POST /jobs` response (no external_ref)
///   resolves to the same id it would have gotten.
/// * On `external_ref` — two concurrent `POST /jobs` with the same
///   caller-supplied ref resolve to the SAME id. Previously the
///   handler's "look up, then upsert" path was a TOCTOU: both
///   lookups returned None, both inserts tried, and the loser hit
///   `jobs_external_ref_uniq` (which `ON CONFLICT (id)` does not
///   cover) → 500 response instead of idempotent success.
///
/// Implementation: we first run the INSERT with `ON CONFLICT (id)` as
/// the arbiter. If that raises a unique-violation on a different
/// constraint (specifically `jobs_external_ref_uniq`), fall through
/// and SELECT the existing row by `external_ref`. This keeps the
/// happy path a single round trip and only pays the extra SELECT on
/// the rare external-ref race.
pub async fn upsert_job(pool: &PgPool, job_id: JobId, external_ref: Option<&str>) -> Result<JobId> {
    let inserted: std::result::Result<Option<(sqlx::types::Uuid,)>, sqlx::Error> = sqlx::query_as(
        r#"
        INSERT INTO jobs (id, status, external_ref)
        VALUES ($1, 'pending', $2)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(job_id.0)
    .bind(external_ref)
    .fetch_optional(pool)
    .await;
    match inserted {
        Ok(Some((id,))) => Ok(JobId(id)),
        Ok(None) => {
            // `ON CONFLICT (id)` fired — our generated UUID already
            // exists (should not happen in practice, but honor the
            // idempotent contract).
            Ok(job_id)
        }
        Err(sqlx::Error::Database(db_err))
            if db_err.constraint() == Some("jobs_external_ref_uniq") =>
        {
            // Concurrent creator won the external_ref race; resolve
            // to the winner's id.
            let Some(ext) = external_ref else {
                // Can't happen — the partial unique index only covers
                // non-NULL external_ref, so a conflict on it implies
                // external_ref is Some. Treat as a DB error if PG
                // ever surprises us.
                return Err(sqlx::Error::Database(db_err).into());
            };
            let row: (sqlx::types::Uuid,) =
                sqlx::query_as("SELECT id FROM jobs WHERE external_ref = $1 LIMIT 1")
                    .bind(ext)
                    .fetch_one(pool)
                    .await?;
            Ok(JobId(row.0))
        }
        Err(e) => Err(e.into()),
    }
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
/// Convenience wrapper: serialize a [`JobStatusResponse`] and persist it
/// via [`transition_job_terminal`]. Every coordinator path that forces
/// a terminal state (seal-fail, user-initiated fail, cancel, oversized
/// ingest rejection, and the graceful all-roots-finished path) funnels
/// through this so the serialize-then-write contract stays in one place.
pub async fn persist_terminal_snapshot(
    pool: &PgPool,
    job_id: JobId,
    snapshot: &JobStatusResponse,
) -> Result<bool> {
    let json = serde_json::to_value(snapshot)
        .map_err(|e| Error::Internal(format!("serialize terminal snapshot: {e}")))?;
    transition_job_terminal(pool, job_id, snapshot.status.as_str(), &json).await
}

/// Observed variant: records pool-acquire wait time on the hot
/// complete path. Prefer this over `persist_terminal_snapshot` from
/// code that has a Metrics handle in scope.
pub async fn persist_terminal_snapshot_observed(
    pool: &PgPool,
    metrics: &Metrics,
    job_id: JobId,
    snapshot: &JobStatusResponse,
) -> Result<bool> {
    let json = serde_json::to_value(snapshot)
        .map_err(|e| Error::Internal(format!("serialize terminal snapshot: {e}")))?;
    transition_job_terminal_observed(pool, metrics, job_id, snapshot.status.as_str(), &json).await
}

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

async fn transition_job_terminal_observed(
    pool: &PgPool,
    metrics: &Metrics,
    job_id: JobId,
    status: &str,
    result: &serde_json::Value,
) -> Result<bool> {
    let mut conn = timed_acquire(pool, metrics).await?;
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
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Insert a batch of failed output paths into the TTL cache. Called
/// once per terminal-failed drv so future ingests of the same output
/// path can short-circuit without dispatching a worker.
///
/// `ttl_secs` sets the row's `expires_at = now() + ttl`. Lets operators
/// pick per-deployment TTLs (short for flaky overlays, long for stable
/// channels) without a migration. The column still defaults to 1h for
/// backwards compatibility with manually-inserted rows.
pub async fn insert_failed_outputs(
    pool: &PgPool,
    drv_hash: &DrvHash,
    output_paths: &[String],
    ttl_secs: u64,
) -> Result<()> {
    insert_failed_outputs_inner(pool, None, drv_hash, output_paths, ttl_secs).await
}

/// Observed variant: records pool-acquire wait time on the hot
/// failure path.
pub async fn insert_failed_outputs_observed(
    pool: &PgPool,
    metrics: &Metrics,
    drv_hash: &DrvHash,
    output_paths: &[String],
    ttl_secs: u64,
) -> Result<()> {
    insert_failed_outputs_inner(pool, Some(metrics), drv_hash, output_paths, ttl_secs).await
}

async fn insert_failed_outputs_inner(
    pool: &PgPool,
    metrics: Option<&Metrics>,
    drv_hash: &DrvHash,
    output_paths: &[String],
    ttl_secs: u64,
) -> Result<()> {
    if output_paths.is_empty() {
        return Ok(());
    }
    let drv_hash_refs: Vec<&str> =
        std::iter::repeat_n(drv_hash.as_str(), output_paths.len()).collect();
    let path_refs: Vec<&str> = output_paths.iter().map(String::as_str).collect();
    let ttl: f64 = ttl_secs as f64;
    let query = r#"
        INSERT INTO failed_outputs (output_path, drv_hash, expires_at)
        SELECT output_path, drv_hash, now() + make_interval(secs => $3)
        FROM UNNEST($1::text[], $2::text[]) AS t(output_path, drv_hash)
        ON CONFLICT (output_path) DO NOTHING
        "#;
    if let Some(m) = metrics {
        let mut conn = timed_acquire(pool, m).await?;
        sqlx::query(query)
            .bind(&path_refs)
            .bind(&drv_hash_refs)
            .bind(ttl)
            .execute(&mut *conn)
            .await?;
    } else {
        sqlx::query(query)
            .bind(&path_refs)
            .bind(&drv_hash_refs)
            .bind(ttl)
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// Remove rows from the `failed_outputs` cache. Operator-invoked
/// counter to a false-positive: e.g. a legitimate drv that was
/// marked failed by a sick worker, or a drv whose environment was
/// fixed before the cache TTL expired.
///
/// Returns the number of rows deleted — callers surface it so the
/// operator can tell their `/admin/refute` call actually hit
/// something (empty `output_paths` + `drv_hash` unspecified is a
/// 0-row no-op, which is fine for idempotency).
///
/// `output_paths` deletes specific entries; `drv_hash` deletes every
/// entry pointing at a derivation (useful when a single failed
/// drv's outputs are distributed across many paths). Passing both
/// deletes the union.
pub async fn delete_failed_outputs(
    pool: &PgPool,
    drv_hash: Option<&DrvHash>,
    output_paths: &[String],
) -> Result<u64> {
    if drv_hash.is_none() && output_paths.is_empty() {
        return Ok(0);
    }
    // Two-arm WHERE so a caller can refute by drv_hash, by path,
    // or both in one call. `COALESCE` on empty arrays lets the
    // `$2 = ANY($2)` arm be "absent" without a separate query.
    let paths_ref: Vec<&str> = output_paths.iter().map(String::as_str).collect();
    let drv_hash_str = drv_hash.map(|h| h.as_str()).unwrap_or("");
    let has_drv_hash = drv_hash.is_some();
    let query = r#"
        DELETE FROM failed_outputs
        WHERE (output_path = ANY($1::text[]))
           OR ($2 AND drv_hash = $3)
        "#;
    let result = sqlx::query(query)
        .bind(&paths_ref)
        .bind(has_drv_hash)
        .bind(drv_hash_str)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
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
