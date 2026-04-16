//! Build log archive. Stores gzipped per-attempt logs in the
//! `build_logs` table behind a [`LogStore`] trait so we can swap to S3
//! / object storage later without touching the HTTP surface.
//!
//! Cap & compression: workers ship logs already gzipped; this module
//! only shuffles bytes. Decompression happens at the read path
//! ([`fetch_decompressed`]) so the on-disk representation stays
//! compact.

use std::io::Read;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::error::{Error, Result};
use crate::types::{BuildLogAttempt, ClaimId, DrvHash, JobId};

/// Metadata accompanying a worker-uploaded log. The bytes are passed
/// separately so impls can avoid copies.
#[derive(Debug, Clone)]
pub struct LogPutRequest {
    pub job_id: JobId,
    pub claim_id: ClaimId,
    pub drv_hash: DrvHash,
    pub attempt: i32,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    /// Size of the *original* log in bytes (pre-gzip, post-truncation).
    pub original_size: u32,
    /// True if the worker had to truncate (raw exceeded its in-memory
    /// cap).
    pub truncated: bool,
}

/// One row from the archive (without the bytes).
#[derive(Debug, Clone)]
pub struct LogRow {
    pub job_id: JobId,
    pub claim_id: ClaimId,
    pub drv_hash: DrvHash,
    pub attempt: i32,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub original_size: u32,
    pub stored_size: u32,
    pub truncated: bool,
}

impl LogRow {
    pub fn into_attempt(self) -> BuildLogAttempt {
        BuildLogAttempt {
            claim_id: self.claim_id,
            attempt: self.attempt,
            success: self.success,
            exit_code: self.exit_code,
            started_at: self.started_at,
            ended_at: self.ended_at,
            original_size: self.original_size,
            stored_size: self.stored_size,
            truncated: self.truncated,
        }
    }
}

/// Storage backend for build logs. Postgres today; S3 / GCS later.
///
/// All methods take `&self` and are expected to be `Send + Sync` so
/// the same instance can be cloned (or wrapped in `Arc`) across async
/// handlers. Implementations should be cheap to clone (typically wrap
/// a `PgPool`-style handle).
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Insert (or upsert on retry) one attempt's gzipped log.
    async fn put(&self, meta: LogPutRequest, gz: Vec<u8>) -> Result<()>;

    /// Fetch a single attempt's gzipped bytes by `claim_id`. Caller
    /// gunzips. Returns `None` if absent.
    async fn fetch_gz(&self, job_id: JobId, claim_id: ClaimId) -> Result<Option<Vec<u8>>>;

    /// List every stored attempt for a (job, drv) ordered newest-first.
    async fn list_attempts(&self, job_id: JobId, drv_hash: &DrvHash) -> Result<Vec<LogRow>>;

    /// Delete rows older than `older_than`. Returns the number of rows
    /// deleted. Used by the cleanup loop.
    async fn prune_older_than(&self, older_than: DateTime<Utc>) -> Result<u64>;

    /// Best-effort byte-size of the table (for the
    /// `nix_ci_build_logs_bytes_total` gauge). Implementations that
    /// can't answer cheaply may return `None`.
    async fn total_bytes(&self) -> Result<Option<u64>>;

    /// Best-effort row count.
    async fn row_count(&self) -> Result<u64>;
}

/// Convenience: fetch + gunzip into a UTF-8 string. Returns `None` if
/// the row is absent. Returns an error if decompression or UTF-8
/// conversion fails.
pub async fn fetch_decompressed(
    store: &dyn LogStore,
    job_id: JobId,
    claim_id: ClaimId,
) -> Result<Option<String>> {
    let Some(gz) = store.fetch_gz(job_id, claim_id).await? else {
        return Ok(None);
    };
    let mut out = String::new();
    flate2::read::GzDecoder::new(gz.as_slice())
        .read_to_string(&mut out)
        .map_err(|e| Error::Internal(format!("gunzip build log: {e}")))?;
    Ok(Some(out))
}

/// Postgres-backed implementation. Cheap to clone (wraps `PgPool`).
#[derive(Clone)]
pub struct PgLogStore {
    pool: PgPool,
}

impl PgLogStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LogStore for PgLogStore {
    async fn put(&self, meta: LogPutRequest, gz: Vec<u8>) -> Result<()> {
        // Upsert: a worker that retried the upload after a network
        // blip should overwrite (same claim_id) rather than 23505.
        sqlx::query(
            r#"
            INSERT INTO build_logs
                (claim_id, job_id, drv_hash, attempt, success, exit_code,
                 started_at, ended_at, original_size, truncated, log_gz)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (claim_id) DO UPDATE SET
                log_gz        = EXCLUDED.log_gz,
                original_size = EXCLUDED.original_size,
                truncated     = EXCLUDED.truncated,
                stored_at     = now()
            "#,
        )
        .bind(meta.claim_id.0)
        .bind(meta.job_id.0)
        .bind(meta.drv_hash.as_str())
        .bind(meta.attempt)
        .bind(meta.success)
        .bind(meta.exit_code)
        .bind(meta.started_at)
        .bind(meta.ended_at)
        .bind(meta.original_size as i32)
        .bind(meta.truncated)
        .bind(gz)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn fetch_gz(&self, job_id: JobId, claim_id: ClaimId) -> Result<Option<Vec<u8>>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT log_gz FROM build_logs WHERE job_id = $1 AND claim_id = $2")
                .bind(job_id.0)
                .bind(claim_id.0)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(g,)| g))
    }

    async fn list_attempts(&self, job_id: JobId, drv_hash: &DrvHash) -> Result<Vec<LogRow>> {
        type Row = (
            sqlx::types::Uuid,
            i32,
            bool,
            Option<i32>,
            DateTime<Utc>,
            DateTime<Utc>,
            i32,
            bool,
            i32,
        );
        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT claim_id, attempt, success, exit_code,
                   started_at, ended_at, original_size, truncated,
                   octet_length(log_gz)
            FROM build_logs
            WHERE job_id = $1 AND drv_hash = $2
            ORDER BY attempt DESC
            "#,
        )
        .bind(job_id.0)
        .bind(drv_hash.as_str())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(cid, attempt, success, exit_code, started_at, ended_at, orig, trunc, stored)| {
                    LogRow {
                        job_id,
                        claim_id: ClaimId(cid),
                        drv_hash: drv_hash.clone(),
                        attempt,
                        success,
                        exit_code,
                        started_at,
                        ended_at,
                        original_size: orig as u32,
                        stored_size: stored.max(0) as u32,
                        truncated: trunc,
                    }
                },
            )
            .collect())
    }

    async fn prune_older_than(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let r = sqlx::query("DELETE FROM build_logs WHERE stored_at < $1")
            .bind(older_than)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected())
    }

    async fn total_bytes(&self) -> Result<Option<u64>> {
        // pg_total_relation_size includes TOAST + indexes, which is
        // what we actually care about for "how much disk does this
        // table cost?". Returns NULL if the relation doesn't exist
        // (shouldn't happen post-migration, but be defensive).
        let row: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT pg_total_relation_size('build_logs')")
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.and_then(|(b,)| b).map(|n| n.max(0) as u64))
    }

    async fn row_count(&self) -> Result<u64> {
        // Use the planner estimate from pg_class — exact count is an
        // O(N) seq scan, which is wasteful for a metric. The estimate
        // is updated by autovacuum and is fine for capacity tracking.
        let row: Option<(Option<f32>,)> = sqlx::query_as(
            "SELECT reltuples FROM pg_class WHERE relname = 'build_logs' AND relkind = 'r'",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row
            .and_then(|(n,)| n)
            .map(|n| n.max(0.0) as u64)
            .unwrap_or(0))
    }
}
