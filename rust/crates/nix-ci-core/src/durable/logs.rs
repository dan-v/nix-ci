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

    /// Byte-based pruning. If `sum(data-bytes-of-stored-blobs)` exceeds
    /// `max_bytes`, delete oldest rows until the sum drops back under
    /// the cap. Returns the number of rows deleted. Used by the cleanup
    /// loop's disk guard. Implementations MUST measure against their
    /// own data-bytes metric (not `pg_total_relation_size` for the PG
    /// backend — that can't drop without autovacuum, so pruning
    /// decisions against it never converge).
    async fn prune_to_byte_ceiling(&self, max_bytes: u64) -> Result<u64>;

    /// Best-effort byte-size of the table (for the
    /// `nix_ci_build_logs_bytes_total` gauge). Implementations that
    /// can't answer cheaply may return `None`.
    async fn total_bytes(&self) -> Result<Option<u64>>;

    /// Best-effort row count.
    async fn row_count(&self) -> Result<u64>;
}

/// Convenience: fetch + gunzip into a display-safe string. Returns
/// `None` if the row is absent. Returns an error only if decompression
/// itself fails (truncated / corrupt gzip).
///
/// Non-UTF-8 bytes in the decompressed payload are mapped to U+FFFD via
/// `String::from_utf8_lossy` rather than failing. The worker deliberately
/// stores raw stderr bytes (comment in runner/worker.rs: "avoiding the
/// lossy conversion preserves binary diagnostic output"), so a strict
/// UTF-8 read here would reject every build whose stderr contained a
/// core-dump fragment or non-UTF-8 locale output. The replacement char
/// surfaces in the operator's log view; all the surrounding text is
/// preserved.
pub async fn fetch_decompressed(
    store: &dyn LogStore,
    job_id: JobId,
    claim_id: ClaimId,
) -> Result<Option<String>> {
    let Some(gz) = store.fetch_gz(job_id, claim_id).await? else {
        return Ok(None);
    };
    let mut raw: Vec<u8> = Vec::new();
    flate2::read::GzDecoder::new(gz.as_slice())
        .read_to_end(&mut raw)
        .map_err(|e| Error::Internal(format!("gunzip build log: {e}")))?;
    Ok(Some(String::from_utf8_lossy(&raw).into_owned()))
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

    async fn prune_to_byte_ceiling(&self, max_bytes: u64) -> Result<u64> {
        // Measure data bytes via octet_length(log_gz). This is the
        // data-only sum; unlike pg_total_relation_size it drops
        // immediately on DELETE without needing autovacuum, so a cap
        // decision made against it converges in a single cleanup tick.
        //
        // Fast check first — on every tick this is the common path
        // (we're under the cap). Avoids the expensive CTE below.
        let sum_row: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT COALESCE(sum(octet_length(log_gz))::bigint, 0) FROM build_logs")
                .fetch_optional(&self.pool)
                .await?;
        let current: i64 = sum_row.and_then(|(v,)| v).unwrap_or(0);
        if current <= 0 {
            return Ok(0);
        }
        // `max_bytes` fits in i64 for the foreseeable future (≤ 8 EiB).
        // Compare in i64 to avoid signed/unsigned churn; overflow is
        // trivially avoided — if `max_bytes > i64::MAX` the cast
        // saturates at i64::MAX and we simply never prune, which is
        // the right behavior (cap is effectively off).
        let cap: i64 = max_bytes.min(i64::MAX as u64) as i64;
        if current <= cap {
            return Ok(0);
        }

        // Over the cap. Keep the newest rows whose cumulative size fits
        // under `cap`; delete the rest. Single pass via a window
        // function — two-stage CTE so `DELETE` can reference the keep
        // set without scanning the whole table twice.
        let r = sqlx::query(
            r#"
            WITH ranked AS (
                SELECT claim_id,
                       SUM(octet_length(log_gz))
                         OVER (ORDER BY stored_at DESC, claim_id DESC) AS running_bytes
                FROM build_logs
            ),
            keep AS (
                SELECT claim_id FROM ranked WHERE running_bytes <= $1
            )
            DELETE FROM build_logs
            WHERE claim_id NOT IN (SELECT claim_id FROM keep)
            "#,
        )
        .bind(cap)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// In-memory LogStore returning a pre-seeded gzipped blob.
    struct SeededStore {
        gz: Vec<u8>,
    }

    #[async_trait]
    impl LogStore for SeededStore {
        async fn put(&self, _meta: LogPutRequest, _gz: Vec<u8>) -> Result<()> {
            unreachable!("not exercised")
        }
        async fn fetch_gz(&self, _job_id: JobId, _claim_id: ClaimId) -> Result<Option<Vec<u8>>> {
            Ok(Some(self.gz.clone()))
        }
        async fn list_attempts(&self, _job_id: JobId, _drv_hash: &DrvHash) -> Result<Vec<LogRow>> {
            Ok(Vec::new())
        }
        async fn prune_older_than(&self, _older_than: DateTime<Utc>) -> Result<u64> {
            Ok(0)
        }
        async fn prune_to_byte_ceiling(&self, _max_bytes: u64) -> Result<u64> {
            Ok(0)
        }
        async fn total_bytes(&self) -> Result<Option<u64>> {
            Ok(None)
        }
        async fn row_count(&self) -> Result<u64> {
            Ok(0)
        }
    }

    /// Workers capture raw stderr bytes; decoding must not reject
    /// non-UTF-8 sequences. Invalid bytes surface as U+FFFD; the
    /// surrounding valid bytes are preserved verbatim.
    #[tokio::test]
    async fn fetch_decompressed_handles_non_utf8_payload() {
        let raw: Vec<u8> = vec![b'e', b'r', b'r', b':', b' ', 0xFF, 0xFE, 0xFD, b'\n'];
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&raw).unwrap();
        let gz = enc.finish().unwrap();

        let store = SeededStore { gz };
        let out = fetch_decompressed(&store, JobId::new(), ClaimId::new())
            .await
            .expect("fetch_decompressed must succeed")
            .expect("store returned Some");
        assert!(out.starts_with("err: "));
        assert!(out.ends_with('\n'));
        assert!(out.contains('\u{FFFD}'), "got: {out:?}");
    }
}
