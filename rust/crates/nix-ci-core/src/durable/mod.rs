//! Durable layer: sqlx Postgres pool, migrations, advisory-lock
//! single-writer primitive, terminal-state writeback, boot-time
//! clear_busy, heartbeat reaper, TTL cleanup.

pub mod cleanup;
pub mod logs;
pub mod reaper;
pub mod writeback;

use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{ConnectOptions, Connection as _, PgConnection};

use crate::error::Result;

/// Open the Postgres pool and run pending migrations. Every connection
/// the pool hands out has `statement_timeout` set to
/// `pg_statement_timeout_ms` — a safety net so a runaway query can't
/// hold locks indefinitely. Migrations are run with the timeout in
/// place too (migrations are small; if one exceeds 60s something is
/// seriously wrong and failing loudly at boot is correct).
pub async fn connect_and_migrate(
    database_url: &str,
    pg_statement_timeout_ms: u64,
) -> Result<PgPool> {
    let opts: PgConnectOptions = database_url
        .parse::<PgConnectOptions>()
        .map_err(|e| crate::Error::Config(format!("bad DATABASE_URL: {e}")))?
        .log_statements(tracing::log::LevelFilter::Debug);

    let pool = PgPoolOptions::new()
        .max_connections(16)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                if pg_statement_timeout_ms > 0 {
                    // `SET` applies to the session (i.e. this pooled
                    // connection for its lifetime). Integer parameter
                    // is milliseconds when quoted; `0` means disabled
                    // in Postgres. We already guard with `> 0` so we
                    // never issue the degenerate disable case here.
                    sqlx::query(&format!(
                        "SET statement_timeout = {pg_statement_timeout_ms}"
                    ))
                    .execute(conn)
                    .await?;
                }
                Ok(())
            })
        })
        .connect_with(opts)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("database migrations up to date");
    Ok(pool)
}

/// Boot-time state repair. Every non-terminal job from a prior
/// coordinator lifetime is flipped to `cancelled` with a sentinel
/// result; the in-memory dispatcher was never rebuilt, so those jobs
/// are unreachable and their workers' next poll will 410. Also evicts
/// expired entries from the `failed_outputs` TTL cache so the first
/// cleanup tick doesn't have to. Idempotent.
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

/// Single-writer enforcement via `pg_advisory_lock`. Owns a dedicated
/// connection separate from the pool; on `Drop` the connection is
/// closed and Postgres releases the lock automatically.
///
/// Why a dedicated connection: advisory locks are session-scoped. If
/// we borrowed from the pool and returned on Drop, the lock would
/// outlive the `CoordinatorLock` value until the pool recycles that
/// specific connection (which may be never). Tying the lock to a
/// single-use connection makes lifetime the same as the handle.
pub struct CoordinatorLock {
    _conn: PgConnection,
}

impl CoordinatorLock {
    /// Blocking acquire. Blocks until the lock is held. Standby
    /// coordinators block here; when the primary's session drops,
    /// Postgres releases the lock and one standby unblocks.
    pub async fn acquire(database_url: &str, key: i64) -> Result<Self> {
        let opts = database_url
            .parse::<PgConnectOptions>()
            .map_err(|e| crate::Error::Config(format!("bad DATABASE_URL: {e}")))?;
        let mut conn = PgConnection::connect_with(&opts).await?;
        tracing::info!(
            key,
            "acquiring coordinator advisory lock (may block on standby)"
        );
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(key)
            .execute(&mut conn)
            .await?;
        tracing::info!(key, "coordinator lock held");
        Ok(Self { _conn: conn })
    }
}
