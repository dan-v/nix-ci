//! Durable layer: sqlx Postgres pool, migrations, advisory-lock
//! single-writer primitive, terminal-state writeback, boot-time
//! clear_busy, heartbeat reaper, TTL cleanup.

pub mod cleanup;
pub mod reaper;
pub mod rehydrate;
pub mod writeback;

use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{ConnectOptions, Connection as _, PgConnection};

use crate::error::Result;

/// Open the Postgres pool and run pending migrations.
pub async fn connect_and_migrate(database_url: &str) -> Result<PgPool> {
    let opts: PgConnectOptions = database_url
        .parse::<PgConnectOptions>()
        .map_err(|e| crate::Error::Config(format!("bad DATABASE_URL: {e}")))?
        .log_statements(tracing::log::LevelFilter::Debug);

    let pool = PgPoolOptions::new()
        .max_connections(16)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("database migrations up to date");
    Ok(pool)
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
