//! H8.2 regression: verify `statement_timeout` is actually set on
//! every pooled connection and is honored by Postgres. A slow-query
//! safety net is worthless if the session doesn't have the setting
//! applied — this locks down the wire-up.

use std::time::Duration;

use sqlx::PgPool;

/// After calling `connect_and_migrate` with a timeout, any query on a
/// pooled connection must see the value. Checked by asking Postgres
/// directly via `SHOW statement_timeout`.
#[sqlx::test]
async fn statement_timeout_set_on_pooled_connection(_pool: PgPool) {
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for this test");
    let pool = nix_ci_core::durable::connect_and_migrate(&db_url, 2_500)
        .await
        .expect("connect");
    let (timeout,): (String,) = sqlx::query_as("SHOW statement_timeout")
        .fetch_one(&pool)
        .await
        .unwrap();
    // Postgres formats as "2500ms" / "2s500ms" / "2.5s" depending on
    // version. Accept any form that parses to 2500ms.
    let parsed = parse_pg_interval_ms(&timeout).unwrap_or_default();
    assert_eq!(
        parsed, 2_500,
        "statement_timeout must be exactly the configured value; got {timeout}"
    );
}

/// A query that exceeds the configured timeout must be aborted by
/// Postgres. This is the actual safety-net behavior — if the SET
/// didn't take effect, pg_sleep would block for 5s and the test
/// would fail via tokio timeout.
#[sqlx::test]
async fn long_query_aborted_by_statement_timeout(_pool: PgPool) {
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for this test");
    let pool = nix_ci_core::durable::connect_and_migrate(&db_url, 500)
        .await
        .expect("connect");
    let start = std::time::Instant::now();
    let res = tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query("SELECT pg_sleep(3)").execute(&pool),
    )
    .await
    .expect("tokio timeout should not fire; PG should abort first");

    assert!(
        res.is_err(),
        "pg_sleep(3) with a 500ms statement_timeout must return an error"
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "PG should abort within ~500ms; took {elapsed:?}"
    );
    // The specific error should mention "canceling statement" (Postgres
    // 57014 error code) — a general connection error wouldn't.
    let err = res.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_ascii_lowercase().contains("canceling")
            || msg.to_ascii_lowercase().contains("timeout"),
        "error should be timeout-related; got: {msg}"
    );
}

/// Parse Postgres's SHOW interval format (e.g. "2500ms", "2s", "2.5s")
/// into milliseconds. Returns None if the format is unrecognized.
fn parse_pg_interval_ms(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some(ms) = s.strip_suffix("ms") {
        return ms.trim().parse::<u64>().ok();
    }
    if let Some(sec) = s.strip_suffix('s') {
        let f: f64 = sec.trim().parse().ok()?;
        return Some((f * 1000.0).round() as u64);
    }
    None
}

#[cfg(test)]
mod parse_tests {
    use super::parse_pg_interval_ms;

    #[test]
    fn parse_ms_suffix() {
        assert_eq!(parse_pg_interval_ms("2500ms"), Some(2500));
    }

    #[test]
    fn parse_s_integer() {
        assert_eq!(parse_pg_interval_ms("2s"), Some(2000));
    }

    #[test]
    fn parse_s_fractional() {
        assert_eq!(parse_pg_interval_ms("2.5s"), Some(2500));
    }

    #[test]
    fn parse_unknown_returns_none() {
        assert_eq!(parse_pg_interval_ms("unlimited"), None);
    }
}
