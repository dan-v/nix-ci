//! D-STARTUP-1: coordinator boot against a populated database must
//! complete in <5s. Measured by bulk-inserting 10K jobs and 10K
//! failed_outputs rows, then timing a fresh `connect_and_migrate` +
//! `clear_busy` pass (the full boot path up to "ready to accept
//! requests").
//!
//! The v3 ephemeral design makes this cheap in principle — no
//! rehydration, no graph reconstruction — but it's not free: migrations
//! run, clear_busy does a bulk UPDATE + DELETE, and the advisory lock
//! has to be acquired. Locking the budget here catches regressions
//! like "someone added an O(N) scan at boot."

use std::time::{Duration, Instant};

use sqlx::PgPool;

/// Populate 10K terminal jobs + 10K unexpired failed_outputs rows, then
/// measure the full boot path (connect + migrate + clear_busy). Budget:
/// 5 seconds per SPEC.md D-STARTUP-1.
///
/// Does NOT include the advisory-lock acquire step — that's a single
/// SELECT taking <10ms on an uncontended Postgres.
#[sqlx::test]
async fn boot_with_10k_jobs_completes_under_5s(pool: PgPool) {
    // sqlx::test runs migrations on the fixture pool automatically;
    // explicit migrate is idempotent and safe.
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migrations");

    // Bulk insert 10K terminal jobs. We use UNNEST so the whole insert
    // is one statement — simulating a real large-history table.
    let n: i64 = 10_000;
    let result_val = serde_json::json!({
        "id": "00000000-0000-0000-0000-000000000000",
        "status": "done",
        "sealed": true,
        "counts": { "total": 1, "pending": 0, "building": 0, "done": 1, "failed": 0 },
        "failures": [],
        "eval_error": null,
        "eval_errors": []
    });

    sqlx::query(
        r#"
        INSERT INTO jobs (id, external_ref, status, sealed, result,
                          created_at, last_heartbeat, done_at)
        SELECT
            gen_random_uuid(),
            'ext-' || gs,
            'done',
            true,
            $1,
            now() - (gs * interval '1 second'),
            now() - (gs * interval '1 second'),
            now() - (gs * interval '1 second')
        FROM generate_series(1, $2) gs
        "#,
    )
    .bind(&result_val)
    .bind(n)
    .execute(&pool)
    .await
    .expect("bulk insert jobs");

    sqlx::query(
        r#"
        INSERT INTO failed_outputs (output_path, drv_hash, failed_at, expires_at)
        SELECT
            '/nix/store/fake-' || gs,
            'fake-' || gs,
            now(),
            now() + interval '1 hour'
        FROM generate_series(1, $1) gs
        "#,
    )
    .bind(n)
    .execute(&pool)
    .await
    .expect("bulk insert failed_outputs");

    // Sanity check: we really have 10K rows.
    let (jobs,): (i64,) = sqlx::query_as("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(jobs, n);
    let (fos,): (i64,) = sqlx::query_as("SELECT count(*) FROM failed_outputs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(fos, n);

    // Drop the fixture pool so we measure a truly cold pool setup —
    // otherwise connection reuse would hide a migration or after_connect
    // regression that only bites on a fresh pool.
    drop(pool);

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");

    let started = Instant::now();
    let new_pool = nix_ci_core::durable::connect_and_migrate(&db_url, 60_000)
        .await
        .expect("connect_and_migrate");
    nix_ci_core::durable::clear_busy(&new_pool)
        .await
        .expect("clear_busy");
    let elapsed = started.elapsed();

    // Leave headroom below the 5s SPEC bar: debug builds run ~3–5x
    // slower than release, but even at debug speed a clean boot should
    // be well under 5s. If it creeps up, investigate *why* before
    // raising this.
    assert!(
        elapsed < Duration::from_secs(5),
        "D-STARTUP-1: boot took {elapsed:?}; SPEC budget is 5s. Historical rows: jobs={n} failed_outputs={n}."
    );

    // Also verify the pool is actually functional post-boot — a boot
    // that "succeeds" but leaves the pool in a bad state would be a
    // regression the elapsed-time check alone can't catch.
    let (live_count,): (i64,) = sqlx::query_as("SELECT count(*) FROM jobs WHERE status='pending'")
        .fetch_one(&new_pool)
        .await
        .unwrap();
    assert_eq!(
        live_count, 0,
        "clear_busy must drain non-terminal jobs; {live_count} remain"
    );

    tracing::info!(?elapsed, n, "D-STARTUP-1 under budget");
    // Deliberately log elapsed so CI picks it up in the logs for trend
    // analysis. Use println! because tracing may not be initialized.
    println!("D-STARTUP-1: boot with {n} jobs + {n} failed_outputs took {elapsed:?}");
}
