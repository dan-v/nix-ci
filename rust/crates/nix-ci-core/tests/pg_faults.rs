//! L4 Postgres fault-injection probes.
//!
//! The in-memory dispatcher is authoritative for in-flight state;
//! Postgres carries only the durable envelope (jobs.result + failed
//! outputs + build logs). The degradation contract:
//!
//! * Claim (both per-job and fleet): **pure in-memory**, must stay
//!   live under any PG state.
//! * Non-terminal complete (propagation, retry): **pure in-memory**,
//!   must stay live.
//! * Terminal complete (triggers writeback): blocks on PG; under
//!   outage, surfaces a clean error — no partial state.
//! * Ingest's `failed_output_hits`: PG errors degrade to
//!   "no cache hits"; ingest proceeds.
//!
//! These tests exercise the contract with realistic fault modes we
//! can inject without external deps.
//!
//! A fuller fault-injection surface (toxiproxy, connection resets,
//! transaction conflicts) is deferred to the L2 deterministic sim
//! (`tests/sim.rs`), which can drive the real dispatcher against a
//! mock PG under adversarial schedules.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::spawn_server;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, IngestDrvRequest};
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};

// ─── Pool exhaustion doesn't stall the hot path ────────────────────

/// The pool is hardcoded to `max_connections=16`
/// (`durable::connect_and_migrate`). We hold all 16 connections then
/// hammer the claim + non-terminal-complete path. These are pure
/// in-memory; the contract says they must stay responsive regardless
/// of pool state.
///
/// Any call that DOES hit PG (heartbeat, ingest's `failed_output_hits`,
/// terminal writeback) will time out after `acquire_timeout=10s`;
/// that's fine. The point is that a saturated DB doesn't drag down
/// workers whose hot path is in-memory.
#[sqlx::test]
async fn claim_path_live_under_pool_exhaustion(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("pgf-pool-exhaust".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    for i in 0..100 {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: format!("/nix/store/pe{i:04}-x.drv"),
                    drv_name: format!("pe{i}"),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: false, // not toplevel — no terminal writeback fires yet
                    attr: None,
                },
            )
            .await
            .unwrap();
    }

    // Hold every pool connection.
    let hog_pool = pool.clone();
    let hog_task = tokio::spawn(async move {
        let mut held: Vec<PoolConnection<Postgres>> = Vec::new();
        for _ in 0..16 {
            match tokio::time::timeout(Duration::from_secs(5), hog_pool.acquire()).await {
                Ok(Ok(c)) => held.push(c),
                _ => break,
            }
        }
        eprintln!("PG-FAULT/POOL: holding {} connections", held.len());
        tokio::time::sleep(Duration::from_secs(10)).await;
        drop(held);
    });

    // Give the hog a moment to acquire the pool.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Hot path: claim + non-terminal complete, repeated. Each iteration
    // must be sub-second even with zero PG slots available.
    let mut claim_times = Vec::new();
    let mut complete_times = Vec::new();
    for _ in 0..50 {
        let t0 = Instant::now();
        let Some(claim) = client.claim(job.id, "x86_64-linux", &[], 1).await.unwrap() else {
            break;
        };
        claim_times.push(t0.elapsed());

        let t0 = Instant::now();
        client
            .complete(
                job.id,
                claim.claim_id,
                &CompleteRequest {
                    success: true,
                    duration_ms: 1,
                    exit_code: Some(0),
                    error_category: None,
                    error_message: None,
                    log_tail: None,
                },
            )
            .await
            .unwrap();
        complete_times.push(t0.elapsed());
    }

    hog_task.abort();
    let _ = hog_task.await;

    let max_claim = claim_times.iter().max().copied().unwrap_or_default();
    let max_complete = complete_times.iter().max().copied().unwrap_or_default();
    eprintln!(
        "PG-FAULT/POOL: claim_n={} complete_n={} max_claim={:?} max_complete={:?}",
        claim_times.len(),
        complete_times.len(),
        max_claim,
        max_complete,
    );
    assert!(
        !claim_times.is_empty(),
        "claim path dead under pool exhaustion"
    );
    assert!(
        max_claim < Duration::from_secs(1),
        "claim regressed under pool exhaustion: {max_claim:?}"
    );
    assert!(
        max_complete < Duration::from_secs(1),
        "non-terminal complete regressed under pool exhaustion: {max_complete:?}"
    );
}

// ─── Ingest survives PG statement timeout ─────────────────────────

/// Inject a 1ms `statement_timeout` at the DATABASE level so every
/// new connection inherits it. Many PG calls will then fail with
/// `QueryCanceled`. The contract: the coordinator's error handling is
/// sane — no panics, no hangs. `failed_output_hits` specifically
/// swallows its own errors and degrades to "no cache hits" so ingest
/// still proceeds in memory.
///
/// We don't assert a specific outcome (create/ingest may succeed if
/// the warm connection completes under 1ms, or fail cleanly if it
/// doesn't). We assert both paths are bounded and don't panic.
#[sqlx::test]
async fn coordinator_survives_database_statement_timeout(pool: PgPool) {
    // Apply a DB-level 1ms statement timeout. Every future
    // connection (including the server's pool) inherits this.
    //
    // We can't ALTER DATABASE while connected to it, so target the
    // shared maintenance DB (`postgres`). This permanently degrades
    // THIS TEST'S private sqlx::test database until the session ends.
    // sqlx::test tears the DB down after; nothing leaks.
    let dbname: (String,) = sqlx::query_as("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    // Escape the name just in case — sqlx::test generates reasonable
    // identifiers but we don't want a stray quote to become an SQL
    // injection surface even in test code.
    let escaped = dbname.0.replace('"', "\"\"");
    sqlx::query(&format!(
        "ALTER DATABASE \"{escaped}\" SET statement_timeout = 1"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let start = Instant::now();
    let result = client
        .create_job(&CreateJobRequest {
            external_ref: Some("pgf-deg-create".into()),
            ..Default::default()
        })
        .await;
    let elapsed = start.elapsed();
    eprintln!(
        "PG-FAULT/DEG: create_job result={:?} elapsed={:?}",
        result.is_ok(),
        elapsed
    );
    // Contract: bounded-time response, not a hang. The response
    // itself can succeed (warm conn finishes <1ms) or fail cleanly.
    assert!(
        elapsed < Duration::from_secs(15),
        "create_job hung under degraded PG: {elapsed:?}"
    );

    // If create_job succeeded, try ingesting under the degraded pool.
    // `failed_output_hits` will time out; the batch must still
    // complete in-memory.
    if let Ok(job) = result {
        let ingest_start = Instant::now();
        let ingest_result = client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: "/nix/store/deg0001-x.drv".into(),
                    drv_name: "deg0001".into(),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                },
            )
            .await;
        let ingest_elapsed = ingest_start.elapsed();
        eprintln!(
            "PG-FAULT/DEG: ingest result={:?} elapsed={:?}",
            ingest_result.is_ok(),
            ingest_elapsed
        );
        assert!(
            ingest_elapsed < Duration::from_secs(15),
            "ingest hung under degraded PG: {ingest_elapsed:?}"
        );
    }
}
