//! L4 degradation-contract tests.
//!
//! The degradation contract (see `docs/SCALE.md`):
//!
//! 1. **Claim-overload shedding** — when `claims_in_flight >=
//!    max_claims_in_flight`, the coordinator returns 503 + Retry-After
//!    for new claim requests. Existing claims continue unaffected.
//!    Clients see a clean rejection, not a hang or OOM.
//! 2. **PG outage** — claim + non-terminal complete stay live (see
//!    `pg_faults.rs`).
//! 3. **Oversized ingest** — rejected at ingest time with 413 +
//!    `eval_too_large` (covered by `oversized.rs`).
//! 4. **Memory ceiling** — `Submission::failures` truncated to
//!    `max_failures_in_result` (covered by `scale.rs`).
//!
//! These tests prove the contract holds — NOT that the coordinator
//! is fast under stress. A 503 under overload is a pass, not a fail.

mod common;

use std::sync::Arc;

use common::{scrape_metric, spawn_server_with_cfg};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CreateJobRequest, IngestDrvRequest};
use sqlx::PgPool;

// ─── Claim shedding under overload ─────────────────────────────────

/// With `max_claims_in_flight = 2`, issuing 3 claims in a row should
/// see the 3rd rejected with 503. The rejection is clean (Retry-After
/// header set), the `overload_rejections` counter increments, and the
/// in-flight gauge reflects only the 2 successful claims.
#[sqlx::test]
async fn claim_shedding_at_threshold(pool: PgPool) {
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.max_claims_in_flight = Some(2);
    })
    .await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("deg-shed".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    // Three drvs so three concurrent claims can race.
    for i in 0..3 {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: format!("/nix/store/shd{i:04}-x.drv"),
                    drv_name: format!("shd{i}"),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                },
            )
            .await
            .unwrap();
    }
    client.seal(job.id).await.unwrap();

    // First two claims succeed (hold without completing).
    let c1 = client
        .claim(job.id, "x86_64-linux", &[], 1)
        .await
        .unwrap()
        .expect("claim 1 succeeds");
    let c2 = client
        .claim(job.id, "x86_64-linux", &[], 1)
        .await
        .unwrap()
        .expect("claim 2 succeeds");

    // Third claim must be shed with 503.
    let c3_err = client
        .claim(job.id, "x86_64-linux", &[], 1)
        .await
        .unwrap_err();
    // The client surfaces the HTTP 503 as an Error::Api / Transport.
    // Exact shape varies; we just need confirmation that the request
    // did not return Ok(Some(_)). A clean error (not a hang) is a pass.
    let err_msg = format!("{c3_err:?}");
    assert!(
        err_msg.contains("503") || err_msg.to_lowercase().contains("unavailable"),
        "expected 503 under overload, got {err_msg}"
    );

    // Overload counter must be non-zero.
    let rejected = scrape_metric(&handle.base_url, "nix_ci_overload_rejections_total", &[])
        .await
        .unwrap_or(0.0);
    assert!(
        rejected >= 1.0,
        "overload_rejections must increment on shed (saw {rejected})"
    );

    // Prevent unused warnings + ensure claims survive through the assertion.
    let _ = (c1.claim_id, c2.claim_id);
}

// ─── Existing claims continue under new-claim shedding ─────────────

/// Once the threshold is hit, the coordinator shed NEW claims — but
/// workers with ACTIVE claims must still be able to complete. This is
/// the critical part of the degradation contract: shedding doesn't
/// penalize in-flight work, only new starts.
#[sqlx::test]
async fn active_claims_complete_despite_shedding(pool: PgPool) {
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.max_claims_in_flight = Some(1);
    })
    .await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("deg-complete".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    for i in 0..2 {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: format!("/nix/store/ac{i:04}-x.drv"),
                    drv_name: format!("ac{i}"),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                },
            )
            .await
            .unwrap();
    }
    client.seal(job.id).await.unwrap();

    // First claim succeeds.
    let c1 = client
        .claim(job.id, "x86_64-linux", &[], 1)
        .await
        .unwrap()
        .expect("claim 1");

    // Second is shed.
    let shed = client.claim(job.id, "x86_64-linux", &[], 1).await;
    assert!(shed.is_err(), "second claim must be shed: got {shed:?}");

    // Now complete the first claim. This must succeed despite the
    // shedding state — complete is always allowed.
    let resp = client
        .complete(
            job.id,
            c1.claim_id,
            &nix_ci_core::types::CompleteRequest {
                success: true,
                duration_ms: 1,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .expect("complete after shed must succeed");
    assert!(!resp.ignored);

    // And now a new claim should succeed (in-flight dropped to 0).
    let c2 = client
        .claim(job.id, "x86_64-linux", &[], 1)
        .await
        .unwrap()
        .expect("claim after complete must succeed");
    assert_ne!(c2.claim_id, c1.claim_id);
}
