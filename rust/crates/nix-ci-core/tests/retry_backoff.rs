//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: retryable failure retry budget and linear backoff.

mod common;

use std::time::Duration;

use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, ErrorCategory, JobStatus};
use sqlx::PgPool;

#[sqlx::test]
async fn retryable_exhaustion_becomes_terminal(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("flk", "flaky");
    client
        .ingest_drv(job.id, &ingest(&drv, "flaky", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Exhaust max_tries (default = 2). Each iteration: claim → fail as
    // transient → DB state flips to pending with backoff. Cheat the
    // backoff timer between attempts. Count successful claims so a
    // mutation that loosens the retry predicate (attempt < vs attempt
    // <=) would let extra attempts sneak through and fail the equality
    // assertion below.
    let mut successful_claims = 0;
    let max_attempts_expected = 2; // matches default ServerConfig.max_attempts
    for iter in 1..=(max_attempts_expected + 2) {
        let c = match client.claim(job.id, "x86_64-linux", &[], 5).await {
            Ok(Some(c)) => c,
            Ok(None) => break,
            // After terminal failure the submission is removed from
            // the dispatcher map and claim returns 410 Gone.
            Err(nix_ci_core::Error::Gone(_)) => break,
            Err(e) => panic!("unexpected claim error: {e}"),
        };
        assert_eq!(
            c.attempt, iter,
            "attempt numbers must be strictly sequential"
        );
        successful_claims += 1;
        client
            .complete(
                job.id,
                c.claim_id,
                &CompleteRequest {
                    success: false,
                    duration_ms: 5,
                    exit_code: Some(137),
                    error_category: Some(ErrorCategory::Transient),
                    error_message: Some("flake".into()),
                    log_tail: None,
                },
            )
            .await
            .unwrap();
        // Clear backoff for the next iteration (in-memory only —
        // there's no durable derivation row to reset).
        let drv_hash = nix_ci_core::types::drv_hash_from_path(&drv).unwrap();
        if let Some(step) = handle.dispatcher.steps.get(&drv_hash) {
            step.next_attempt_at
                .store(0, std::sync::atomic::Ordering::Release);
        }
    }
    assert_eq!(
        successful_claims, max_attempts_expected,
        "exactly max_attempts claims should have been issued"
    );

    // Job should now be failed (final attempt exhausted retries).
    let mut status = client.status(job.id).await.unwrap();
    for _ in 0..20 {
        if status.status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        status = client.status(job.id).await.unwrap();
    }
    assert_eq!(status.status, JobStatus::Failed);
}

#[sqlx::test]
async fn flaky_retry_sets_next_attempt_at_to_linear_backoff(pool: PgPool) {
    // The retry backoff for attempt N is `backoff_step_ms * N`, stored
    // as an absolute wall-clock millis. Arithmetic mutations (× → +,
    // + → -, + → *) silently wreck this: too-short backoff hammers the
    // coordinator on every claim loop; too-long backoff starves the
    // retry.
    use nix_ci_core::config::ServerConfig;
    const STEP_MS: i64 = 5_000;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.flaky_retry_backoff_step_ms = STEP_MS;
        // Allow attempts 1..=3 as retryable so we can observe both
        // attempt=1 and attempt=2 backoff windows.
        cfg.max_attempts = 4;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("retrybk", "retryme");
    client
        .ingest_drv(job.id, &ingest(&drv, "retryme", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // For each attempt in 1..=2, fail as Transient and assert
    // next_attempt_at = now + STEP_MS * attempt (± a tight window).
    // Attempt=2 is critical: it's where `*` vs `+` diverges visibly
    // (5000*2=10000 vs 5000+2=5002), and where `*` vs `/` does too
    // (5000*2=10000 vs 5000/2=2500).
    let drv_hash = nix_ci_core::types::drv_hash_from_path(&drv).unwrap();
    // max_tries default is 2 → attempts 1 and 2 are retryable.
    for attempt_n in 1..=2 {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 3)
            .await
            .unwrap()
            .expect("claim");
        assert_eq!(c.attempt, attempt_n);
        let before_ms = chrono::Utc::now().timestamp_millis();
        client
            .complete(
                job.id,
                c.claim_id,
                &CompleteRequest {
                    success: false,
                    duration_ms: 1,
                    exit_code: Some(137),
                    error_category: Some(ErrorCategory::Transient),
                    error_message: Some("net".into()),
                    log_tail: None,
                },
            )
            .await
            .unwrap();
        let after_ms = chrono::Utc::now().timestamp_millis();

        let step = handle
            .dispatcher
            .steps
            .get(&drv_hash)
            .expect("step still in registry during retry");
        let next = step
            .next_attempt_at
            .load(std::sync::atomic::Ordering::Acquire);
        let expected = STEP_MS * i64::from(attempt_n);
        let lo = before_ms + expected;
        let hi = after_ms + expected + 100; // generous upper margin
        assert!(
            (lo..=hi).contains(&next),
            "attempt={attempt_n}: next_attempt_at={next} outside [{lo}, {hi}] (expected ~+{expected}ms)"
        );
        // Clear backoff to let attempt N+1 run immediately.
        step.next_attempt_at
            .store(0, std::sync::atomic::Ordering::Release);
    }
}
