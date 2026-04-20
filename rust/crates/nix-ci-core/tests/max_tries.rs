//! A step whose retry budget is exhausted must not be issued
//! again. Without this, a drv that repeatedly times out
//! (worker crashes, network blips that outlast the retry budget, etc.)
//! would be re-armed by the claim reaper indefinitely — workers
//! would take turns claiming, crashing, and getting re-armed, with no
//! convergence to a terminal job state. From the caller's perspective
//! the job would appear "stuck forever" until the job-heartbeat reaper
//! finally cancels the whole thing.
//!
//! The enforcement lives at claim-issue time (server/claim.rs) because
//! that's the narrow gate every claim passes through — regardless of
//! whether the step became ready via initial ingest, handle_failure
//! retry, or reaper re-arm.

use std::time::Duration;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::config::ServerConfig;
use nix_ci_core::types::{CreateJobRequest, IngestBatchRequest, IngestDrvRequest, JobStatus};
use sqlx::PgPool;

mod common;

fn drv_path(hash: &str, name: &str) -> String {
    format!("/nix/store/{hash}-{name}.drv")
}

/// Core claim: after a step's retry budget is exhausted, claim requests
/// that would pop it transition the step to terminal-failed instead of
/// issuing yet another claim. The job becomes Failed (sealed + all
/// toplevels finished) and callers see a DrvFailure with a
/// max_retries_exceeded message.
#[sqlx::test]
async fn exhausted_step_terminal_fails_on_next_claim(pool: PgPool) {
    // max_attempts=1 makes the first claim also the last. We also set
    // a tight reaper interval so the re-arm happens quickly even in a
    // test harness that doesn't run the reaper task.
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_attempts = 1;
        cfg.claim_deadline_secs = 2;
        cfg.reaper_interval_secs = 1;
    })
    .await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("aaa", "flaky");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![IngestDrvRequest {
                    drv_path: drv.clone(),
                    drv_name: "flaky".into(),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                }],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Attempt 1: worker claims but never reports. Claim expires. Reaper
    // re-arms the step. tries is now 1.
    let c1 = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("first claim");
    assert_eq!(c1.attempt, 1);
    // Force the claim's deadline into the past and reap.
    {
        let claim = handle
            .dispatcher
            .claims
            .take(c1.claim_id)
            .expect("claim present");
        *claim.deadline.lock() = tokio::time::Instant::now() - Duration::from_secs(5);
        handle.dispatcher.claims.insert(claim);
    }
    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);

    // Attempt 2: next claim pops the re-armed step, sees tries >=
    // max_tries, and terminal-fails rather than issuing. The worker's
    // POST /claim returns 204 (nothing to claim) because the only drv
    // in the job just failed.
    let response = client.claim(job.id, "x86_64-linux", &[], 1).await;
    match response {
        // Job may transition terminal between the pop and the next loop
        // iteration — either "nothing to claim" (204 → None) or "job is
        // terminal" (410 → Err::Gone) is acceptable. The invariant we
        // care about: the step did NOT get another claim issued. We
        // verify that observationally below via the job's terminal
        // status and failure record — avoiding direct reads of the
        // dispatcher's in-memory step flags (which would test the
        // shape instead of the contract).
        Ok(None) | Err(nix_ci_core::Error::Gone(_)) => {}
        Ok(Some(c)) => panic!("exhausted step must not issue another claim; got {:?}", c),
        Err(e) => panic!("unexpected error: {e}"),
    }

    // Job converges to Failed with a failure record matching the drv.
    // This is the observable proof that the step terminal-failed:
    // the JobStatus snapshot is how a CCI client actually learns the
    // outcome, and DrvFailure in `failures` is how it learns WHY.
    let status = poll_until_terminal(&client, job.id).await;
    assert_eq!(status.status, JobStatus::Failed);
    assert_eq!(status.failures.len(), 1);
    let f = &status.failures[0];
    assert_eq!(f.drv_name, "flaky");
    assert!(
        f.error_message
            .as_deref()
            .unwrap_or("")
            .contains("max_retries_exceeded"),
        "failure message must explain why: got {:?}",
        f.error_message
    );

    // And the ops counter increments — scraped via /metrics so we
    // test the Prometheus contract, not the in-memory shape.
    let n = common::scrape_metric_expect(
        &handle.base_url,
        "nix_ci_builds_completed_total",
        &[("outcome", "max_retries_exceeded")],
    )
    .await;
    assert_eq!(n, 1.0, "exactly one max_retries_exceeded on /metrics");
}

/// Max_tries enforcement must also fire on the fleet-claim endpoint.
/// Symmetrical coverage — the per-job and fleet paths share the same
/// `step_exhausted` gate, so if one regresses the other is likely to
/// too, but we test both to lock the behavior down.
#[sqlx::test]
async fn exhausted_step_terminal_fails_on_fleet_claim(pool: PgPool) {
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_attempts = 1;
        cfg.claim_deadline_secs = 2;
        cfg.reaper_interval_secs = 1;
    })
    .await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("bbb", "fleet-flaky");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![IngestDrvRequest {
                    drv_path: drv.clone(),
                    drv_name: "fleet-flaky".into(),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                }],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let c1 = client
        .claim_any("x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("first fleet claim");
    // Expire it and reap.
    {
        let claim = handle.dispatcher.claims.take(c1.claim_id).unwrap();
        *claim.deadline.lock() = tokio::time::Instant::now() - Duration::from_secs(5);
        handle.dispatcher.claims.insert(claim);
    }
    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);

    // Second fleet claim sees the exhausted step and terminal-fails.
    let r = client.claim_any("x86_64-linux", &[], 1).await.unwrap();
    assert!(r.is_none(), "no claim issued for exhausted step");

    let status = poll_until_terminal(&client, job.id).await;
    assert_eq!(status.status, JobStatus::Failed);
}

/// A fresh step whose tries == 0 must still be claimable even if
/// max_attempts happens to be low. Guards against a mutation that
/// swaps `>=` for `>` in `step_exhausted`.
#[sqlx::test]
async fn fresh_step_with_zero_tries_still_claimable(pool: PgPool) {
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_attempts = 1;
    })
    .await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("ccc", "fresh");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![IngestDrvRequest {
                    drv_path: drv.clone(),
                    drv_name: "fresh".into(),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                }],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("fresh step must be claimable on first attempt");
    assert_eq!(c.attempt, 1);
}

async fn poll_until_terminal(
    client: &CoordinatorClient,
    id: nix_ci_core::types::JobId,
) -> nix_ci_core::types::JobStatusResponse {
    for _ in 0..60 {
        let s = client.status(id).await.unwrap();
        if s.status.is_terminal() {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("job {id} did not reach terminal status within 3s");
}
