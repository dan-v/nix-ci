//! Memory-bound & resource-cleanup probes.
//!
//! These tests each exercise one of the unbounded-growth risks that
//! the L3 scale survey flagged, asserting the coordinator's cap or
//! cleanup path actually fires. Unlike the scale tests in
//! `tests/scale.rs`, these run in seconds — they're gated on NO
//! feature flag and run on every PR.
//!
//! Coverage map:
//!
//! | Risk                                | Probe                                        |
//! |-------------------------------------|----------------------------------------------|
//! | `Submission::failures` vec growth   | `tests/scale.rs::scale_failures_vec_...`     |
//! |                                     | + `result_jsonb_size_bounded`                 |
//! | `Claims` map leak post-complete     | `claims_map_drains_after_all_completes`      |
//! | `StepsRegistry` weak-ref leak       | `steps_registry_gcd_after_submission_drop`   |
//! | `Submissions::sorted_by_created_at` | `fleet_scan_cost_sublinear_at_1k_subs`       |
//! |   O(N) cost under submission growth |                                              |
//! | `ready` deque unbounded on starve   | `ready_deques_drain_on_cancel`               |

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::spawn_server;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, IngestDrvRequest};
use sqlx::PgPool;

// ─── Claims map drains after complete ──────────────────────────────

/// After every worker completes its claim, the in-memory `Claims` map
/// must be empty. A leak here — say, a missing `decrement_active_claim`
/// on an error path — would inflate `claims_in_flight` forever and
/// pin drv_hashes in memory.
#[sqlx::test]
async fn claims_map_drains_after_all_completes(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("mb-claims-drain".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    for i in 0..50 {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: format!("/nix/store/mc{i:04}-x.drv"),
                    drv_name: format!("mc{i}"),
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

    // Drive to terminal with a single worker. Once the job terminates
    // the per-job claim endpoint returns 410 Gone — we treat that as
    // "no more work" and exit the loop cleanly.
    loop {
        match client.claim(job.id, "x86_64-linux", &[], 3).await {
            Ok(Some(claim)) => {
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
            }
            Ok(None) => {
                if client.status(job.id).await.unwrap().status.is_terminal() {
                    break;
                }
            }
            Err(_) => {
                // Job terminated (410 Gone); we're done.
                if client.status(job.id).await.unwrap().status.is_terminal() {
                    break;
                }
            }
        }
    }

    // After terminal, claims must be empty. Reading the dispatcher's
    // internal map directly is the observable contract for this probe.
    assert!(
        handle.dispatcher.claims.is_empty(),
        "claims map leaked after terminal; size={}",
        handle.dispatcher.claims.len()
    );
}

// ─── Steps registry GC after submission drop ───────────────────────

/// When a submission reaches terminal and is removed from the
/// Submissions map, the last strong ref to its members is released.
/// A subsequent `StepsRegistry::live()` walk must GC every weak entry
/// — if the `retain` branch misfires, the registry size stays pinned.
#[sqlx::test]
async fn steps_registry_gcd_after_submission_drop(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("mb-registry-gc".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    for i in 0..100 {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: format!("/nix/store/reg{i:04}-x.drv"),
                    drv_name: format!("reg{i}"),
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
    assert_eq!(
        handle.dispatcher.steps.len(),
        100,
        "precondition: 100 steps after ingest"
    );

    // Complete every drv. Per-job claim returns 410 Gone once the
    // submission terminates; treat that as "done" and exit cleanly.
    loop {
        match client.claim(job.id, "x86_64-linux", &[], 3).await {
            Ok(Some(claim)) => {
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
            }
            Ok(None) | Err(_) => {
                if client.status(job.id).await.unwrap().status.is_terminal() {
                    break;
                }
            }
        }
    }
    let status = client.status(job.id).await.unwrap();
    assert!(status.status.is_terminal(), "job must be terminal");
    // Submission was removed by check_and_publish_terminal; the last
    // strong ref to members is now gone.
    let live = handle.dispatcher.steps.live();
    assert_eq!(
        live.len(),
        0,
        "StepsRegistry leaked {} entries after submission drop",
        live.len(),
    );
}

// ─── Fleet scan cost scaling ───────────────────────────────────────

/// Create 1000 live submissions (no workers, no completion), then
/// measure how long a single fleet-claim scan takes. The dual-indexed
/// `sorted_by_created_at` must be O(N) — not O(N log N) or worse —
/// and the constant factor must be tight enough that a single wake-up
/// of 1000 workers doesn't produce pathological tails.
///
/// **SLO asserted**: one fleet-claim round trip at 1000 live subs
/// completes in < 200ms. A quadratic regression (pre-fix code) would
/// blow past this.
#[sqlx::test]
async fn fleet_scan_cost_sublinear_at_1k_subs(pool: PgPool) {
    const SUBS: usize = 1_000;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));

    for j in 0..SUBS {
        let job = client
            .create_job(&CreateJobRequest {
                external_ref: Some(format!("mb-fleet-{j}")),
                ..Default::default()
            })
            .await
            .unwrap();
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: format!("/nix/store/fl{j:05}-x.drv"),
                    drv_name: format!("fl{j}"),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                },
            )
            .await
            .unwrap();
        client.seal(job.id).await.unwrap();
    }
    assert_eq!(handle.dispatcher.submissions.len(), SUBS);

    // Single fleet claim — drains one drv from one submission.
    let t0 = Instant::now();
    let c = client
        .claim_any_as_worker("x86_64-linux", &[], 1, Some("probe"))
        .await
        .unwrap();
    let elapsed = t0.elapsed();
    assert!(c.is_some(), "fleet claim returned nothing");
    eprintln!(
        "MEMORY/FLEET-SCAN: 1 claim across {} subs took {:.2}ms",
        SUBS,
        elapsed.as_secs_f64() * 1000.0,
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "fleet scan regression at {SUBS} subs: {elapsed:?} >= 200ms"
    );
}

// ─── Cancel drains ready deques ────────────────────────────────────

/// Cancel a submission with thousands of runnable drvs in its ready
/// queue. The submission is removed, claims evicted, and subsequent
/// fleet claims don't see dangling entries. The weak refs in the
/// ready deque become unreachable and are dropped when the Submission
/// Arc is dropped.
#[sqlx::test]
async fn ready_deques_drain_on_cancel(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("mb-cancel-drain".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    for i in 0..500 {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: format!("/nix/store/c{i:04}-x.drv"),
                    drv_name: format!("c{i}"),
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
    assert_eq!(handle.dispatcher.submissions.len(), 1);

    client.cancel(job.id).await.unwrap();

    // Submission must be gone; claims map empty; registry GC'd.
    assert_eq!(
        handle.dispatcher.submissions.len(),
        0,
        "cancel must remove the submission"
    );
    assert!(
        handle.dispatcher.claims.is_empty(),
        "cancel must evict in-flight claims"
    );
    let live = handle.dispatcher.steps.live();
    assert_eq!(
        live.len(),
        0,
        "registry must GC drvs when cancelled submission was the only holder"
    );
}
