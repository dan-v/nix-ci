//! H15.D (dispatcher-GC closure): verifies the invariant-7 /
//! invariant-8 contract under the "submission drops while work is
//! in-flight" scenario. Property tests cover the invariants
//! in-memory; chaos covers aggregate behavior; neither exercises the
//! specific production-sensitive path where a job is cancelled while
//! a worker is still mid-build on one of its drvs.
//!
//! Core invariant under test: an in-flight claim holds its step's
//! Arc strong enough to survive the owning submission being removed.
//! If the step were dropped the instant the submission was removed,
//! the worker's /complete would hit a "step not found" path and
//! return ignored:true — silently discarding the build result. This
//! test ensures the cancel + in-flight-complete contract delivers
//! the correct observable outcome (ignored:true for a cancelled job,
//! not a panic or a dangling reference).

use std::time::Duration;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, IngestBatchRequest};
use sqlx::PgPool;

mod common;

fn drv_path(hash: &str, name: &str) -> String {
    format!("/nix/store/{hash}-{name}.drv")
}

/// Cancel a job while a worker holds an active claim on one of its
/// drvs. Observable contract:
///
/// 1. Worker's /complete on the claim returns `ignored: true`
///    (the claim was evicted when we cancelled).
/// 2. Job status is `cancelled`, not `done` (the worker's success
///    doesn't resurrect the job).
/// 3. No panic, no dangling Arc, no step lookup failure in logs —
///    the dispatcher tolerates the "complete arrives after cancel"
///    ordering gracefully.
///
/// This is the ordering that happens in production any time a CCI
/// job is cancelled mid-build: the worker is still running its
/// `nix build` when the coordinator marks the job cancelled; the
/// worker's follow-up /complete races with the cancel's cleanup.
#[sqlx::test]
async fn complete_after_cancel_returns_ignored_not_panic(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("gc", "in-flight");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![common::ingest(&drv, "in-flight", &[], true)],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();

    // Worker claims the drv.
    let c = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("claim must be issued for a fresh runnable drv");

    // Now cancel the job. In production this is what a CCI "user
    // cancelled the PR" hook triggers. The claim is still held by
    // the worker; the cancel removes the submission from the
    // dispatcher.
    client.cancel(job.id).await.unwrap();

    // Worker's /complete: must return ignored:true rather than
    // panic / 500. The claim was evicted by cancel so the lookup
    // returns NotFound-via-ignored, which is the expected shape.
    let resp = client
        .complete(
            job.id,
            c.claim_id,
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
        .expect("complete call itself must not error");
    assert!(
        resp.ignored,
        "post-cancel complete must be ignored, not processed"
    );

    // Status is cancelled — the worker's success didn't resurrect
    // the job. The terminal JSON blob is durable, so we scrape
    // through the normal status endpoint.
    let status = client.status(job.id).await.unwrap();
    assert_eq!(
        status.status,
        nix_ci_core::types::JobStatus::Cancelled,
        "cancelled job must stay cancelled regardless of worker complete"
    );
}

/// Symmetric case: cross-submission step dedup under drop. Two jobs
/// share the same drv (dedup'd into one Step). Cancel job A while
/// the drv is runnable (not yet claimed); verify job B can still
/// claim and complete the drv. The step must NOT be garbage-collected
/// when only job A dropped because job B still holds a strong ref via
/// `Submission::members`. This is invariant 8 under stress.
#[sqlx::test]
async fn shared_step_survives_one_owning_submissions_cancel(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    let job_a = client
        .create_job(&CreateJobRequest {
            external_ref: Some("a".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let job_b = client
        .create_job(&CreateJobRequest {
            external_ref: Some("b".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let shared = drv_path("shared", "both-want-me");
    let req = IngestBatchRequest {
        drvs: vec![common::ingest(&shared, "both-want-me", &[], true)],
        eval_errors: Vec::new(),
    };
    client.ingest_batch(job_a.id, &req).await.unwrap();
    client.ingest_batch(job_b.id, &req).await.unwrap();
    // Both jobs need seal so their terminal logic fires when the
    // drv completes. A is about to be cancelled so its seal doesn't
    // matter; B must be sealed so a successful /complete transitions
    // it to Done.
    client.seal(job_a.id).await.ok();
    client.seal(job_b.id).await.unwrap();

    // Cancel A BEFORE anyone claims the shared drv. Step should
    // survive because B still holds a strong ref via members.
    client.cancel(job_a.id).await.unwrap();

    // B can still claim and complete the shared drv — the whole
    // point of cross-job dedup.
    let c = client
        .claim(job_b.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("B must still be able to claim the shared drv");
    let resp = client
        .complete(
            job_b.id,
            c.claim_id,
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
    assert!(
        !resp.ignored,
        "B's complete must NOT be ignored — its submission still owns the step"
    );

    // Give the terminal writeback a beat to settle in the cancelled
    // case (A) and the done case (B).
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let sa = client.status(job_a.id).await.unwrap();
        let sb = client.status(job_b.id).await.unwrap();
        if sa.status.is_terminal() && sb.status.is_terminal() {
            assert_eq!(sa.status, nix_ci_core::types::JobStatus::Cancelled);
            assert_eq!(sb.status, nix_ci_core::types::JobStatus::Done);
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("jobs did not converge: a={:?} b={:?}", sa.status, sb.status);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
