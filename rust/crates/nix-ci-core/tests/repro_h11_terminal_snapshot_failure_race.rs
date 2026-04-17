//! Regression test for H11: concurrent `POST /complete` calls on two
//! toplevels X and Y could lose one of the failures from the
//! terminal snapshot (DB `jobs.result` + the `JobDone` broadcast
//! event).
//!
//! Before the fix, `handle_failure` ordered:
//!
//! ```text
//! step.previous_failure = true
//! step.finished = true            // visible to all readers (Release)
//! .await failed_outputs insert    // yields
//! record_failure(step) on subs    // happens AFTER finished=true is public
//! ```
//!
//! With two concurrent tasks on a single-threaded tokio runtime:
//!   - Task X: sets X.finished=true, yields at the DB await.
//!   - Task Y: sets Y.finished=true, yields at the DB await.
//!   - Task X resumes, records X's failure, reaches
//!     `check_and_publish_terminal`.
//!   - `all_finished` passes (both finished). Snapshot reads
//!     `sub.failures` → contains X but NOT Y (Y hasn't yet recorded
//!     — Y is still yielded). X persists and marks terminal.
//!   - Task Y resumes, records Y's failure, reaches
//!     `check_and_publish_terminal` — `mark_terminal` already true,
//!     bails. Y's failure is stranded in a detached in-memory Submission
//!     and never lands in `jobs.result` or `JobDone`.
//!
//! The fix reorders `handle_failure`: `record_failure` runs BEFORE
//! `step.finished.store(true, Release)`, so the release barrier
//! carries the recorded failure along. Any reader that observes
//! `finished=true` via `Acquire` also observes the failure in
//! `sub.failures`.

mod common;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, ErrorCategory, IngestBatchRequest, IngestDrvRequest,
    JobStatus,
};
use sqlx::PgPool;
use std::sync::Arc;

fn drv(hash: &str, name: &str) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: format!("/nix/store/{hash}-{name}.drv"),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        attr: None,
    }
}

#[sqlx::test]
async fn concurrent_top_failures_both_appear_in_terminal_snapshot(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    // Two toplevels, no deps. Both become immediately runnable.
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![drv("aaaaaaaa", "top-x"), drv("bbbbbbbb", "top-y")],
                eval_errors: vec![],
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Claim both sequentially so we own two distinct claim_ids.
    let claim_x = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("first claim");
    let claim_y = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("second claim");

    // Build two `/complete` requests with `BuildFailure` (non-retryable
    // so handle_failure goes down the terminal path).
    let mk_req = |msg: &str| CompleteRequest {
        success: false,
        duration_ms: 100,
        exit_code: Some(1),
        error_category: Some(ErrorCategory::BuildFailure),
        error_message: Some(msg.to_string()),
        log_tail: None,
    };

    let req_x = mk_req("x failed");
    let req_y = mk_req("y failed");
    // Fire both completes concurrently. On tokio's current_thread
    // runtime (sqlx::test default), the two tasks yield at their
    // `failed_outputs insert` .await points and interleave — the
    // exact scenario that triggers the race before the fix.
    let (cx, cy) = tokio::join!(
        client.complete(job.id, claim_x.claim_id, &req_x),
        client.complete(job.id, claim_y.claim_id, &req_y),
    );
    cx.expect("complete X must succeed");
    cy.expect("complete Y must succeed");

    // Force the DB-read path: even if the live submission is still
    // in memory, drop it to guarantee we see the persisted snapshot.
    handle.dispatcher.submissions.remove(job.id);

    let snap = client.status(job.id).await.expect("status must succeed");
    assert_eq!(snap.status, JobStatus::Failed);
    let drv_names: std::collections::HashSet<String> =
        snap.failures.iter().map(|f| f.drv_name.clone()).collect();
    assert!(
        drv_names.contains("top-x"),
        "X's failure must be in the terminal snapshot: {:?}",
        snap.failures
    );
    assert!(
        drv_names.contains("top-y"),
        "Y's failure must be in the terminal snapshot: {:?}",
        snap.failures
    );
}
