//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: seal validation, cycle detection, self-loop stripping.

mod common;

use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CreateJobRequest, IngestBatchRequest, JobStatus};
use sqlx::PgPool;

#[sqlx::test]
async fn ingest_on_sealed_job_is_rejected(pool: PgPool) {
    // Once a job is sealed the caller has told us "no more drvs" —
    // a late ingest after seal must 410 so a lost-retry client
    // doesn't silently re-open a terminating submission.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("sealed", "pkg");
    client
        .ingest_drv(job.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let late = drv_path("after", "late");
    match client
        .ingest_drv(job.id, &ingest(&late, "late", &[], false))
        .await
    {
        Err(nix_ci_core::Error::Gone(_)) => {}
        other => panic!("expected 410 on sealed-ingest, got {other:?}"),
    }
}

#[sqlx::test]
async fn seal_job_returns_false_for_missing_job(pool: PgPool) {
    // `seal_job` returns true when a row was updated, false when the
    // job doesn't exist. The seal handler uses this to distinguish a
    // real seal from a seal-of-unknown-id (which 404s). A mutant that
    // flips the `>=` boundary would let either case through.
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let job_id = nix_ci_core::types::JobId::new();
    nix_ci_core::durable::writeback::upsert_job(&pool, job_id, None)
        .await
        .unwrap();
    let present = nix_ci_core::durable::writeback::seal_job(&pool, job_id)
        .await
        .unwrap();
    assert!(present, "seal of present job returns true");
    let missing_id = nix_ci_core::types::JobId::new();
    let absent = nix_ci_core::durable::writeback::seal_job(&pool, missing_id)
        .await
        .unwrap();
    assert!(!absent, "seal of absent job returns false");
}

#[sqlx::test]
async fn seal_with_no_toplevels_transitions_to_done(pool: PgPool) {
    // A sealed submission whose toplevels set is empty has "all
    // toplevels finished" vacuously true, so it must terminate as Done
    // immediately. Without the explicit check in `seal`, the client
    // waits for a completion that will never come.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // No ingest — straight to seal.
    let sealed = client.seal(job.id).await.unwrap();
    assert_eq!(
        sealed.status,
        JobStatus::Done,
        "empty sealed submission must report Done immediately"
    );

    // And the terminal snapshot is durably persisted.
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Done);
    assert!(status.sealed);
    assert_eq!(status.counts.total, 0);
    assert!(status.failures.is_empty());
}

/// A cyclic dep graph (a → b → a) must fail the job at seal time with a
/// clean `dep_cycle` cause. Without this, both steps sit forever with
/// runnable=false — workers never claim them, the job stalls, eventually
/// the heartbeat reaper cancels it with an uninformative sentinel.
#[sqlx::test]
async fn seal_rejects_dep_cycle(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let a = drv_path("cyclea", "a");
    let b = drv_path("cycleb", "b");
    // Ingest A depending on B, and B depending on A. Each ingest phase
    // creates the placeholder for the other, so we need both rows in
    // the same batch to wire the back-edge.
    let batch = IngestBatchRequest {
        drvs: vec![ingest(&a, "a", &[&b], true), ingest(&b, "b", &[&a], false)],
        eval_errors: Vec::new(),
    };
    client.ingest_batch(job.id, &batch).await.unwrap();

    // Seal must fail the job immediately (without stalling) with
    // eval_error naming dep_cycle.
    let seal_resp = client.seal(job.id).await.unwrap();
    assert_eq!(seal_resp.status, JobStatus::Failed);

    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Failed);
    assert!(
        status
            .eval_error
            .as_deref()
            .unwrap_or("")
            .contains("dep_cycle"),
        "expected dep_cycle cause, got: {:?}",
        status.eval_error
    );
}

/// Self-loop (a drv that lists itself as an input) must be stripped at
/// ingest time — the edge is rejected, the drv itself still enters the
/// graph with no deps and is thus immediately runnable. The batch
/// response reports `errored=1` for the rejected edge so the submitter
/// can log it. Without this guard, `attach_dep(parent, parent)` would
/// stick the step's own handle into its own deps set, wedging it
/// forever as unrunnable.
#[sqlx::test]
async fn ingest_strips_self_loop(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let a = drv_path("selfloopX", "a");
    let resp = client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![ingest(&a, "a", &[&a], true)],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    // The drv itself was accepted (it's still a real drv that can be
    // built); only the self-loop edge was rejected.
    assert_eq!(resp.new_drvs, 1);
    assert_eq!(resp.errored, 1, "self-loop edge must count as an error");

    // And the step is runnable (a worker can claim it) — proving the
    // bad edge didn't wedge it.
    client.seal(job.id).await.unwrap();
    let claim = client.claim(job.id, "x86_64-linux", &[], 2).await.unwrap();
    assert!(claim.is_some(), "self-loop-stripped drv must be claimable");
}
