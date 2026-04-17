//! Close the gap between SPEC.md exit bars and concrete regression
//! tests. Each `#[sqlx::test]` here names the SPEC bar it covers in
//! its first comment line — so `spec_report.sh` (H14) can parse the
//! file and prove "this bar has a test."
//!
//! Scope: the bars the v2-era audit found uncovered.
//!
//! - C-CORRECT-3: no drv silently dropped from a sealed, non-terminal
//!   job — total always = pending + building + done + failed.
//! - O-METRIC-PARITY: for every terminal transition, exactly one
//!   jobs_terminal{status=x} counter increment.
//! - O-CLAIMS-INFLIGHT: claims_in_flight gauge converges to 0 within
//!   5s of quiescence.

use std::time::Duration;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::observability::metrics::TerminalLabels;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, IngestBatchRequest, IngestDrvRequest, JobStatus,
};
use sqlx::PgPool;

mod common;

fn drv_path(hash: &str, name: &str) -> String {
    format!("/nix/store/{hash}-{name}.drv")
}

fn ingest(drv: &str, name: &str, deps: &[&str], is_root: bool) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: deps.iter().map(|s| s.to_string()).collect(),
        is_root,
        attr: None,
    }
}

/// C-CORRECT-3: every drv ingested into a sealed submission must end
/// up in exactly one of the count buckets — pending / building /
/// done / failed — from seal through to terminal. If a drv were
/// silently dropped (e.g. a bad rdep prune, a broken seal check) the
/// sum of the buckets would drift below the total and this would
/// catch it.
#[sqlx::test]
async fn drvs_accounted_after_seal(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // 6-drv diamond + chain. Roots: top and sibling.
    let leaf = drv_path("l", "leaf");
    let mid1 = drv_path("m1", "mid1");
    let mid2 = drv_path("m2", "mid2");
    let top = drv_path("t", "top");
    let sib = drv_path("s", "sibling");
    let chain = drv_path("c", "chain"); // depends on sibling
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![
                    ingest(&leaf, "leaf", &[], false),
                    ingest(&mid1, "mid1", &[&leaf], false),
                    ingest(&mid2, "mid2", &[&leaf], false),
                    ingest(&top, "top", &[&mid1, &mid2], true),
                    ingest(&sib, "sibling", &[], false),
                    ingest(&chain, "chain", &[&sib], true),
                ],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // At every observable point — before any completion, during
    // completion, after terminal — total must equal the sum of the
    // buckets. Single drops would show up as total > sum.
    let assert_accounted = |status: &nix_ci_core::types::JobStatusResponse,
                            where_: &str| {
        let c = &status.counts;
        let sum = c.pending + c.building + c.done + c.failed;
        assert_eq!(
            c.total, sum,
            "[{where_}] total={} but pending+building+done+failed={}+{}+{}+{}={}; \
             a drv was dropped somewhere",
            c.total, c.pending, c.building, c.done, c.failed, sum
        );
    };

    // Pre-completion snapshot.
    let s0 = client.status(job.id).await.unwrap();
    assert_eq!(s0.counts.total, 6);
    assert_accounted(&s0, "post-seal");

    // Complete drvs one at a time; after each, verify accounting.
    // A 410 Gone means the job became terminal mid-loop (last
    // completion crossed the threshold), which is the natural exit.
    let mut completed: u32 = 0;
    for _ in 0..10 {
        let c = match client.claim(job.id, "x86_64-linux", &[], 5).await {
            Ok(Some(c)) => c,
            Ok(None) => break,
            Err(nix_ci_core::Error::Gone(_)) => break,
            Err(e) => panic!("unexpected claim error: {e}"),
        };
        client
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
            .unwrap();
        completed += 1;
        let s = client.status(job.id).await.unwrap();
        assert_accounted(&s, &format!("after {completed} completions"));
    }
    assert_eq!(completed, 6);

    // Terminal snapshot: all 6 accounted in `done`.
    let final_s = client.status(job.id).await.unwrap();
    assert_eq!(final_s.status, JobStatus::Done);
    assert_eq!(final_s.counts.done, 6);
    assert_eq!(final_s.counts.total, 6);
    assert_accounted(&final_s, "terminal");
}

/// O-METRIC-PARITY: every terminal transition bumps the labelled
/// counter exactly once. Three transitions in this test (done,
/// failed, cancelled) produce three increments, one per status.
#[sqlx::test]
async fn jobs_terminal_counter_parity(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    // Baseline counters.
    let count = |status: &str| {
        handle
            .dispatcher
            .metrics
            .inner
            .jobs_terminal
            .get_or_create(&TerminalLabels {
                status: status.to_string(),
            })
            .get()
    };
    assert_eq!(count("done"), 0);
    assert_eq!(count("failed"), 0);
    assert_eq!(count("cancelled"), 0);

    // 1. Done: trivial sealed job with no toplevels -> Done immediately.
    let done_job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    client.seal(done_job.id).await.unwrap();

    // 2. Failed: ingest one drv, fail it via /fail.
    let fail_job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    client
        .ingest_batch(
            fail_job.id,
            &IngestBatchRequest {
                drvs: vec![ingest(&drv_path("f", "f"), "f", &[], true)],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    // Use the /fail endpoint for a direct terminal transition.
    let body = reqwest::Client::new()
        .post(format!("{}/jobs/{}/fail", handle.base_url, fail_job.id))
        .json(&serde_json::json!({"message": "forced"}))
        .send()
        .await
        .unwrap();
    assert!(body.status().is_success());

    // 3. Cancelled: create + cancel.
    let cancel_job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    client.cancel(cancel_job.id).await.unwrap();

    assert_eq!(count("done"), 1, "exactly one done transition");
    assert_eq!(count("failed"), 1, "exactly one failed transition");
    assert_eq!(count("cancelled"), 1, "exactly one cancelled transition");
}

/// Idempotent cancel must not double-count. A second cancel on an
/// already-cancelled job is a no-op — the counter stays at 1, not 2.
#[sqlx::test]
async fn idempotent_cancel_does_not_double_count_terminal(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let count = || {
        handle
            .dispatcher
            .metrics
            .inner
            .jobs_terminal
            .get_or_create(&TerminalLabels {
                status: "cancelled".into(),
            })
            .get()
    };
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    client.cancel(job.id).await.unwrap();
    let after_first = count();
    // Second cancel on a job that's already terminal must not emit
    // another terminal event.
    client.cancel(job.id).await.unwrap();
    assert_eq!(
        count(),
        after_first,
        "cancel on already-terminal job must not double-count"
    );
}

/// O-CLAIMS-INFLIGHT: after all activity ends, the claims_in_flight
/// gauge converges to 0. If the convergence is slow (because a
/// handler forgot to decrement) or incorrect (gauge drift), this
/// catches it within the 5s SPEC budget.
#[sqlx::test]
async fn claims_in_flight_converges_to_zero_after_quiesce(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // 10 independent leaves so the claim flow exercises the
    // increment-decrement lifecycle under parallelism.
    let mut drvs = Vec::with_capacity(10);
    for i in 0..10 {
        let d = drv_path(&format!("q{i}"), &format!("q{i}"));
        drvs.push(ingest(&d, &format!("q{i}"), &[], true));
    }
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs,
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Mid-flight sanity: while we're claiming, the gauge should be
    // non-zero. Claim all ten to prove the increment side.
    let mut claims = Vec::new();
    for _ in 0..10 {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 5)
            .await
            .unwrap()
            .expect("claim");
        claims.push(c);
    }
    let mid_flight = handle.dispatcher.metrics.inner.claims_in_flight.get();
    assert_eq!(
        mid_flight, 10,
        "all 10 claims outstanding must register on the gauge"
    );

    // Complete all of them. Gauge should converge to 0 once the
    // last complete returns — we don't even need the 5s SPEC budget
    // in this test (the assertion is < 1s), the 5s bar is for chaos
    // scenarios with many submissions.
    for c in claims {
        client
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
            .unwrap();
    }
    // Bounded poll: must reach 0 well within SPEC's 5s budget.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let g = handle.dispatcher.metrics.inner.claims_in_flight.get();
        if g == 0 {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "O-CLAIMS-INFLIGHT: gauge stuck at {g} after 5s quiesce; \
                 a decrement path is broken"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
