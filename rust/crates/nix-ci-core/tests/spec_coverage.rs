//! Close the gap between SPEC.md exit bars and concrete regression
//! tests. Each `#[sqlx::test]` here names the SPEC bar it covers in
//! its first comment line — so `spec_report.sh` (H14) can parse the
//! file and prove "this bar has a test."
//!
//! All metric assertions go through the `/metrics` HTTP endpoint
//! (via `common::scrape_metric`) — not direct reads of
//! `dispatcher.metrics.inner.*.get()`. The audit flagged that as a
//! COMPLIANCE anti-pattern: it tests the in-memory shape, not the
//! observable Prometheus contract that alerts / dashboards actually
//! consume. Regressions in how metrics are EXPORTED (label order,
//! name suffixing, unregistered counter) would slip by an internal
//! read and only manifest in production when a dashboard goes blank.
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
/// counter exactly once, as visible on the `/metrics` endpoint.
/// Three transitions here (done, failed, cancelled) must produce
/// three increments, one per status — scraped through the same
/// surface a Prometheus server would see.
#[sqlx::test]
async fn jobs_terminal_counter_parity(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());

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

    // Scrape /metrics — the source of truth for observability.
    // prometheus-client appends `_total` to Counter metric names on
    // the wire (OpenMetrics requirement); the registered name is
    // `nix_ci_jobs_terminal`.
    let name = "nix_ci_jobs_terminal_total";
    assert_eq!(
        common::scrape_metric(&handle.base_url, name, &[("status", "done")]).await,
        Some(1.0),
        "exactly one done transition must appear on /metrics"
    );
    assert_eq!(
        common::scrape_metric(&handle.base_url, name, &[("status", "failed")]).await,
        Some(1.0),
        "exactly one failed transition on /metrics"
    );
    assert_eq!(
        common::scrape_metric(&handle.base_url, name, &[("status", "cancelled")]).await,
        Some(1.0),
        "exactly one cancelled transition on /metrics"
    );
}

/// Idempotent cancel must not double-count. A second cancel on an
/// already-cancelled job is a no-op — the counter stays at 1, not 2.
/// Scraped via /metrics for the same reason as above.
#[sqlx::test]
async fn idempotent_cancel_does_not_double_count_terminal(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let name = "nix_ci_jobs_terminal_total";
    let cancel_labels = &[("status", "cancelled")];

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    client.cancel(job.id).await.unwrap();
    let after_first =
        common::scrape_metric(&handle.base_url, name, cancel_labels)
            .await
            .expect("cancelled counter must be present after first cancel");
    // Second cancel on a job that's already terminal must not emit
    // another terminal event.
    client.cancel(job.id).await.unwrap();
    let after_second = common::scrape_metric_expect(&handle.base_url, name, cancel_labels).await;
    assert_eq!(
        after_first, after_second,
        "cancel on already-terminal job must not double-count on /metrics"
    );
}

/// O-CLAIMS-INFLIGHT: after all activity ends, the claims_in_flight
/// gauge (as visible on /metrics) converges to 0. If the convergence
/// is slow (handler forgot to decrement) or the gauge drifts, this
/// catches it within the 5s SPEC budget.
#[sqlx::test]
async fn claims_in_flight_converges_to_zero_after_quiesce(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let gauge_name = "nix_ci_claims_in_flight";

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

    // Claim all ten; gauge on /metrics must be 10.
    let mut claims = Vec::new();
    for _ in 0..10 {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 5)
            .await
            .unwrap()
            .expect("claim");
        claims.push(c);
    }
    let mid_flight = common::scrape_metric_expect(&handle.base_url, gauge_name, &[]).await;
    assert_eq!(
        mid_flight, 10.0,
        "all 10 outstanding claims must register on /metrics"
    );

    // Complete all; the gauge must converge to 0.
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
        let g = common::scrape_metric_expect(&handle.base_url, gauge_name, &[]).await;
        if g == 0.0 {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "O-CLAIMS-INFLIGHT: /metrics gauge stuck at {g} after 5s quiesce; \
                 a decrement path is broken"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
