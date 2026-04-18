//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: Prometheus /metrics accuracy and admin_snapshot gauges.

mod common;

use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, IngestBatchRequest, IngestDrvRequest};
use sqlx::PgPool;

#[sqlx::test]
async fn metrics_counts_ingested_and_completed(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("met", "measure");
    client
        .ingest_drv(job.id, &ingest(&drv, "measure", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 25,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();

    let body = reqwest::get(format!("{}/metrics", handle.base_url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        body.contains("nix_ci_jobs_created_total 1") || body.contains("nix_ci_jobs_created 1"),
        "jobs_created counter missing:\n{body}"
    );
    assert!(
        body.contains("nix_ci_drvs_ingested_total 1") || body.contains("nix_ci_drvs_ingested 1"),
        "drvs_ingested counter missing"
    );
    // builds_completed with outcome=success should show up at least once.
    assert!(
        body.contains(r#"nix_ci_builds_completed_total{outcome="success"}"#)
            || body.contains(r#"nix_ci_builds_completed{outcome="success"}"#),
        "builds_completed success label missing"
    );

    // build_duration histogram sum: we reported duration_ms=25 which
    // the handler divides by 1000 to record seconds (0.025). Assert the
    // histogram sum stays in seconds-scale, NOT milliseconds. A
    // mutation swapping `/` for `*` would multiply 25 × 1000 = 25000
    // seconds, blowing the sum out of range.
    let sum_line = body
        .lines()
        .find(|l| l.starts_with("nix_ci_build_duration_seconds_sum"))
        .unwrap_or_else(|| panic!("build_duration sum missing:\n{body}"));
    let sum: f64 = sum_line
        .split_whitespace()
        .next_back()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("failed to parse sum line: {sum_line}"));
    assert!(
        (0.0..1.0).contains(&sum),
        "build_duration sum should be in seconds (expected ~0.025), got {sum} from line {sum_line}"
    );
}

#[sqlx::test]
async fn admin_snapshot_reflects_live_state(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let a = drv_path("aaa", "a");
    let b = drv_path("bbb", "b");
    client
        .ingest_drv(job.id, &ingest(&a, "a", &[], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&b, "b", &[&a], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let snap: nix_ci_core::types::AdminSnapshot =
        reqwest::get(format!("{}/admin/snapshot", handle.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(snap.submissions, 1);
    assert_eq!(snap.steps_total, 2);
    assert_eq!(snap.steps_pending, 2);
    assert_eq!(snap.steps_building, 0);
    assert_eq!(snap.steps_done, 0);

    // Start a build so one step is building.
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(c.drv_path, a);
    let snap: nix_ci_core::types::AdminSnapshot =
        reqwest::get(format!("{}/admin/snapshot", handle.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(snap.active_claims, 1);
}

#[sqlx::test]
async fn metrics_expose_dispatcher_snapshot_gauges(pool: PgPool) {
    // `/metrics` refreshes submissions_active and steps_registry_size
    // on every scrape so Prometheus can graph dispatcher memory
    // pressure without hitting /admin/snapshot.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    for i in 0..3 {
        client
            .ingest_drv(
                job.id,
                &ingest(
                    &drv_path(&format!("gauge{i}"), &format!("g{i}")),
                    &format!("g{i}"),
                    &[],
                    true,
                ),
            )
            .await
            .unwrap();
    }

    let body = reqwest::get(format!("{}/metrics", handle.base_url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    // One submission, three steps.
    assert!(
        body.contains("nix_ci_submissions_active 1"),
        "submissions_active not 1: {body}"
    );
    assert!(
        body.contains("nix_ci_steps_registry_size 3"),
        "steps_registry_size not 3: {body}"
    );
}

#[sqlx::test]
async fn admin_snapshot_counts_scale_linearly_with_members(pool: PgPool) {
    // Guards against arithmetic mutations in `Submission::live_counts`
    // that would leave totals stuck regardless of membership size.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drvs: Vec<IngestDrvRequest> = (0..5)
        .map(|i| {
            ingest(
                &drv_path(&format!("lin{i}"), &format!("n{i}")),
                &format!("n{i}"),
                &[],
                true,
            )
        })
        .collect();
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

    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.counts.total, 5);
    assert_eq!(status.counts.pending, 5);

    // Complete two successfully, check counts update.
    for _ in 0..2 {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 3)
            .await
            .unwrap()
            .expect("claim");
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
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.counts.total, 5);
    assert_eq!(status.counts.done, 2);
    assert_eq!(status.counts.pending, 3);
}
