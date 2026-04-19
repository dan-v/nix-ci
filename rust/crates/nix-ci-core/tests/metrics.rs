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

/// Every terminal-writing handler (/fail, /cancel, auto_fail_on_seal,
/// auto_fail_oversized, /complete's last-drv path) must route its PG
/// UPDATE through `timed_acquire` so `pg_pool_acquire_duration_seconds`
/// reflects real pool pressure on those paths. Before this fix, only
/// the /complete path observed the histogram — pool starvation on
/// /fail or /cancel was silent to the operator dashboard that alerts
/// on rising pool-acquire p99.
///
/// We exercise /fail as the simplest of the fixed paths and assert the
/// histogram's `_count` grew by at least one. The `scrape_metric`
/// helper accepts the full OpenMetrics `_count` suffix directly.
#[sqlx::test]
async fn fail_observes_pg_pool_acquire_duration_histogram(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let before = common::scrape_metric(
        &handle.base_url,
        "nix_ci_pg_pool_acquire_duration_seconds_count",
        &[],
    )
    .await
    .unwrap_or(0.0);

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("{}/jobs/{}/fail", handle.base_url, job.id))
        .json(&serde_json::json!({"message": "observed"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "fail endpoint must succeed");

    let after = common::scrape_metric_expect(
        &handle.base_url,
        "nix_ci_pg_pool_acquire_duration_seconds_count",
        &[],
    )
    .await;
    assert!(
        after >= before + 1.0,
        "expected at least one new pg_pool_acquire observation on /fail; \
         before={before}, after={after}"
    );
}

/// /metrics must stay coherent under concurrent scrape + concurrent
/// traffic. The endpoint takes a `parking_lot::Mutex<Registry>` for
/// encoding; concurrent scrapes serialize on that lock, and
/// concurrent writes (to counters/gauges) go via independent atomics.
/// The test guards against two failure modes:
///   * Torn reads: a scrape returning partial / invalid OpenMetrics
///     text because another scrape was mid-encode. Impossible with
///     Mutex serialization, but we validate the surface.
///   * Scrape-vs-mutate deadlock: a scrape holds the registry lock
///     while the traffic path tries to register a new metric family.
///     All our families are registered at startup — not at runtime —
///     so this is structurally impossible, but the test exercises
///     the shape to lock it in as a contract.
///
/// Shape: 50 concurrent scrapes interleaved with 50 concurrent
/// job-create calls. Every scrape response must parse as valid
/// Prometheus text and contain at least one expected family.
#[sqlx::test]
async fn metrics_scrape_survives_concurrent_scrapes_and_traffic(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let base = handle.base_url.clone();
    let client = CoordinatorClient::new(&base);

    // Warm up: one job so there's some per-job histogram data.
    let _ = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let mut scrape_tasks = tokio::task::JoinSet::new();
    let mut traffic_tasks = tokio::task::JoinSet::new();

    // 50 concurrent scrapes. reqwest::Client is cheap to reuse but
    // cloning via base_url also works; prefer the simple shape.
    for i in 0..50 {
        let url = format!("{base}/metrics");
        scrape_tasks.spawn(async move {
            let resp = reqwest::get(&url).await.unwrap_or_else(|e| {
                panic!("scrape {i} failed: {e}");
            });
            let status = resp.status();
            let body = resp.text().await.unwrap();
            (i, status, body)
        });
    }

    // 50 concurrent job creates — exercises jobs_created counter +
    // pool_acquire histogram while the scrapes are hitting /metrics.
    for i in 0..50 {
        let c = CoordinatorClient::new(&base);
        traffic_tasks.spawn(async move {
            c.create_job(&CreateJobRequest {
                external_ref: Some(format!("metrics-race-{i}")),
                ..Default::default()
            })
            .await
        });
    }

    // Drain scrapes.
    while let Some(res) = scrape_tasks.join_next().await {
        let (idx, status, body) = res.expect("scrape task must not panic");
        assert_eq!(status.as_u16(), 200, "scrape {idx} status != 200");

        // Structural: OpenMetrics format has `# HELP`, `# TYPE`, and
        // a numeric-value-per-line shape. We assert the hallmarks
        // are there — a truncated / corrupt body would be missing.
        assert!(
            body.contains("# HELP"),
            "scrape {idx} body missing # HELP header; first 200B: {:?}",
            &body[..body.len().min(200)]
        );
        assert!(
            body.contains("# TYPE"),
            "scrape {idx} body missing # TYPE header"
        );
        // A handful of metric families we know must be registered.
        for expected in [
            "nix_ci_jobs_created",
            "nix_ci_claims_in_flight",
            "nix_ci_submissions_active",
        ] {
            assert!(
                body.contains(expected),
                "scrape {idx} body missing expected family {expected}"
            );
        }
        // OpenMetrics ends every scrape with `# EOF\n` (prometheus-client
        // convention). A torn encode would break this.
        assert!(
            body.trim_end().ends_with("# EOF"),
            "scrape {idx} body missing # EOF terminator"
        );
    }

    // Drain traffic — all 50 creates must succeed (no 5xx from
    // scrape contention).
    let mut created = 0;
    while let Some(res) = traffic_tasks.join_next().await {
        let r = res.expect("create-job task must not panic");
        r.expect("create_job must succeed under scrape contention");
        created += 1;
    }
    assert_eq!(created, 50);
}

/// Same property for /cancel.
#[sqlx::test]
async fn cancel_observes_pg_pool_acquire_duration_histogram(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let before = common::scrape_metric(
        &handle.base_url,
        "nix_ci_pg_pool_acquire_duration_seconds_count",
        &[],
    )
    .await
    .unwrap_or(0.0);

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    client.cancel(job.id).await.unwrap();

    let after = common::scrape_metric_expect(
        &handle.base_url,
        "nix_ci_pg_pool_acquire_duration_seconds_count",
        &[],
    )
    .await;
    assert!(
        after >= before + 1.0,
        "expected at least one new pg_pool_acquire observation on /cancel; \
         before={before}, after={after}"
    );
}
