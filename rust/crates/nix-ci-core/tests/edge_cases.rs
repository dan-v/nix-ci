//! Edge-case and failure-mode HTTP tests. Complements `http_e2e.rs`
//! with scenarios that stress validation, admin actions, reaper
//! behavior, retry budgets, and observability.

mod common;

use std::time::Duration;

use common::{drv_path, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, ErrorCategory, IngestBatchRequest, IngestDrvRequest,
    JobStatus,
};
use sqlx::PgPool;

fn ingest(drv: &str, name: &str, deps: &[&str], is_root: bool) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: deps.iter().map(|s| s.to_string()).collect(),
        is_root,
        cache_status: None,
    }
}

// ─── Input validation ────────────────────────────────────────────────

#[sqlx::test]
async fn ingest_rejects_empty_drv_path(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let bad = IngestDrvRequest {
        drv_path: "".into(),
        drv_name: "x".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        cache_status: None,
    };
    let err = client.ingest_drv(job.id, &bad).await.unwrap_err();
    match err {
        nix_ci_core::Error::BadRequest(_) => {}
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

#[sqlx::test]
async fn ingest_rejects_malformed_drv_path(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    // No trailing .drv, no hyphen — drv_hash_from_path should reject.
    let bad = IngestDrvRequest {
        drv_path: "/nix/store/nohyphen".into(),
        drv_name: "x".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        cache_status: None,
    };
    let err = client.ingest_drv(job.id, &bad).await.unwrap_err();
    match err {
        nix_ci_core::Error::BadRequest(_) => {}
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

#[sqlx::test]
async fn ingest_batch_partial_validation_counts_errors(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let good = drv_path("good", "pkg");
    let bad_empty = IngestDrvRequest {
        drv_path: "".into(),
        drv_name: "x".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: false,
        cache_status: None,
    };
    let bad_nohyphen = IngestDrvRequest {
        drv_path: "/nix/store/nohyphen.drv".into(),
        drv_name: "x".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: false,
        cache_status: None,
    };
    let batch = IngestBatchRequest {
        drvs: vec![ingest(&good, "pkg", &[], true), bad_empty, bad_nohyphen],
    };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.new_drvs, 1);
    assert_eq!(resp.errored, 2);
}

// ─── Terminal-state guards ───────────────────────────────────────────

#[sqlx::test]
async fn ingest_after_cancel_returns_gone(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    // Cancel the job
    let url = format!("{}/jobs/{}/cancel", handle.base_url, job.id);
    reqwest::Client::new()
        .delete(&url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let drv = drv_path("aaa", "x");
    let err = client
        .ingest_drv(job.id, &ingest(&drv, "x", &[], true))
        .await
        .unwrap_err();
    match err {
        nix_ci_core::Error::Gone(_) => {}
        other => panic!("expected Gone after cancel, got {other:?}"),
    }
}

#[sqlx::test]
async fn heartbeat_after_cancel_returns_gone(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    reqwest::Client::new()
        .delete(format!("{}/jobs/{}/cancel", handle.base_url, job.id))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let err = client.heartbeat(job.id).await.unwrap_err();
    match err {
        nix_ci_core::Error::Gone(_) => {}
        other => panic!("expected Gone heartbeat after cancel, got {other:?}"),
    }
}

// ─── Cross-submission feature matching ───────────────────────────────

#[sqlx::test]
async fn worker_without_required_feature_never_claims(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("kvm", "vm-test");
    let req = IngestDrvRequest {
        drv_path: drv,
        drv_name: "vm-test".into(),
        system: "x86_64-linux".into(),
        required_features: vec!["kvm".into()],
        input_drvs: vec![],
        is_root: true,
        cache_status: None,
    };
    client.ingest_drv(job.id, &req).await.unwrap();
    client.seal(job.id).await.unwrap();

    // Worker without kvm: gets 204
    let none = client.claim(job.id, "x86_64-linux", &[], 1).await.unwrap();
    assert!(none.is_none(), "worker without kvm must not claim");

    // Worker with kvm: gets the drv
    let some = client
        .claim(job.id, "x86_64-linux", &["kvm".into()], 2)
        .await
        .unwrap();
    assert!(some.is_some(), "worker with kvm must claim");
}

#[sqlx::test]
async fn worker_with_wrong_system_never_claims(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("linuxonly", "kernel");
    client
        .ingest_drv(
            job.id,
            &IngestDrvRequest {
                drv_path: drv,
                drv_name: "kernel".into(),
                system: "x86_64-linux".into(),
                required_features: vec![],
                input_drvs: vec![],
                is_root: true,
                cache_status: None,
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Worker on darwin: nothing to claim
    let none = client
        .claim(job.id, "aarch64-darwin", &[], 1)
        .await
        .unwrap();
    assert!(none.is_none(), "darwin worker must not claim linux drv");
}

// ─── Retry budget exhaustion ─────────────────────────────────────────

#[sqlx::test]
async fn retryable_exhaustion_becomes_terminal(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
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
    // backoff timer between attempts.
    for expected_attempt in 1..=3 {
        let c = match client.claim(job.id, "x86_64-linux", &[], 5).await {
            Ok(Some(c)) => c,
            Ok(None) => break,
            // After terminal failure the submission is removed from
            // the dispatcher map and claim returns 410 Gone.
            Err(nix_ci_core::Error::Gone(_)) => break,
            Err(e) => panic!("unexpected claim error: {e}"),
        };
        assert_eq!(c.attempt, expected_attempt);
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

// ─── Admin cancel propagates to in-flight claim ──────────────────────

#[sqlx::test]
async fn cancel_while_claim_outstanding(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("can", "cancelme");
    client
        .ingest_drv(job.id, &ingest(&drv, "cancelme", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");

    // Cancel the job while the claim is outstanding.
    reqwest::Client::new()
        .delete(format!("{}/jobs/{}/cancel", handle.base_url, job.id))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Worker finishes build successfully and tries to complete.
    // The submission was removed, so the step lookup fails →
    // ignored=true. No PG writes for this completion.
    let resp = client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 5,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();
    assert!(
        resp.ignored,
        "complete after cancel must be ignored (submission gone)"
    );

    // Job status reflects cancellation.
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Cancelled);
}

// ─── SSE terminal event is visible ───────────────────────────────────

#[sqlx::test]
async fn sse_job_done_event_surfaces_for_success(pool: PgPool) {
    use eventsource_stream::Eventsource;
    use futures::StreamExt;

    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("sse", "single");
    client
        .ingest_drv(job.id, &ingest(&drv, "single", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let events_url = format!("{}/jobs/{}/events", handle.base_url, job.id);
    let sse_task = tokio::spawn(async move {
        let resp = reqwest::Client::new()
            .get(&events_url)
            .header("accept", "text/event-stream")
            .send()
            .await
            .unwrap();
        let mut stream = resp.bytes_stream().eventsource();
        while let Some(ev) = stream.next().await {
            let ev = ev.unwrap();
            if ev.event == "job_done" {
                return ev.data;
            }
        }
        panic!("SSE closed without job_done");
    });

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
                duration_ms: 5,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();

    let payload = tokio::time::timeout(Duration::from_secs(5), sse_task)
        .await
        .expect("SSE task deadline")
        .expect("SSE task joined");
    assert!(
        payload.contains("\"status\":\"done\""),
        "job_done SSE payload missing status: {payload}"
    );
}

// ─── Metrics counters reflect real activity ──────────────────────────

#[sqlx::test]
async fn metrics_counts_ingested_and_completed(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
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
}

// ─── Admin snapshot accuracy ─────────────────────────────────────────

#[sqlx::test]
async fn admin_snapshot_reflects_live_state(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
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

// ─── Submission cleanup on terminal ──────────────────────────────────

#[sqlx::test]
async fn terminal_jobs_removed_from_in_memory_map(pool: PgPool) {
    // Regression guard: check_and_publish_terminal must drop the
    // submission from dispatcher.submissions. Before this fix,
    // successfully-completed jobs stayed in memory indefinitely,
    // leaking one Arc<Submission> + its member graph per job.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Drive 5 jobs to successful completion, then check the map size.
    for i in 0..5 {
        let job = client
            .create_job(&CreateJobRequest {
                external_ref: Some(format!("terminal-cleanup-{i}")),
            })
            .await
            .unwrap();
        let drv = drv_path(&format!("t{i:02}"), "single");
        client
            .ingest_drv(job.id, &ingest(&drv, "single", &[], true))
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
                    duration_ms: 1,
                    exit_code: Some(0),
                    error_category: None,
                    error_message: None,
                    log_tail: None,
                },
            )
            .await
            .unwrap();
        for _ in 0..20 {
            if client.status(job.id).await.unwrap().status.is_terminal() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // Give the async terminal-publish a moment to finish the removal.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let snap: nix_ci_core::types::AdminSnapshot =
        reqwest::get(format!("{}/admin/snapshot", handle.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(
        snap.submissions, 0,
        "all 5 terminal submissions must be removed from the map"
    );
}

// ─── Heartbeat reset behavior ────────────────────────────────────────

#[sqlx::test]
async fn heartbeat_keeps_live_job_fresh(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let resp = client.heartbeat(job.id).await.unwrap();
    assert!(resp.ok);
}
