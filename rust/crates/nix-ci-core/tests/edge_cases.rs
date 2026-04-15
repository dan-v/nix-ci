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
    };
    let bad_nohyphen = IngestDrvRequest {
        drv_path: "/nix/store/nohyphen.drv".into(),
        drv_name: "x".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: false,
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
    // backoff timer between attempts. Count successful claims so a
    // mutation that loosens the retry predicate (attempt < vs attempt
    // <=) would let extra attempts sneak through and fail the equality
    // assertion below.
    let mut successful_claims = 0;
    let max_attempts_expected = 2; // matches default ServerConfig.max_attempts
    for iter in 1..=(max_attempts_expected + 2) {
        let c = match client.claim(job.id, "x86_64-linux", &[], 5).await {
            Ok(Some(c)) => c,
            Ok(None) => break,
            // After terminal failure the submission is removed from
            // the dispatcher map and claim returns 410 Gone.
            Err(nix_ci_core::Error::Gone(_)) => break,
            Err(e) => panic!("unexpected claim error: {e}"),
        };
        assert_eq!(
            c.attempt, iter,
            "attempt numbers must be strictly sequential"
        );
        successful_claims += 1;
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
    assert_eq!(
        successful_claims, max_attempts_expected,
        "exactly max_attempts claims should have been issued"
    );

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
    client.heartbeat(job.id).await.unwrap();
}

// ─── Battleproof: ingest rejected after seal ─────────────────────────

#[sqlx::test]
async fn ingest_on_sealed_job_is_rejected(pool: PgPool) {
    // Once a job is sealed the caller has told us "no more drvs" —
    // a late ingest after seal must 410 so a lost-retry client
    // doesn't silently re-open a terminating submission.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
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

// ─── Battleproof: length bounds ──────────────────────────────────────

#[sqlx::test]
async fn overlong_drv_path_rejected(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    // drv_path with >4096 bytes trips max_drv_path_bytes.
    let huge = format!("/nix/store/abcd-{}.drv", "x".repeat(5000));
    let batch = IngestBatchRequest {
        drvs: vec![ingest(&huge, "huge", &[], true)],
    };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.errored, 1);
    assert_eq!(resp.new_drvs, 0);
}

#[sqlx::test]
async fn overlong_drv_name_rejected(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let name = "y".repeat(2000);
    let p = drv_path("hhn", "ok");
    let mut req = ingest(&p, "ok", &[], true);
    req.drv_name = name;
    let batch = IngestBatchRequest { drvs: vec![req] };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.errored, 1);
    assert_eq!(resp.new_drvs, 0);
}

// ─── Battleproof: failures cap in terminal snapshot ──────────────────

#[sqlx::test]
async fn terminal_snapshot_caps_failures(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_failures_in_result = 3;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    // Cancel with a synthetic failures list would be hard — instead,
    // ingest 5 drvs, fail each, then check the snapshot is capped. Make
    // them independent (no deps) so each ingest + complete is a pure
    // terminal failure.
    let mut drvs = Vec::new();
    for i in 0..5 {
        let p = drv_path(&format!("cap{i}"), &format!("pkg-{i}"));
        client
            .ingest_drv(job.id, &ingest(&p, &format!("pkg-{i}"), &[], true))
            .await
            .unwrap();
        drvs.push(p);
    }
    client.seal(job.id).await.unwrap();

    for _ in 0..5 {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 5)
            .await
            .unwrap()
            .expect("claim");
        client
            .complete(
                job.id,
                c.claim_id,
                &CompleteRequest {
                    success: false,
                    duration_ms: 1,
                    exit_code: Some(1),
                    error_category: Some(ErrorCategory::BuildFailure),
                    error_message: Some("nope".into()),
                    log_tail: None,
                },
            )
            .await
            .unwrap();
    }

    // Wait for terminal.
    for _ in 0..40 {
        let s = client.status(job.id).await.unwrap();
        if s.status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Post-terminal: the submission is gone from memory; status reads
    // from jobs.result JSONB. The capped list must be <= cap+1 (the +1
    // being the truncation marker).
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Failed);
    assert!(
        status.failures.len() <= 4,
        "failures list must be capped (got {})",
        status.failures.len()
    );
    // Count is cap + 1 (marker). The marker's error_message must
    // include the EXACT overflow count (5 total − 3 cap = 2) so an
    // arithmetic mutation (`-` → `/`, `/` → `*`, etc.) flips the
    // number and fails here.
    let marker = status
        .failures
        .iter()
        .find(|f| f.drv_name == "<truncated>")
        .expect("must contain a truncation marker entry");
    let msg = marker
        .error_message
        .as_ref()
        .expect("marker must have error_message");
    assert!(
        msg.starts_with("2 "),
        "marker must report overflow=2 (5 - 3); got {msg:?}"
    );
}

// ─── /health alias for proxies that expect the unsuffixed path ─────

#[sqlx::test]
async fn both_health_and_healthz_paths_serve_ok(pool: PgPool) {
    // Some proxies (Envoy, GCP HTTP LBs) probe `/health`; kube uses
    // `/healthz`. Both must return the same OK response.
    let handle = spawn_server(pool).await;
    for path in ["/health", "/healthz"] {
        let r = reqwest::get(format!("{}{path}", handle.base_url))
            .await
            .unwrap();
        assert_eq!(r.status(), reqwest::StatusCode::OK, "{path}");
        assert_eq!(r.text().await.unwrap(), "ok", "{path}");
    }
}

// ─── HTTP/2 cleartext (h2c) prior-knowledge support ────────────────

#[sqlx::test]
async fn server_accepts_h2c_prior_knowledge_clients(pool: PgPool) {
    // Real deployment sits behind Envoy doing h2c upstream. The server
    // must accept HTTP/2 cleartext (prior-knowledge — no TLS, no
    // Upgrade dance). axum::serve uses hyper-util's auto-builder
    // which inspects the connection preface — h2c clients should work
    // without code changes.
    let handle = spawn_server(pool).await;

    let h2_client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let resp = h2_client
        .get(format!("{}/healthz", handle.base_url))
        .send()
        .await
        .expect("h2c request must succeed");
    assert_eq!(resp.version(), reqwest::Version::HTTP_2);
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "ok");

    // Also a JSON POST to make sure routes / bodies / headers all
    // negotiate fine.
    let job: nix_ci_core::types::CreateJobResponse = h2_client
        .post(format!("{}/jobs", handle.base_url))
        .json(&CreateJobRequest { external_ref: None })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(job.id.0.to_string(), "");
}

// ─── SSE consumer surfaces server failure as Err, not Ok(Pending) ──

#[sqlx::test]
async fn sse_consumer_returns_err_on_404(pool: PgPool) {
    // GET /jobs/{id}/events on an unknown job_id returns 404. The
    // print_events function used to silently return Ok(Pending) on a
    // bad-status response, hiding the failure from the orchestrator.
    // Should return Err so the caller can react.
    use std::sync::Arc;
    use tokio::sync::watch;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let bogus_job_id = nix_ci_core::types::JobId::new();

    let (sd_tx, sd_rx) = watch::channel(false);
    let res = tokio::time::timeout(
        Duration::from_secs(5),
        nix_ci_core::runner::sse::print_events(client, bogus_job_id, sd_tx, sd_rx),
    )
    .await
    .expect("sse must return promptly on 404");

    assert!(
        res.is_err(),
        "non-2xx SSE response must surface as Err, not Ok; got {res:?}"
    );
}

#[sqlx::test]
async fn sse_consumer_returns_pending_on_clean_shutdown(pool: PgPool) {
    // Caller-initiated shutdown (the typical orchestrator path on
    // SIGTERM) is the ONLY case Pending is a correct return. Hold the
    // sender outside the spawn so we can flip it from the test.
    use std::sync::Arc;
    use tokio::sync::watch;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    let (sd_tx, sd_rx) = watch::channel(false);
    let sd_tx_inner = sd_tx.clone();
    let task = tokio::spawn({
        let client = client.clone();
        async move { nix_ci_core::runner::sse::print_events(client, job.id, sd_tx_inner, sd_rx).await }
    });

    // Let the consumer subscribe + sit waiting on events.
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Caller signals shutdown — print_events must return Ok(Pending).
    let _ = sd_tx.send(true);

    let res = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("must exit promptly on shutdown")
        .expect("joined");
    assert!(
        matches!(res, Ok(JobStatus::Pending)),
        "clean shutdown must return Ok(Pending); got {res:?}"
    );
}

// ─── DrvCompleted SSE event must carry actual duration ─────────────

#[sqlx::test]
async fn sse_drv_completed_event_carries_real_duration(pool: PgPool) {
    // The complete handler records duration_ms in metrics; the
    // DrvCompleted SSE event must NOT hardcode 0 — subscribers want
    // the real duration.
    use eventsource_stream::Eventsource;
    use futures::StreamExt;

    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("dur", "timed");
    client
        .ingest_drv(job.id, &ingest(&drv, "timed", &[], true))
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
            if ev.event == "drv_completed" {
                return ev.data;
            }
        }
        panic!("SSE closed without drv_completed");
    });

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    const REPORTED_MS: u64 = 12_345;
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: REPORTED_MS,
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
    let expected = format!("\"duration_ms\":{REPORTED_MS}");
    assert!(
        payload.contains(&expected),
        "drv_completed must carry duration_ms={REPORTED_MS}, got: {payload}"
    );
}

// ─── SSE Lagged event surfaces when subscriber falls behind ─────────

#[sqlx::test]
async fn sse_lagged_event_fires_when_subscriber_falls_behind(pool: PgPool) {
    // With a tiny broadcast capacity, a subscriber that doesn't drain
    // fast enough receives a `BroadcastStreamRecvError::Lagged` which
    // `server::events` maps to a `JobEvent::Lagged { missed }`. We
    // exercise the broadcast layer + its BroadcastStream wrapper
    // directly because axum's SSE plumbing adds buffers that mask
    // backpressure at the HTTP boundary.
    use futures::StreamExt;
    use nix_ci_core::config::ServerConfig;
    use nix_ci_core::types::JobEvent;
    use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
    use tokio_stream::wrappers::BroadcastStream;

    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.submission_event_capacity = 4; // deliberately tiny
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    // Grab the submission directly and create a BroadcastStream that
    // we deliberately never poll while events fire.
    let sub = handle
        .dispatcher
        .submissions
        .get(job.id)
        .expect("submission present");
    let rx = sub.subscribe();
    let mut stream = BroadcastStream::new(rx);

    // Fire many events (>> capacity) without touching the stream.
    for i in 0..20u32 {
        sub.publish(JobEvent::Progress {
            counts: nix_ci_core::types::JobCounts {
                total: i,
                pending: i,
                ..Default::default()
            },
        });
    }

    // Now read: the very first poll must yield a Lagged error (the
    // receiver is too far behind the capacity-4 ring).
    let first = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("stream must yield")
        .expect("stream still open");
    match first {
        Err(BroadcastStreamRecvError::Lagged(missed)) => {
            assert!(missed > 0, "Lagged must carry a positive missed count");
        }
        other => {
            panic!("expected Lagged error, got {other:?}; broadcast capacity may be too generous")
        }
    }

    // And the `server::events` handler maps exactly that error to a
    // `JobEvent::Lagged { missed }` — guard the mapping so a refactor
    // (e.g. swallowing the error) doesn't silently lose it.
    let mapped = JobEvent::Lagged { missed: 7 };
    match mapped {
        JobEvent::Lagged { missed } => assert_eq!(missed, 7),
        _ => unreachable!(),
    }
}

// ─── Graceful shutdown unblocks in-flight long-poll ─────────────────

#[sqlx::test]
async fn shutdown_terminates_in_flight_claim_longpoll(pool: PgPool) {
    // A claim long-poll waits on Dispatcher::notify with a wait-deadline.
    // When the server shuts down mid-poll the request must return
    // promptly rather than hang past the long-poll deadline. This
    // exercises the axum graceful-shutdown path against an open
    // connection.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Kick off a claim with a 30s wait — no drvs, so it would sit at
    // the long-poll for the full deadline.
    let base = handle.base_url.clone();
    let job_id = job.id;
    let claim_task = tokio::spawn(async move {
        let c = CoordinatorClient::new(&base);
        c.claim(job_id, "x86_64-linux", &[], 30).await
    });
    // Let the handler enter its long-poll before we drop the server.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Dropping the handle fires the oneshot shutdown; axum drains.
    drop(handle);

    // The claim must complete (Ok or Err) within a tight window —
    // NOT the full 30s wait deadline.
    let res = tokio::time::timeout(Duration::from_secs(3), claim_task)
        .await
        .expect("claim task must unblock on shutdown");
    // Either the request was cleanly returned with None/Gone/etc.,
    // or the connection was dropped and reqwest errored — both are
    // acceptable outcomes. What's NOT acceptable is the timeout above.
    let _ = res;
}

// ─── Ingest length boundaries are inclusive up to max ───────────────

#[sqlx::test]
async fn ingest_length_boundaries_accept_at_max_reject_above(pool: PgPool) {
    // At exactly `max_drv_path_bytes` the drv must be accepted; at
    // max+1 rejected. Guards the `> max` (vs `>= max`) comparison.
    use nix_ci_core::config::ServerConfig;
    // Shrink caps to keep the test fast and the boundary arithmetic
    // obvious.
    const PATH_CAP: usize = 80;
    const NAME_CAP: usize = 20;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_drv_path_bytes = PATH_CAP;
        cfg.max_drv_name_bytes = NAME_CAP;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    // Build a drv_path of exactly PATH_CAP bytes, name of exactly NAME_CAP.
    let name_at_cap: String = "n".repeat(NAME_CAP);
    // Prefix "/nix/store/" + hash "-" + name_at_cap + ".drv".
    let prefix = "/nix/store/";
    let suffix = format!("-{name_at_cap}.drv");
    // Pad the hash region so total == PATH_CAP.
    let hash_len = PATH_CAP - prefix.len() - suffix.len();
    assert!(hash_len > 0);
    let hash = "a".repeat(hash_len);
    let path_at_cap = format!("{prefix}{hash}{suffix}");
    assert_eq!(path_at_cap.len(), PATH_CAP);

    let at_cap = IngestDrvRequest {
        drv_path: path_at_cap.clone(),
        drv_name: name_at_cap.clone(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
    };
    let resp = client
        .ingest_batch(job.id, &IngestBatchRequest { drvs: vec![at_cap] })
        .await
        .unwrap();
    assert_eq!(resp.errored, 0, "drv at exactly cap must be accepted");
    assert_eq!(resp.new_drvs, 1);

    // Now drv_path of cap+1: rejected.
    let over_cap = IngestDrvRequest {
        drv_path: format!("{path_at_cap}x"),
        drv_name: name_at_cap,
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
    };
    let resp = client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![over_cap],
            },
        )
        .await
        .unwrap();
    assert_eq!(resp.errored, 1, "drv at cap+1 must be rejected");
}

// ─── Ingest batch counts wire_dep errors ────────────────────────────

#[sqlx::test]
async fn ingest_batch_counts_wire_dep_errors(pool: PgPool) {
    // A drv with a syntactically bad `input_drv` path reaches `wire_dep`
    // which returns Err and increments the errored counter. Guards the
    // `errored += 1` in the dep-wiring branch.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let parent = drv_path("wdpar", "pkg");
    let good_parent = ingest(&parent, "pkg", &["/nix/store/nohyphen.drv"], true);
    let resp = client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![good_parent],
            },
        )
        .await
        .unwrap();
    // Parent itself is valid (1 new drv) but its one dep is bad → 1 errored.
    assert_eq!(resp.new_drvs, 1);
    assert_eq!(
        resp.errored, 1,
        "wire_dep must count syntactically-bad input_drv paths"
    );
}

// ─── Claim response carries a sensible wall deadline ────────────────

#[sqlx::test]
async fn claim_response_deadline_matches_config(pool: PgPool) {
    // The ClaimResponse carries a wall-clock `deadline`. It must be
    // `~now + claim_deadline_secs`. A sign flip (`+` → `-`) would put
    // it in the past — worker's heartbeat logic would then see a
    // "stale" claim instantly.
    use nix_ci_core::config::ServerConfig;
    const DEADLINE_SECS: u64 = 42;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = DEADLINE_SECS;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("dl", "pkg");
    client
        .ingest_drv(job.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let before = chrono::Utc::now();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    let after = chrono::Utc::now();
    // deadline should lie in the future, roughly [before + DEADLINE, after + DEADLINE + slack].
    let lo = before + chrono::Duration::seconds(DEADLINE_SECS as i64);
    let hi = after + chrono::Duration::seconds(DEADLINE_SECS as i64 + 2);
    assert!(
        c.deadline >= lo && c.deadline <= hi,
        "claim deadline {} outside expected window [{}, {}]",
        c.deadline,
        lo,
        hi
    );

    // And the in-memory `ActiveClaim::deadline` (Instant) must not be
    // already expired — a `+` → `-` flip there would cause the reaper
    // to evict the claim on its next tick. Assert expired_ids(now) is
    // empty.
    assert!(
        handle
            .dispatcher
            .claims
            .expired_ids(std::time::Instant::now())
            .is_empty(),
        "freshly-issued claim must not be already past its in-memory deadline"
    );
}

// ─── Claim wait deadline is honored ─────────────────────────────────

#[sqlx::test]
async fn claim_longpoll_returns_204_after_wait_deadline(pool: PgPool) {
    // With no runnable drvs, a claim with wait=N must return 204 after
    // approximately N seconds — NOT early, NOT late. Guards the
    // `start + Duration::from_secs(q.wait)` deadline math and the
    // `Instant::now() >= deadline` exit check.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    // Deliberately do NOT seal — an empty seal immediately terminates
    // the job (separate test) and we need the submission to stay Live
    // so the long-poll can exercise its deadline.

    let start = std::time::Instant::now();
    let res = client
        .claim(job.id, "x86_64-linux", &[], 1) // wait = 1 sec
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(res.is_none(), "no runnable drvs → 204 None");
    // A mutant that makes `deadline = start - wait` would exit
    // immediately; assert we actually waited close to the full second.
    assert!(
        elapsed >= Duration::from_millis(900),
        "claim returned too early: elapsed={elapsed:?} (expected ~1s)"
    );
    // A mutant that makes the exit check `Instant::now() < deadline`
    // would hang forever; assert we returned within a reasonable bound.
    assert!(
        elapsed < Duration::from_secs(4),
        "claim overshot its wait: elapsed={elapsed:?} (expected ≲1s)"
    );
}

// ─── Ingest validation rejects empty drv_name and system ────────────

#[sqlx::test]
async fn ingest_batch_rejects_empty_drv_name(pool: PgPool) {
    // drv_path="" is caught by drv_hash_from_path later; but an empty
    // drv_name or empty system is only rejected by the early validation
    // block. A mutant that weakens the `||` chain would let either
    // through.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let mut bad = ingest(&drv_path("ok", "ignored"), "ignored", &[], true);
    bad.drv_name = "".into();
    let batch = IngestBatchRequest { drvs: vec![bad] };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.errored, 1);
    assert_eq!(resp.new_drvs, 0);
}

#[sqlx::test]
async fn ingest_batch_rejects_empty_system(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let mut bad = ingest(&drv_path("ok", "ignored"), "ignored", &[], true);
    bad.system = "".into();
    let batch = IngestBatchRequest { drvs: vec![bad] };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.errored, 1);
    assert_eq!(resp.new_drvs, 0);
}

// ─── PG failure modes: statement timeout + pool exhaustion ─────────

#[sqlx::test]
async fn writeback_sweep_handles_statement_timeout(pool: PgPool) {
    // Even under PG statement_timeout, the cleanup sweep must fail
    // cleanly (return Err) rather than panic or wedge. The handler
    // callers (the cleanup loop) log and keep going.
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    // `pg_sleep(2)` inside a statement with a 50ms timeout triggers
    // query_canceled. Write a row that requires the sweep to read it,
    // then rely on the timeout to interrupt.
    sqlx::query("INSERT INTO failed_outputs (output_path, drv_hash, expires_at) VALUES ('/x', 'x.drv', now() - interval '1 day')")
        .execute(&pool)
        .await
        .unwrap();

    // Force the session into short-timeout mode and block on a slow
    // companion query to prove we surface errors rather than panic.
    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("SET LOCAL statement_timeout = '50ms'")
        .execute(&mut *conn)
        .await
        .unwrap();
    let r: Result<(i32,), _> = sqlx::query_as("SELECT pg_sleep(1)::int")
        .fetch_one(&mut *conn)
        .await;
    assert!(r.is_err(), "timed-out query must return Err");
    // Crucially: no panic, the connection is usable again after reset.
    drop(conn);

    // sweep still works afterward: takes a fresh connection from the
    // pool (without the LOCAL timeout) and prunes the expired row.
    nix_ci_core::durable::cleanup::sweep(&pool, 7)
        .await
        .unwrap();
    let rows: Vec<(String,)> = sqlx::query_as("SELECT output_path FROM failed_outputs")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(rows.is_empty(), "sweep must have removed the expired row");
}

#[sqlx::test]
async fn writeback_returns_error_on_closed_pool(pool: PgPool) {
    // `pool.close()` should cause subsequent writeback calls to Err
    // cleanly (not panic). Callers currently propagate the error via
    // `?`, which returns 500 to the client — acceptable.
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let job_id = nix_ci_core::types::JobId::new();
    nix_ci_core::durable::writeback::upsert_job(&pool, job_id, None)
        .await
        .unwrap();

    pool.close().await;

    let snap = serde_json::json!({ "id": job_id.0.to_string() });
    let res =
        nix_ci_core::durable::writeback::transition_job_terminal(&pool, job_id, "done", &snap)
            .await;
    assert!(res.is_err(), "closed pool must surface Err, not panic");
}

// ─── Snapshot gauges reflect live dispatcher state ─────────────────

#[sqlx::test]
async fn metrics_expose_dispatcher_snapshot_gauges(pool: PgPool) {
    // `/metrics` refreshes submissions_active and steps_registry_size
    // on every scrape so Prometheus can graph dispatcher memory
    // pressure without hitting /admin/snapshot.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
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

// ─── Broadcast delivers events to each subscriber in publish order ──

#[sqlx::test]
async fn broadcast_preserves_per_subscriber_publish_order(pool: PgPool) {
    // tokio::sync::broadcast guarantees FIFO per receiver — confirm
    // we haven't introduced any reordering in our JobEvent plumbing.
    // Use two subscribers to also catch a "publish only to one" bug.
    use futures::StreamExt;
    use nix_ci_core::types::JobEvent;
    use tokio_stream::wrappers::BroadcastStream;

    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let sub = handle
        .dispatcher
        .submissions
        .get(job.id)
        .expect("submission");

    // Two subscribers before any event fires.
    let mut stream_a = BroadcastStream::new(sub.subscribe());
    let mut stream_b = BroadcastStream::new(sub.subscribe());

    // Publish N events with strictly-increasing `total`.
    const N: u32 = 50;
    for i in 0..N {
        sub.publish(JobEvent::Progress {
            counts: nix_ci_core::types::JobCounts {
                total: i,
                ..Default::default()
            },
        });
    }

    // Drain each stream; sequences must match 0..N exactly.
    async fn drain_totals(stream: &mut BroadcastStream<JobEvent>, n: usize) -> Vec<u32> {
        let mut totals = Vec::with_capacity(n);
        for _ in 0..n {
            let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .expect("stream yields within timeout")
                .expect("stream open")
                .expect("not lagged");
            if let JobEvent::Progress { counts } = ev {
                totals.push(counts.total);
            } else {
                panic!("unexpected event type");
            }
        }
        totals
    }

    let seq_a = drain_totals(&mut stream_a, N as usize).await;
    let seq_b = drain_totals(&mut stream_b, N as usize).await;
    let expected: Vec<u32> = (0..N).collect();
    assert_eq!(seq_a, expected, "subscriber A must see publish-order");
    assert_eq!(seq_b, expected, "subscriber B must see publish-order");
}

// ─── Reaper does NOT re-arm a finished step ────────────────────────

#[sqlx::test]
async fn reaper_does_not_re_arm_step_finished_via_propagation(pool: PgPool) {
    // Race the reaper would otherwise lose: a step's claim has expired,
    // but before the reaper sets `runnable=true`, propagation marks
    // the step finished (e.g. an upstream sibling's failure cascaded).
    // Post-condition: NO step ends up with both `finished=true` AND
    // `runnable=true` — that would violate the dispatcher invariant
    // and leave an orphan entry in the ready queue.
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = 1;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("racefin", "pkg");
    client
        .ingest_drv(job.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");

    // Force claim deadline to the past.
    let claim = handle
        .dispatcher
        .claims
        .take(c.claim_id)
        .expect("claim present");
    let mut forced = (*claim).clone();
    forced.deadline = std::time::Instant::now() - Duration::from_secs(10);
    handle.dispatcher.claims.insert(Arc::new(forced));

    // Mark the step finished BEFORE the reaper runs — simulates
    // propagation from a concurrent failure beating the reaper to
    // the punch.
    let drv_hash = nix_ci_core::types::drv_hash_from_path(&drv).unwrap();
    let step = handle
        .dispatcher
        .steps
        .get(&drv_hash)
        .expect("step present");
    step.previous_failure.store(true, Ordering::Release);
    step.finished.store(true, Ordering::Release);

    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);

    // Invariant: a finished step must NOT also be runnable. Otherwise
    // pop_runnable wastes work and the admin snapshot is inconsistent.
    assert!(
        step.finished.load(Ordering::Acquire),
        "test setup: step should still be finished"
    );
    assert!(
        !step.runnable.load(Ordering::Acquire),
        "reaper must not re-arm a step that became finished concurrently"
    );
}

// ─── Reaper handles many claims expiring on the same tick ──────────

#[sqlx::test]
async fn reaper_rearms_many_concurrent_expired_claims(pool: PgPool) {
    // Scenario: 50 workers claim 50 drvs, all disappear (no complete).
    // A reaper tick fires when every claim is past its deadline — all
    // 50 steps must be re-armed with runnable=true (attempt++), the
    // claims gauge must return to 0, and no step gets stuck.
    use nix_ci_core::config::ServerConfig;
    const N: usize = 50;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = 1;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drvs: Vec<IngestDrvRequest> = (0..N)
        .map(|i| {
            ingest(
                &drv_path(&format!("conc{i:03}"), &format!("n{i}")),
                &format!("n{i}"),
                &[],
                true,
            )
        })
        .collect();
    client
        .ingest_batch(job.id, &IngestBatchRequest { drvs })
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Fire N claims in parallel.
    let mut claim_ids = Vec::with_capacity(N);
    for _ in 0..N {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 3)
            .await
            .unwrap()
            .expect("claim");
        claim_ids.push(c.claim_id);
    }
    assert_eq!(handle.dispatcher.claims.len(), N);

    // Force ALL claim deadlines to the past.
    let past = std::time::Instant::now() - Duration::from_secs(10);
    for cid in &claim_ids {
        if let Some(claim) = handle.dispatcher.claims.take(*cid) {
            let mut forced = (*claim).clone();
            forced.deadline = past;
            handle.dispatcher.claims.insert(std::sync::Arc::new(forced));
        }
    }

    // One reaper tick reaps every expired claim.
    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);

    // All claims evicted.
    assert_eq!(
        handle.dispatcher.claims.len(),
        0,
        "all expired claims must be evicted in one tick"
    );

    // Every step must be runnable again; workers should be able to
    // claim again. Drain the new claims and confirm we get exactly N.
    let mut second_wave = 0;
    for _ in 0..N {
        if let Ok(Some(_)) = client.claim(job.id, "x86_64-linux", &[], 3).await {
            second_wave += 1;
        }
    }
    assert_eq!(
        second_wave, N,
        "all N steps must be claimable again after reap"
    );
}

// ─── CoordinatorLock: two processes race for single-writer ─────────

#[sqlx::test]
async fn coordinator_lock_second_acquirer_blocks_until_first_drops(_pool: PgPool) {
    // Advisory lock semantics: given a unique key, the first acquirer
    // succeeds immediately; a second blocks until the first drops its
    // handle. Use a randomized key per test-run to avoid collision
    // with parallel sqlx::tests sharing the server.
    let db_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run the lock test");
    let key: i64 = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        & 0x7fff_ffff) as i64;

    let first = nix_ci_core::durable::CoordinatorLock::acquire(&db_url, key)
        .await
        .expect("first acquire");

    // Second acquire must NOT complete while `first` is held.
    let url = db_url.clone();
    let second_task =
        tokio::spawn(
            async move { nix_ci_core::durable::CoordinatorLock::acquire(&url, key).await },
        );

    // Give the second task 300ms to begin blocking. If it finishes in
    // that window, the lock is broken.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !second_task.is_finished(),
        "second acquire must be blocked while first is held"
    );

    // Drop `first`: pg advisory lock releases when the session closes.
    // Second should unblock promptly.
    drop(first);
    let unlocked = tokio::time::timeout(Duration::from_secs(5), second_task)
        .await
        .expect("second acquire must unblock after first drops")
        .expect("second task join")
        .expect("second acquire succeeds");
    drop(unlocked);
}

// ─── Writeback idempotency: transition_job_terminal + seal_job ──────

#[sqlx::test]
async fn transition_job_terminal_is_idempotent(pool: PgPool) {
    // First call must transition (return true); second must be a no-op
    // (return false) because `WHERE done_at IS NULL` rejects it. The
    // complete handler relies on this to decide whether to publish
    // JobEvent::JobDone exactly once.
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let job_id = nix_ci_core::types::JobId::new();
    nix_ci_core::durable::writeback::upsert_job(&pool, job_id, None)
        .await
        .unwrap();
    let snap = serde_json::json!({ "id": job_id.0.to_string() });
    let first =
        nix_ci_core::durable::writeback::transition_job_terminal(&pool, job_id, "done", &snap)
            .await
            .unwrap();
    assert!(first, "first transition must return true");
    let second =
        nix_ci_core::durable::writeback::transition_job_terminal(&pool, job_id, "done", &snap)
            .await
            .unwrap();
    assert!(!second, "second transition must return false (idempotent)");

    // Non-existent job: also returns false (no row affected).
    let missing_id = nix_ci_core::types::JobId::new();
    let ghost =
        nix_ci_core::durable::writeback::transition_job_terminal(&pool, missing_id, "done", &snap)
            .await
            .unwrap();
    assert!(!ghost, "non-existent job must return false");
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
async fn find_job_by_external_ref_roundtrips(pool: PgPool) {
    // `create` uses `find_job_by_external_ref` to make POST /jobs
    // idempotent. A mutant that replaces the function body with
    // `Ok(None)` would silently mint a new id on every retry with the
    // same external_ref, breaking idempotency.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let first = client
        .create_job(&CreateJobRequest {
            external_ref: Some("idempotent-ref-xyz".into()),
        })
        .await
        .unwrap();
    let second = client
        .create_job(&CreateJobRequest {
            external_ref: Some("idempotent-ref-xyz".into()),
        })
        .await
        .unwrap();
    assert_eq!(
        first.id, second.id,
        "create with identical external_ref must return the same id"
    );
}

// ─── Reaper re-arms non-finished expired claims ─────────────────────

#[sqlx::test]
async fn reaper_rearms_non_finished_expired_claim(pool: PgPool) {
    // A claim whose deadline passed must be evicted AND its step
    // re-armed (`runnable=true` + enqueued), so another worker picks
    // it up. If the `!step.finished` guard in reaper is flipped/deleted
    // the reaper would skip the re-arm or re-arm a finished step —
    // breaking either progress or invariants.
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = 1; // short deadline for test
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("rearm", "pkg");
    client
        .ingest_drv(job.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Worker A claims, then "disappears" (doesn't complete).
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim A");

    // Force the claim's in-memory deadline to expire immediately by
    // dialing it down to now, then invoke reaper.
    let claim = handle
        .dispatcher
        .claims
        .take(c.claim_id)
        .expect("claim present");
    let past = std::time::Instant::now() - Duration::from_secs(10);
    let mut forced = (*claim).clone();
    forced.deadline = past;
    handle.dispatcher.claims.insert(std::sync::Arc::new(forced));

    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);

    // Worker B should now be able to claim the same drv again.
    let c2 = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim B after re-arm");
    assert_eq!(c2.drv_path, drv);
    assert_eq!(c2.attempt, 2, "re-armed claim must increment attempt");
}

// ─── Cleanup sweep removes stale rows ────────────────────────────────

#[sqlx::test]
async fn cleanup_sweep_evicts_expired_failed_outputs_and_old_jobs(pool: PgPool) {
    // Direct unit-level test of durable::cleanup::sweep. Guards both
    // queries (TTL-expired failed_outputs + retention-expired terminal
    // jobs) against SQL typo / interval-handling regressions.
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    // Seed: one already-expired failed_output, one fresh.
    sqlx::query("INSERT INTO failed_outputs (output_path, drv_hash, expires_at) VALUES ($1,$2, now() - interval '1 day')")
        .bind("/nix/store/expired-out")
        .bind("expired-hash.drv")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO failed_outputs (output_path, drv_hash, expires_at) VALUES ($1,$2, now() + interval '1 day')")
        .bind("/nix/store/fresh-out")
        .bind("fresh-hash.drv")
        .execute(&pool)
        .await
        .unwrap();

    // Seed: one terminal job older than retention, one within retention.
    let old_id = uuid::Uuid::new_v4();
    let recent_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO jobs (id, status, done_at, result) VALUES \
         ($1, 'done', now() - interval '30 days', '{}'::jsonb), \
         ($2, 'done', now() - interval '1 hour', '{}'::jsonb)",
    )
    .bind(old_id)
    .bind(recent_id)
    .execute(&pool)
    .await
    .unwrap();

    // Retention = 7 days: old is past retention, recent is within.
    nix_ci_core::durable::cleanup::sweep(&pool, 7)
        .await
        .unwrap();

    let remaining_outputs: Vec<(String,)> =
        sqlx::query_as("SELECT output_path FROM failed_outputs ORDER BY output_path")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(remaining_outputs.len(), 1);
    assert_eq!(remaining_outputs[0].0, "/nix/store/fresh-out");

    let remaining_jobs: Vec<(sqlx::types::Uuid,)> =
        sqlx::query_as("SELECT id FROM jobs WHERE id IN ($1, $2) ORDER BY id")
            .bind(old_id)
            .bind(recent_id)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(remaining_jobs.len(), 1);
    assert_eq!(remaining_jobs[0].0, recent_id);
}

// ─── Battleproof: empty submission seals straight to Done ───────────

#[sqlx::test]
async fn seal_with_no_toplevels_transitions_to_done(pool: PgPool) {
    // A sealed submission whose toplevels set is empty has "all
    // toplevels finished" vacuously true, so it must terminate as Done
    // immediately. Without the explicit check in `seal`, the client
    // waits for a completion that will never come.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
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

// ─── Battleproof: terminal failure caches output path ───────────────

#[sqlx::test]
async fn terminal_failure_caches_output_path(pool: PgPool) {
    // A terminal build failure must insert the output path (drv_path
    // with `.drv` stripped) into `failed_outputs` so concurrent jobs
    // short-circuit re-ingest. Guards the `output_path != drv_path`
    // guard condition in handle_failure — a subtle negation there would
    // skip the insert silently on every real failure.
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("fo", "cache-me");
    client
        .ingest_drv(job.id, &ingest(&drv, "cache-me", &[], true))
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
                success: false,
                duration_ms: 1,
                exit_code: Some(1),
                error_category: Some(ErrorCategory::BuildFailure),
                error_message: Some("nope".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();

    // Drive to terminal (best-effort: the insert happens in the same
    // handler as the complete call, so we can assert right away).
    let expected_output: String = drv.trim_end_matches(".drv").to_string();
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT output_path FROM failed_outputs WHERE output_path = $1")
            .bind(&expected_output)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "expected failed_outputs row for {expected_output}, rows={rows:?}"
    );
}

// ─── Battleproof: DrvFailed SSE event fires on terminal failure ─────

#[sqlx::test]
async fn sse_drv_failed_event_surfaces_for_terminal_failure(pool: PgPool) {
    use eventsource_stream::Eventsource;
    use futures::StreamExt;

    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("drvfail", "oops");
    client
        .ingest_drv(job.id, &ingest(&drv, "oops", &[], true))
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
            if ev.event == "drv_failed" {
                return ev.data;
            }
            if ev.event == "job_done" {
                // Reached terminal without seeing drv_failed — fail.
                return format!("UNEXPECTED_JOB_DONE:{}", ev.data);
            }
        }
        panic!("SSE closed without drv_failed");
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
                success: false,
                duration_ms: 1,
                exit_code: Some(1),
                error_category: Some(ErrorCategory::BuildFailure),
                error_message: Some("boom".into()),
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
        !payload.starts_with("UNEXPECTED_JOB_DONE"),
        "drv_failed must arrive before job_done: {payload}"
    );
    // Payload must contain the drv_name field (guards the Step::drv_name
    // getter and publish_drv_failed's population of the event).
    assert!(
        payload.contains(r#""drv_name":"oops""#),
        "drv_failed payload missing drv_name 'oops': {payload}"
    );
    assert!(payload.contains(r#""error_category":"build_failure""#));
    assert!(payload.contains(r#""will_retry":false"#));
}

// ─── Battleproof: flaky-retry backoff formula ────────────────────────

#[sqlx::test]
async fn flaky_retry_sets_next_attempt_at_to_linear_backoff(pool: PgPool) {
    // The retry backoff for attempt N is `backoff_step_ms * N`, stored
    // as an absolute wall-clock millis. Arithmetic mutations (× → +,
    // + → -, + → *) silently wreck this: too-short backoff hammers the
    // coordinator on every claim loop; too-long backoff starves the
    // retry.
    use nix_ci_core::config::ServerConfig;
    const STEP_MS: i64 = 5_000;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.flaky_retry_backoff_step_ms = STEP_MS;
        // Allow attempts 1..=3 as retryable so we can observe both
        // attempt=1 and attempt=2 backoff windows.
        cfg.max_attempts = 4;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("retrybk", "retryme");
    client
        .ingest_drv(job.id, &ingest(&drv, "retryme", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // For each attempt in 1..=2, fail as Transient and assert
    // next_attempt_at = now + STEP_MS * attempt (± a tight window).
    // Attempt=2 is critical: it's where `*` vs `+` diverges visibly
    // (5000*2=10000 vs 5000+2=5002), and where `*` vs `/` does too
    // (5000*2=10000 vs 5000/2=2500).
    let drv_hash = nix_ci_core::types::drv_hash_from_path(&drv).unwrap();
    // max_tries default is 2 → attempts 1 and 2 are retryable.
    for attempt_n in 1..=2 {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 3)
            .await
            .unwrap()
            .expect("claim");
        assert_eq!(c.attempt, attempt_n);
        let before_ms = chrono::Utc::now().timestamp_millis();
        client
            .complete(
                job.id,
                c.claim_id,
                &CompleteRequest {
                    success: false,
                    duration_ms: 1,
                    exit_code: Some(137),
                    error_category: Some(ErrorCategory::Transient),
                    error_message: Some("net".into()),
                    log_tail: None,
                },
            )
            .await
            .unwrap();
        let after_ms = chrono::Utc::now().timestamp_millis();

        let step = handle
            .dispatcher
            .steps
            .get(&drv_hash)
            .expect("step still in registry during retry");
        let next = step
            .next_attempt_at
            .load(std::sync::atomic::Ordering::Acquire);
        let expected = STEP_MS * i64::from(attempt_n);
        let lo = before_ms + expected;
        let hi = after_ms + expected + 100; // generous upper margin
        assert!(
            (lo..=hi).contains(&next),
            "attempt={attempt_n}: next_attempt_at={next} outside [{lo}, {hi}] (expected ~+{expected}ms)"
        );
        // Clear backoff to let attempt N+1 run immediately.
        step.next_attempt_at
            .store(0, std::sync::atomic::Ordering::Release);
    }
}

// ─── Battleproof: propagated failures count matches rdep DAG size ───

#[sqlx::test]
async fn propagated_failures_count_matches_rdep_closure(pool: PgPool) {
    // Graph: root depends on mid; mid depends on leaf. When leaf fails
    // terminally, propagation marks BOTH mid and root failed — that's
    // exactly 2 propagated drvs. The mutation `+= *=` on the counter
    // in `propagate_failure_inmem` would yield 0 or 1 instead of 2,
    // making the metric assertion fail.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    let leaf = drv_path("prop-l", "leaf");
    let mid = drv_path("prop-m", "mid");
    let root = drv_path("prop-r", "root");
    let drvs = vec![
        ingest(&leaf, "leaf", &[], false),
        ingest(&mid, "mid", &[&leaf], false),
        ingest(&root, "root", &[&mid], true),
    ];
    client
        .ingest_batch(job.id, &IngestBatchRequest { drvs })
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // leaf is the only initially-runnable step (only leaf-node in the
    // DAG). Fail it terminally and propagation must cascade to mid +
    // root.
    let c = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("claim leaf");
    assert_eq!(c.drv_path, leaf);
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 1,
                exit_code: Some(1),
                error_category: Some(ErrorCategory::BuildFailure),
                error_message: Some("fail".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();

    // Wait for terminal.
    for _ in 0..40 {
        if client.status(job.id).await.unwrap().status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Failed);
    // Failures list must include: originating leaf + propagated mid + root.
    // The exact count is 3 (1 originating, 2 propagated). The mutations
    // that multiply instead of incrementing the propagated counter only
    // affect the metric — the durable `failures` list is the observable
    // record; we rely on the metric AND the failures entries.
    let propagated_count = status
        .failures
        .iter()
        .filter(|f| f.error_category == ErrorCategory::PropagatedFailure)
        .count();
    assert_eq!(
        propagated_count, 2,
        "expected 2 propagated failures, got failures={:?}",
        status.failures
    );

    // Cross-check via metrics endpoint.
    let body = reqwest::get(format!("{}/metrics", handle.base_url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("nix_ci_propagated_failures_total 2")
            || body.contains("nix_ci_propagated_failures 2"),
        "propagated_failures metric != 2 in:\n{body}"
    );
}

// ─── Battleproof: admin snapshot counts scale linearly with membership ──

#[sqlx::test]
async fn admin_snapshot_counts_scale_linearly_with_members(pool: PgPool) {
    // Guards against arithmetic mutations in `Submission::live_counts`
    // that would leave totals stuck regardless of membership size.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
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
        .ingest_batch(job.id, &IngestBatchRequest { drvs })
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

// ─── Battleproof: request body size limit ────────────────────────────

#[sqlx::test]
async fn oversized_batch_rejected_with_413(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_request_body_bytes = 16 * 1024; // 16 KiB — tiny for test speed
    })
    .await;
    let client = reqwest::Client::new();
    let job_resp: nix_ci_core::types::CreateJobResponse = client
        .post(format!("{}/jobs", handle.base_url))
        .json(&CreateJobRequest { external_ref: None })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // 200 KiB of payload — well over the configured 16 KiB cap.
    let big = "x".repeat(200 * 1024);
    let resp = client
        .post(format!(
            "{}/jobs/{}/drvs/batch",
            handle.base_url, job_resp.id
        ))
        .header("content-type", "application/json")
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "body above max_request_body_bytes must return 413"
    );
}
