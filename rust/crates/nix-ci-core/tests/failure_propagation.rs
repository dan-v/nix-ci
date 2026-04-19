//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: dep-failure propagation and failed_outputs cache.

mod common;

use std::time::Duration;

use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, ErrorCategory, IngestBatchRequest, JobStatus,
};
use sqlx::PgPool;

#[sqlx::test]
async fn previously_failed_drv_short_circuits_on_fresh_ingest(pool: PgPool) {
    // The whole point of failed_outputs: when a *new* job ingests a
    // drv that already failed terminally in a prior job (within the
    // TTL), the dispatcher must mark it failed immediately rather
    // than dispatching another build. The short-circuit was silently
    // broken: inserts wrote the stripped output path while ingests
    // queried with the full `.drv` path → never matched.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Job 1: drive a drv to terminal BuildFailure → cache populated.
    let job1 = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("shorted", "pkg");
    client
        .ingest_drv(job1.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job1.id).await.unwrap();
    let c = client
        .claim(job1.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    client
        .complete(
            job1.id,
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

    // Wait for job1 terminal.
    for _ in 0..40 {
        if client.status(job1.id).await.unwrap().status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        client.status(job1.id).await.unwrap().status,
        JobStatus::Failed
    );

    // Job 2: ingest the SAME drv. The cache should mark it
    // previous_failure on ingest. After seal, the job is immediately
    // Failed — no claim is ever issued.
    let job2 = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .ingest_drv(job2.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job2.id).await.unwrap();

    // No worker should be able to claim — the drv is pre-marked
    // finished+previous_failure on ingest.
    let res = client.claim(job2.id, "x86_64-linux", &[], 1).await;
    match res {
        Ok(None) => {}                         // 204: nothing to do (terminated)
        Err(nix_ci_core::Error::Gone(_)) => {} // 410: terminal
        other => panic!("expected None/Gone (cache short-circuit); got {other:?}"),
    }

    // And status is Failed without ever issuing a claim.
    for _ in 0..20 {
        if client.status(job2.id).await.unwrap().status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let s = client.status(job2.id).await.unwrap();
    assert_eq!(
        s.status,
        JobStatus::Failed,
        "second job must terminate Failed via cache short-circuit (no rebuild)"
    );
}

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
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
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

#[sqlx::test]
async fn transient_retry_exhaustion_does_not_cache_output_path(pool: PgPool) {
    // Transient / DiskFull failures are builder-environment problems,
    // not drv problems. Even after retries exhaust, the drv is still
    // potentially buildable on a different worker — caching it in
    // failed_outputs would falsely short-circuit future ingests.
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("nopoison", "flaky");
    client
        .ingest_drv(job.id, &ingest(&drv, "flaky", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Fail with Transient until attempts exhaust (max_attempts default = 2).
    let drv_hash = nix_ci_core::types::drv_hash_from_path(&drv).unwrap();
    for _ in 0..3 {
        let c = match client.claim(job.id, "x86_64-linux", &[], 3).await {
            Ok(Some(c)) => c,
            Ok(None) => break,
            Err(nix_ci_core::Error::Gone(_)) => break,
            Err(e) => panic!("unexpected: {e}"),
        };
        client
            .complete(
                job.id,
                c.claim_id,
                &CompleteRequest {
                    success: false,
                    duration_ms: 1,
                    exit_code: Some(137),
                    error_category: Some(ErrorCategory::Transient),
                    error_message: Some("network blip".into()),
                    log_tail: None,
                },
            )
            .await
            .unwrap();
        if let Some(step) = handle.dispatcher.steps.get(&drv_hash) {
            step.next_attempt_at
                .store(0, std::sync::atomic::Ordering::Release);
        }
    }

    // Job is terminal Failed (retries exhausted). Verify failed_outputs
    // table is EMPTY for this drv — it's not the drv's fault.
    let expected_output = drv.trim_end_matches(".drv").to_string();
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT output_path FROM failed_outputs WHERE output_path = $1")
            .bind(&expected_output)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        rows.is_empty(),
        "Transient retry-exhaustion must NOT cache output_path; rows={rows:?}"
    );
}

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
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
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

/// End-to-end for the `suspected_worker_infra` terminal heuristic:
/// 4-of-5 originating DiskFull failures from the same worker_id
/// must flag that host in `GET /jobs/{id}.suspected_worker_infra`.
///
/// Configured with `max_attempts=1` so a single infra-like failure
/// terminalizes each drv — gets us into the
/// `!can_retry → handle_failure terminal` path where the DrvFailure's
/// `worker_id` is populated from the claim.
///
/// This proves the value round-trips from /complete → Submission's
/// record_failure → terminal JSONB snapshot → /jobs/{id} response,
/// complementing the unit tests in `server::complete::tests::suspicion_*`
/// which cover the heuristic logic in isolation.
#[sqlx::test]
async fn suspected_worker_infra_fires_on_single_host_infra_burst(pool: PgPool) {
    let handle = common::spawn_server_with_cfg(pool, |cfg| {
        cfg.max_attempts = 1;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let leaves: Vec<_> = (0..5)
        .map(|i| drv_path(&format!("siw{i}"), &format!("siw{i}")))
        .collect();
    let drvs: Vec<_> = leaves
        .iter()
        .enumerate()
        .map(|(i, d)| ingest(d, &format!("siw{i}"), &[], true))
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

    let sick_host = "sick-host-a";
    for i in 0..5 {
        let worker = if i < 4 { sick_host } else { "healthy-host" };
        let c = client
            .claim_as_worker(job.id, "x86_64-linux", &[], 3, Some(worker))
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
                    error_category: Some(ErrorCategory::DiskFull),
                    error_message: Some(format!("siw{i}: disk full")),
                    log_tail: None,
                },
            )
            .await
            .unwrap();
    }

    // Wait for terminal.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let s = client.status(job.id).await.unwrap();
        if s.status.is_terminal() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("job never terminalized");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let terminal = client.status(job.id).await.unwrap();
    assert_eq!(terminal.status, JobStatus::Failed);
    assert_eq!(
        terminal.suspected_worker_infra.as_deref(),
        Some(sick_host),
        "4-of-5 originating DiskFull from one host must flag that host; \
         failures={:?}",
        terminal.failures
    );
}

/// Counter-test: 5 BuildFailure failures all from the same host
/// must NOT flag suspected_worker_infra. BuildFailure is
/// deterministic — the drv genuinely failed to compile — so
/// attributing it to the worker would mislead the operator into
/// chasing infra when the bug is in the build script.
#[sqlx::test]
async fn suspected_worker_infra_stays_none_for_buildfailure_cluster(pool: PgPool) {
    let handle = common::spawn_server_with_cfg(pool, |cfg| {
        cfg.max_attempts = 1;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let leaves: Vec<_> = (0..5)
        .map(|i| drv_path(&format!("bf{i}"), &format!("bf{i}")))
        .collect();
    let drvs: Vec<_> = leaves
        .iter()
        .enumerate()
        .map(|(i, d)| ingest(d, &format!("bf{i}"), &[], true))
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

    for i in 0..5 {
        let c = client
            .claim_as_worker(job.id, "x86_64-linux", &[], 3, Some("host-a"))
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
                    error_message: Some(format!("bf{i}: builder failed")),
                    log_tail: None,
                },
            )
            .await
            .unwrap();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let s = client.status(job.id).await.unwrap();
        if s.status.is_terminal() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("job never terminalized");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let terminal = client.status(job.id).await.unwrap();
    assert_eq!(terminal.status, JobStatus::Failed);
    assert!(
        terminal.suspected_worker_infra.is_none(),
        "BuildFailure cluster must NOT flag a host; got {:?}",
        terminal.suspected_worker_infra
    );
}
