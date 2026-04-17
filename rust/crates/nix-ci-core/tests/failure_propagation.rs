//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: dep-failure propagation and failed_outputs cache.

mod common;

use std::time::Duration;

use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, ErrorCategory, IngestBatchRequest,
    JobStatus,
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
        .ingest_batch(job.id, &IngestBatchRequest { drvs, eval_errors: Vec::new() })
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
