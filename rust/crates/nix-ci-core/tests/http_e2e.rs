//! End-to-end HTTP tests against an in-process server and real
//! Postgres. Each test gets its own database via `sqlx::test`.

mod common;

use common::{drv_path, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, IngestBatchRequest, IngestDrvRequest, JobStatus,
};
use sqlx::PgPool;
use std::time::Duration;

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

#[sqlx::test]
async fn create_and_status(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Pending);
    assert_eq!(status.counts.total, 0);
}

#[sqlx::test]
async fn linear_chain_of_three_completes(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    // a ← b ← c (c is root, depends on b, which depends on a)
    let a = drv_path("aaa", "leaf-a");
    let b = drv_path("bbb", "mid-b");
    let c = drv_path("ccc", "root-c");

    client
        .ingest_drv(job.id, &ingest(&a, "leaf-a", &[], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&b, "mid-b", &[&a], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&c, "root-c", &[&b], true))
        .await
        .unwrap();

    client.seal(job.id).await.unwrap();

    // Worker loop: claim → pretend to build → complete, chasing
    // opportunistic next_build suggestions. Bail once 204.
    let mut iterations = 0;
    let mut pending: Option<nix_ci_core::types::ClaimResponse> = None;
    loop {
        iterations += 1;
        assert!(iterations < 20, "too many iterations");
        let c = match pending.take() {
            Some(c) => c,
            None => match client.claim(job.id, "x86_64-linux", &[], 5).await {
                Ok(Some(c)) => c,
                Ok(None) | Err(nix_ci_core::Error::Gone(_)) => break,
                Err(e) => panic!("unexpected claim error: {e}"),
            },
        };
        let resp = client
            .complete(
                job.id,
                c.claim_id,
                &CompleteRequest {
                    success: true,
                    duration_ms: 10,
                    exit_code: Some(0),
                    error_category: None,
                    error_message: None,
                    log_tail: None,
                },
            )
            .await
            .unwrap();
        pending = resp.next_build;
    }

    // Eventually the job's status becomes done
    let mut status = client.status(job.id).await.unwrap();
    for _ in 0..20 {
        if status.status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        status = client.status(job.id).await.unwrap();
    }

    assert_eq!(status.status, JobStatus::Done);
    assert_eq!(status.counts.done, 3);
    assert_eq!(status.counts.failed, 0);
}

#[sqlx::test]
async fn build_failure_marks_job_failed(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    let root = drv_path("aaa", "will-fail");
    client
        .ingest_drv(job.id, &ingest(&root, "will-fail", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let claim = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("must have one claim");
    client
        .complete(
            job.id,
            claim.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 100,
                exit_code: Some(1),
                error_category: Some(nix_ci_core::types::ErrorCategory::BuildFailure),
                error_message: Some("compile failed".into()),
                log_tail: Some("error: something broke".into()),
            },
        )
        .await
        .unwrap();

    // Build failure is non-retryable → terminal failure immediately.
    let mut status = client.status(job.id).await.unwrap();
    for _ in 0..20 {
        if status.status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        status = client.status(job.id).await.unwrap();
    }
    assert_eq!(status.status, JobStatus::Failed);
    assert_eq!(status.counts.failed, 1);
}

#[sqlx::test]
async fn failure_propagates_to_dependents(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    let leaf = drv_path("ddd", "leaf");
    let mid = drv_path("eee", "mid");
    let root = drv_path("fff", "root");

    client
        .ingest_drv(job.id, &ingest(&leaf, "leaf", &[], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&mid, "mid", &[&leaf], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&root, "root", &[&mid], true))
        .await
        .unwrap();

    client.seal(job.id).await.unwrap();

    // Worker claims leaf → fails. Explicitly long-poll longer to let
    // the dispatcher arm the leaf after ingest.
    let claim = client
        .claim(job.id, "x86_64-linux", &[], 10)
        .await
        .unwrap()
        .expect("leaf claim");
    assert!(claim.drv_path.ends_with("leaf.drv"));
    client
        .complete(
            job.id,
            claim.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 100,
                exit_code: Some(1),
                error_category: Some(nix_ci_core::types::ErrorCategory::BuildFailure),
                error_message: Some("leaf fail".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();

    // mid + root should be failed via propagation; we should not get
    // another claim for either. After terminal, the submission is
    // dropped from the dispatcher map, so claim returns 410 Gone (or
    // 204 if we race before removal).
    match client.claim(job.id, "x86_64-linux", &[], 2).await {
        Ok(None) | Err(nix_ci_core::Error::Gone(_)) => {}
        Ok(Some(c)) => panic!("no more claims after propagation, got {}", c.drv_path),
        Err(e) => panic!("unexpected claim error: {e}"),
    }

    let mut status = client.status(job.id).await.unwrap();
    for _ in 0..20 {
        if status.status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        status = client.status(job.id).await.unwrap();
    }
    assert_eq!(status.status, JobStatus::Failed);
    assert_eq!(status.counts.done, 0);
    assert!(status.counts.failed >= 1);
}

#[sqlx::test]
async fn retryable_failure_retries_then_succeeds(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("rrr", "flaky");
    client
        .ingest_drv(job.id, &ingest(&drv, "flaky", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // First claim → transient fail (retryable)
    let c = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .unwrap();
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 50,
                exit_code: Some(137),
                error_category: Some(nix_ci_core::types::ErrorCategory::Transient),
                error_message: Some("network blip".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();

    // The drv is now in retry backoff. Bypass it so the test doesn't
    // wait 30s — backoff lives in-memory only, no DB row to reset.
    let dispatcher = handle.dispatcher.clone();
    let drv_hash = nix_ci_core::types::drv_hash_from_path(&drv).unwrap();
    let step = dispatcher.steps.get(&drv_hash).unwrap();
    step.next_attempt_at
        .store(0, std::sync::atomic::Ordering::Release);

    // Next claim → succeed
    let c = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c.attempt, 2);
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 60,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();

    let mut status = client.status(job.id).await.unwrap();
    for _ in 0..20 {
        if status.status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        status = client.status(job.id).await.unwrap();
    }
    assert_eq!(status.status, JobStatus::Done);
}

#[sqlx::test]
async fn stale_complete_returns_ignored(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("sss", "solo");
    client
        .ingest_drv(job.id, &ingest(&drv, "solo", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let c = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .unwrap();
    // Complete successfully
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 10,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();

    // Repeat the complete with the same claim_id — should be ignored
    let repeat = client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 10,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();
    assert!(repeat.ignored);
}

#[sqlx::test]
async fn batch_ingest_full_dag_sequences_builds(pool: PgPool) {
    // Full-DAG ingest: a 3-level chain submitted in one batch.
    // The dispatcher must serve them in dep-order (leaf first), then
    // propagate runnable bit upward on each completion.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    let leaf = drv_path("aaa", "leaf");
    let mid = drv_path("bbb", "mid");
    let root = drv_path("ccc", "root");

    let batch = IngestBatchRequest {
        drvs: vec![
            ingest(&leaf, "leaf", &[], false),
            ingest(&mid, "mid", &[&leaf], false),
            ingest(&root, "root", &[&mid], true),
        ],
    };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.new_drvs, 3);
    assert_eq!(resp.dedup_skipped, 0);
    assert_eq!(resp.errored, 0);

    client.seal(job.id).await.unwrap();

    // Must see leaf first, then mid, then root — dispatcher enforces
    // dep ordering even when the three were submitted in one batch.
    let mut order: Vec<String> = Vec::new();
    let mut pending: Option<nix_ci_core::types::ClaimResponse> = None;
    for _ in 0..10 {
        let c = match pending.take() {
            Some(c) => c,
            None => match client.claim(job.id, "x86_64-linux", &[], 5).await {
                Ok(Some(c)) => c,
                Ok(None) | Err(nix_ci_core::Error::Gone(_)) => break,
                Err(e) => panic!("unexpected claim error: {e}"),
            },
        };
        order.push(c.drv_path.clone());
        let r = client
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
        pending = r.next_build;
    }

    assert_eq!(order, vec![leaf.clone(), mid.clone(), root.clone()]);
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Done);
    assert_eq!(status.counts.done, 3);
}

#[sqlx::test]
async fn batch_ingest_cross_job_transitive_dedup(pool: PgPool) {
    // Two jobs both submit the same shared leaf (e.g. custom stdenv).
    // Only one of them ever gets a claim for it.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job_a = client
        .create_job(&CreateJobRequest {
            external_ref: Some("a".into()),
        })
        .await
        .unwrap();
    let job_b = client
        .create_job(&CreateJobRequest {
            external_ref: Some("b".into()),
        })
        .await
        .unwrap();

    let shared_leaf = drv_path("sha", "shared-stdenv");
    let leaf_a = drv_path("alf", "a-specific");
    let leaf_b = drv_path("blf", "b-specific");

    let batch_a = IngestBatchRequest {
        drvs: vec![
            ingest(&shared_leaf, "shared-stdenv", &[], false),
            ingest(&leaf_a, "a-specific", &[&shared_leaf], true),
        ],
    };
    client.ingest_batch(job_a.id, &batch_a).await.unwrap();
    let batch_b = IngestBatchRequest {
        drvs: vec![
            ingest(&shared_leaf, "shared-stdenv", &[], false),
            ingest(&leaf_b, "b-specific", &[&shared_leaf], true),
        ],
    };
    let resp_b = client.ingest_batch(job_b.id, &batch_b).await.unwrap();
    // B submitted 2 drvs; shared-stdenv should dedup.
    assert_eq!(resp_b.new_drvs, 1);
    assert_eq!(resp_b.dedup_skipped, 1);

    client.seal(job_a.id).await.unwrap();
    client.seal(job_b.id).await.unwrap();

    // One round of claim-and-complete until both jobs terminal.
    // Track how many distinct drvs were built to prove dedup.
    let mut built: std::collections::HashSet<String> = std::collections::HashSet::new();
    let jobs_active = [job_a.id, job_b.id];
    for iter in 0..40 {
        let mut progress = false;
        for jid in &jobs_active {
            while let Ok(Some(c)) = client.claim(*jid, "x86_64-linux", &[], 1).await {
                progress = true;
                built.insert(c.drv_path.clone());
                eprintln!("[iter={iter}] claimed {} for {jid}", c.drv_path);
                client
                    .complete(
                        *jid,
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
            }
        }
        let sa = client.status(job_a.id).await.unwrap();
        let sb = client.status(job_b.id).await.unwrap();
        eprintln!("[iter={iter}] sa={:?} sb={:?}", sa.status, sb.status);
        if sa.status.is_terminal() && sb.status.is_terminal() {
            assert_eq!(sa.status, JobStatus::Done);
            assert_eq!(sb.status, JobStatus::Done);
            assert_eq!(
                built.len(),
                3,
                "dedup broken: should have built exactly 3 \
                 distinct drvs (shared-stdenv, a-specific, b-specific); \
                 actually built: {:?}",
                built
            );
            return;
        }
        if !progress {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    panic!("jobs didn't reach terminal in time; built={:?}", built);
}

#[sqlx::test]
async fn cross_job_dedup_only_one_worker_builds(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job_a = client
        .create_job(&CreateJobRequest {
            external_ref: Some("a".into()),
        })
        .await
        .unwrap();
    let job_b = client
        .create_job(&CreateJobRequest {
            external_ref: Some("b".into()),
        })
        .await
        .unwrap();

    let shared = drv_path("xyz", "shared-drv");
    client
        .ingest_drv(job_a.id, &ingest(&shared, "shared-drv", &[], true))
        .await
        .unwrap();
    let resp_b = client
        .ingest_drv(job_b.id, &ingest(&shared, "shared-drv", &[], true))
        .await
        .unwrap();
    assert!(resp_b.dedup_skipped, "second submission must dedup");

    client.seal(job_a.id).await.unwrap();
    client.seal(job_b.id).await.unwrap();

    // Two workers race for the same drv. Exactly one wins.
    let c1 = client
        .claim(job_a.id, "x86_64-linux", &[], 3)
        .await
        .unwrap();
    let c2 = client
        .claim(job_b.id, "x86_64-linux", &[], 3)
        .await
        .unwrap();
    let winner = match (c1, c2) {
        (Some(c), None) => (job_a.id, c),
        (None, Some(c)) => (job_b.id, c),
        (Some(_a), Some(_b)) => panic!("both jobs claimed the same drv — dedup broken"),
        (None, None) => panic!("neither job claimed — starvation"),
    };
    client
        .complete(
            winner.0,
            winner.1.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 10,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();

    // Both jobs should transition to done now.
    for _ in 0..40 {
        let a = client.status(job_a.id).await.unwrap();
        let b = client.status(job_b.id).await.unwrap();
        if a.status.is_terminal() && b.status.is_terminal() {
            assert_eq!(a.status, JobStatus::Done);
            assert_eq!(b.status, JobStatus::Done);
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("jobs never terminated");
}
