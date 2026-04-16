//! Fleet-mode tests for `GET /claim` (job-agnostic) + `nix-ci worker`-
//! style worker that pulls work from any live submission.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::{drv_path, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::runner::worker::{self, ClaimMode, WorkerConfig};
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, IngestBatchRequest, IngestDrvRequest, JobStatus,
};
use sqlx::PgPool;
use tokio::sync::watch;

fn ingest(drv: &str, name: &str, is_root: bool) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root,
    }
}

/// FIFO across submissions: when two jobs are live and both have
/// runnable work, the OLDER job's drv must be claimed first.
#[sqlx::test]
async fn claim_any_drains_oldest_job_first(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Job 1 created first.
    let job1 = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv1 = drv_path("fifo1", "first");
    client
        .ingest_drv(job1.id, &ingest(&drv1, "first", true))
        .await
        .unwrap();
    client.seal(job1.id).await.unwrap();

    // Brief gap so created_at differs reliably even on fast systems.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Job 2 created later. Same shape.
    let job2 = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv2 = drv_path("fifo2", "second");
    client
        .ingest_drv(job2.id, &ingest(&drv2, "second", true))
        .await
        .unwrap();
    client.seal(job2.id).await.unwrap();

    // Fleet claim must yield job1's drv first.
    let c = client
        .claim_any("x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(c.job_id, job1.id, "FIFO must drain job1 before job2");
    assert_eq!(c.drv_path, drv1);

    // Complete it; next claim must yield job2's drv.
    client
        .complete(
            c.job_id,
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

    let c2 = client
        .claim_any("x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(c2.job_id, job2.id);
    assert_eq!(c2.drv_path, drv2);
}

/// When the oldest job is "saturated" (all its runnable drvs already
/// claimed by other workers), additional fleet workers naturally fall
/// through to newer jobs — no starvation of small new jobs behind a
/// big old one.
#[sqlx::test]
async fn claim_any_falls_through_to_next_job_when_oldest_is_saturated(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Job 1: ONE root drv (gets claimed first by worker A).
    let job1 = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv1 = drv_path("sat1", "only");
    client
        .ingest_drv(job1.id, &ingest(&drv1, "only", true))
        .await
        .unwrap();
    client.seal(job1.id).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Job 2 (newer): also one root.
    let job2 = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv2 = drv_path("sat2", "next");
    client
        .ingest_drv(job2.id, &ingest(&drv2, "next", true))
        .await
        .unwrap();
    client.seal(job2.id).await.unwrap();

    // Worker A claims job1's only drv.
    let a = client
        .claim_any("x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim A");
    assert_eq!(a.job_id, job1.id);

    // Worker B asks for any work — job1 is saturated, must yield job2.
    let b = client
        .claim_any("x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim B");
    assert_eq!(
        b.job_id, job2.id,
        "worker B must skip the saturated oldest job"
    );
}

/// Fleet long-poll respects its wait deadline and returns 204 (None)
/// — never 410 — when no work is claimable.
#[sqlx::test]
async fn claim_any_returns_204_after_wait_when_nothing_claimable(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    // No jobs at all.
    let start = std::time::Instant::now();
    let res = client.claim_any("x86_64-linux", &[], 1).await.unwrap();
    let elapsed = start.elapsed();
    assert!(res.is_none(), "no work → None");
    assert!(elapsed >= Duration::from_millis(900));
    assert!(elapsed < Duration::from_secs(4));
    drop(handle);
}

/// A fleet worker drains every job's work concurrently. Two jobs ×
/// many drvs each, one worker with max_parallel=4 (dry-run) — both
/// jobs reach Done.
#[sqlx::test]
async fn fleet_worker_drains_multiple_jobs_concurrently(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));

    // Two jobs, 6 root drvs each.
    let mut job_ids = Vec::new();
    for j in 0..2u32 {
        let job = client
            .create_job(&CreateJobRequest { external_ref: None })
            .await
            .unwrap();
        let drvs: Vec<IngestDrvRequest> = (0..6)
            .map(|i| {
                ingest(
                    &drv_path(&format!("flj{j}-{i}"), &format!("d{j}{i}")),
                    &format!("d{j}{i}"),
                    true,
                )
            })
            .collect();
        client
            .ingest_batch(job.id, &IngestBatchRequest { drvs })
            .await
            .unwrap();
        client.seal(job.id).await.unwrap();
        job_ids.push(job.id);
    }

    // Spin up a fleet worker. It should drain BOTH jobs without being
    // bound to either.
    let (sd_tx, sd_rx) = watch::channel(false);
    let worker_handle = {
        let client = client.clone();
        let cfg = WorkerConfig {
            mode: ClaimMode::Fleet,
            system: "x86_64-linux".into(),
            supported_features: vec![],
            max_parallel: 4,
            dry_run: true,
        };
        tokio::spawn(async move { worker::run(client, cfg, sd_rx).await })
    };

    // Wait for both jobs terminal.
    for jid in &job_ids {
        let mut s = client.status(*jid).await.unwrap();
        for _ in 0..200 {
            if s.status.is_terminal() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            s = client.status(*jid).await.unwrap();
        }
        assert_eq!(s.status, JobStatus::Done, "job {jid} did not finish");
    }

    // Stop the worker. Fleet workers don't self-exit, so we drive shutdown.
    let _ = sd_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
}

/// A fleet worker keeps running across job lifetimes — it picks up
/// job 2's work after job 1 completes, never exiting on its own.
/// Guards against a regression where a fleet worker mistakenly
/// short-circuits on Gone.
#[sqlx::test]
async fn fleet_worker_outlives_individual_jobs(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));

    let (sd_tx, sd_rx) = watch::channel(false);
    let worker_handle = {
        let client = client.clone();
        let cfg = WorkerConfig {
            mode: ClaimMode::Fleet,
            system: "x86_64-linux".into(),
            supported_features: vec![],
            max_parallel: 1,
            dry_run: true,
        };
        tokio::spawn(async move { worker::run(client, cfg, sd_rx).await })
    };

    // Submit + wait for job 1 to finish.
    let job1 = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    client
        .ingest_drv(job1.id, &ingest(&drv_path("life1", "x"), "x", true))
        .await
        .unwrap();
    client.seal(job1.id).await.unwrap();
    for _ in 0..200 {
        if client.status(job1.id).await.unwrap().status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        client.status(job1.id).await.unwrap().status,
        JobStatus::Done
    );

    // Worker should STILL be alive — the fleet worker doesn't exit when
    // an individual job terminates.
    assert!(
        !worker_handle.is_finished(),
        "fleet worker exited prematurely"
    );

    // Now submit job 2 — same worker should pick it up.
    let job2 = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    client
        .ingest_drv(job2.id, &ingest(&drv_path("life2", "y"), "y", true))
        .await
        .unwrap();
    client.seal(job2.id).await.unwrap();
    for _ in 0..200 {
        if client.status(job2.id).await.unwrap().status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        client.status(job2.id).await.unwrap().status,
        JobStatus::Done
    );

    // Clean shutdown.
    let _ = sd_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
}

/// Multiple fleet workers racing for the same job's drvs: every drv
/// must be built exactly once (the existing CAS-on-runnable invariant
/// extends to claim_any).
#[sqlx::test]
async fn many_fleet_workers_one_job_exactly_once_each(pool: PgPool) {
    use parking_lot::Mutex;
    use std::collections::HashSet;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let n_drvs = 30;
    let drvs: Vec<IngestDrvRequest> = (0..n_drvs)
        .map(|i| {
            ingest(
                &drv_path(&format!("fl{i:03}"), &format!("d{i}")),
                &format!("d{i}"),
                true,
            )
        })
        .collect();
    client
        .ingest_batch(job.id, &IngestBatchRequest { drvs })
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let built = Arc::new(Mutex::new(HashSet::<String>::new()));
    let dupes = Arc::new(AtomicU32::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8u32 {
        let client = client.clone();
        let built = built.clone();
        let dupes = dupes.clone();
        tasks.spawn(async move {
            loop {
                let Some(c) = client
                    .claim_any("x86_64-linux", &[], 1)
                    .await
                    .unwrap_or(None)
                else {
                    return;
                };
                if !built.lock().insert(c.drv_path.clone()) {
                    dupes.fetch_add(1, Ordering::Relaxed);
                }
                client
                    .complete(
                        c.job_id,
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
        });
    }
    while tasks.join_next().await.is_some() {}

    assert_eq!(dupes.load(Ordering::Relaxed), 0);
    assert_eq!(built.lock().len(), n_drvs as usize);
    let s = client.status(job.id).await.unwrap();
    assert_eq!(s.status, JobStatus::Done);
}
