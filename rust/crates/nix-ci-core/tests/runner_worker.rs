//! Integration test for `runner::worker` — drives a real coordinator
//! with a worker in dry-run mode (no nix subprocess). Exercises the
//! claim/complete loop, retry-budget behavior, and shutdown drain.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::spawn_server;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::runner::worker::{self, ClaimMode, WorkerConfig};
use nix_ci_core::types::{CreateJobRequest, IngestBatchRequest, IngestDrvRequest, JobStatus};
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
        attr: None,
    }
}

/// Worker in dry_run mode drains an entire job's ready queue, marking
/// every drv complete without spawning nix. End-to-end proof that the
/// claim + complete + shutdown-drain loop works against a real server.
#[sqlx::test]
async fn worker_dryrun_drains_all_drvs_to_done(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let drvs: Vec<IngestDrvRequest> = (0..8)
        .map(|i| {
            let p = format!("/nix/store/wdr{i:03}-drv{i}.drv");
            ingest(&p, &format!("drv{i}"), true)
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

    let (tx, rx) = watch::channel(false);
    let worker_handle = {
        let client = client.clone();
        let cfg = WorkerConfig {
            mode: ClaimMode::Job(job.id),
            system: "x86_64-linux".into(),
            supported_features: vec![],
            max_parallel: 4,
            dry_run: true,
            worker_id: None,
            tuning: nix_ci_core::runner::worker::WorkerTuning::default(),
            nix_options: vec![],
        };
        tokio::spawn(async move { worker::run(client, cfg, rx).await })
    };

    // Wait for job to reach terminal.
    let status = loop {
        let s = client.status(job.id).await.unwrap();
        if s.status.is_terminal() {
            break s;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(status.status, JobStatus::Done);
    assert_eq!(status.counts.done, 8);

    // Tell the worker to stop and confirm it exits cleanly.
    let _ = tx.send(true);
    let exited = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    assert!(
        exited.is_ok(),
        "worker must exit promptly on shutdown (no drain deadlock)"
    );
}

/// A shutdown signal raised while the worker is mid-claim long-poll
/// must abort the poll within a bounded time rather than waiting to
/// the long-poll deadline.
#[sqlx::test]
async fn worker_shutdown_mid_longpoll_returns_quickly(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // Don't seal, don't ingest — worker will sit in a long-poll.

    let (tx, rx) = watch::channel(false);
    let worker_handle = {
        let client = client.clone();
        let cfg = WorkerConfig {
            mode: ClaimMode::Job(job.id),
            system: "x86_64-linux".into(),
            supported_features: vec![],
            max_parallel: 1,
            dry_run: true,
            worker_id: None,
            tuning: nix_ci_core::runner::worker::WorkerTuning::default(),
            nix_options: vec![],
        };
        tokio::spawn(async move { worker::run(client, cfg, rx).await })
    };

    // Let the worker enter its long-poll, then signal shutdown.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let start = std::time::Instant::now();
    let _ = tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(3), worker_handle)
        .await
        .expect("worker must unblock on shutdown");
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "shutdown-during-longpoll must be prompt, took {:?}",
        start.elapsed()
    );
}

/// If the job is cancelled out from under the worker (coordinator
/// returns 410 Gone), the worker exits cleanly rather than spinning.
#[sqlx::test]
async fn worker_exits_on_410_gone(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Spawn the worker. The job has no drvs so a seal immediately
    // transitions it to Done — next claim returns Gone, worker exits.
    let (_tx, rx) = watch::channel(false);
    let client_c = client.clone();
    let worker_handle = tokio::spawn(async move {
        worker::run(
            client_c,
            WorkerConfig {
                mode: ClaimMode::Job(job.id),
                system: "x86_64-linux".into(),
                supported_features: vec![],
                max_parallel: 1,
                dry_run: true,
                worker_id: None,
                tuning: nix_ci_core::runner::worker::WorkerTuning::default(),
                nix_options: vec![],
            },
            rx,
        )
        .await
    });

    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle)
        .await
        .expect("worker must exit on 410 Gone");
}
