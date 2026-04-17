//! Concurrency stress tests. Workers race on the same drvs; asserts
//! exactly-once build semantics and invariant consistency under load.

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::{drv_path, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, IngestBatchRequest, IngestDrvRequest, JobStatus,
};
use parking_lot::Mutex;
use sqlx::PgPool;

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

/// Many concurrent workers race on a single job's ready steps. Every
/// drv must be built exactly once — no drv shows up in two workers'
/// claim logs.
#[sqlx::test]
async fn many_workers_single_job_exactly_once(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let base = handle.base_url.clone();
    let client = CoordinatorClient::new(&base);

    let n_drvs = 100;
    let n_workers = 32;
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // Flat 100-drv batch — all leaves (no deps).
    let drvs: Vec<IngestDrvRequest> = (0..n_drvs)
        .map(|i| {
            let p = drv_path(&format!("c{i:04}"), &format!("leaf-{i}"));
            ingest(&p, &format!("leaf-{i}"), &[], true)
        })
        .collect();
    client
        .ingest_batch(job.id, &IngestBatchRequest { drvs })
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let built = Arc::new(Mutex::new(HashSet::<String>::new()));
    let double_builds = Arc::new(AtomicU32::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..n_workers {
        let client = CoordinatorClient::new(&base);
        let job_id = job.id;
        let built = built.clone();
        let double_builds = double_builds.clone();
        tasks.spawn(async move {
            loop {
                let c = client.claim(job_id, "x86_64-linux", &[], 2).await.unwrap();
                let Some(c) = c else {
                    return;
                };
                let was_new = built.lock().insert(c.drv_path.clone());
                if !was_new {
                    double_builds.fetch_add(1, Ordering::Relaxed);
                }
                client
                    .complete(
                        job_id,
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

    assert_eq!(
        double_builds.load(Ordering::Relaxed),
        0,
        "some drv was claimed by two workers"
    );
    assert_eq!(built.lock().len(), n_drvs);
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Done);
    assert_eq!(status.counts.done, n_drvs as u32);
}

/// N concurrent jobs all submit the SAME drv. Only one worker (across
/// the whole fleet) should build it.
#[sqlx::test]
async fn cross_job_contention_builds_shared_drv_once(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let base = handle.base_url.clone();
    let n_jobs = 8;
    let shared = drv_path("sha", "global-pkg");

    // Create all jobs first, submit shared drv to each, then seal.
    let client = CoordinatorClient::new(&base);
    let mut job_ids = Vec::with_capacity(n_jobs);
    for i in 0..n_jobs {
        let job = client
            .create_job(&CreateJobRequest {
                external_ref: Some(format!("job-{i}")),
                ..Default::default()
            })
            .await
            .unwrap();
        client
            .ingest_drv(job.id, &ingest(&shared, "global-pkg", &[], true))
            .await
            .unwrap();
        client.seal(job.id).await.unwrap();
        job_ids.push(job.id);
    }

    // Each job runs a worker racing for claims.
    let builds = Arc::new(AtomicU32::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for jid in &job_ids {
        let client = CoordinatorClient::new(&base);
        let jid = *jid;
        let builds = builds.clone();
        tasks.spawn(async move {
            loop {
                let c = client.claim(jid, "x86_64-linux", &[], 2).await.unwrap();
                let Some(c) = c else {
                    return;
                };
                builds.fetch_add(1, Ordering::Relaxed);
                client
                    .complete(
                        jid,
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

    assert_eq!(
        builds.load(Ordering::Relaxed),
        1,
        "shared drv must be built exactly once across all jobs"
    );

    // All jobs terminal-Done.
    for jid in &job_ids {
        let s = client.status(*jid).await.unwrap();
        assert_eq!(s.status, JobStatus::Done, "job {jid} not done");
    }
}

/// Ingest fan-in: many concurrent ingest calls against the same drv
/// from the same job. Steps::create dedupes — only one Step exists at
/// the end.
#[sqlx::test]
async fn concurrent_ingest_same_drv_single_step(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let base = handle.base_url.clone();
    let client = CoordinatorClient::new(&base);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let drv = drv_path("dup", "duplicate");
    let req = ingest(&drv, "duplicate", &[], true);

    let n = 20;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..n {
        let client = CoordinatorClient::new(&base);
        let job_id = job.id;
        let req = req.clone();
        tasks.spawn(async move { client.ingest_drv(job_id, &req).await });
    }
    let mut new_count = 0;
    while let Some(res) = tasks.join_next().await {
        let resp = res.unwrap().unwrap();
        if resp.new_drv {
            new_count += 1;
        }
    }
    // Exactly one ingest should have observed is_new=true.
    assert_eq!(
        new_count, 1,
        "concurrent ingests must dedupe: one observed new, {n} total"
    );

    let snap: nix_ci_core::types::AdminSnapshot = reqwest::get(format!("{base}/admin/snapshot"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(snap.steps_total, 1);
}

/// Wide fan-out with workers: many leaves under one root. Workers
/// race for leaves; when the last leaf completes, root becomes
/// runnable.
#[sqlx::test]
async fn wide_fan_out_sequences_correctly_under_load(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let base = handle.base_url.clone();
    let client = CoordinatorClient::new(&base);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let n_leaves = 50;
    let mut drvs: Vec<IngestDrvRequest> = Vec::with_capacity(n_leaves + 1);
    let leaf_paths: Vec<String> = (0..n_leaves)
        .map(|i| drv_path(&format!("l{i:03}"), &format!("leaf-{i}")))
        .collect();
    for (i, p) in leaf_paths.iter().enumerate() {
        drvs.push(ingest(p, &format!("leaf-{i}"), &[], false));
    }
    let root = drv_path("root", "final");
    let root_deps: Vec<&str> = leaf_paths.iter().map(String::as_str).collect();
    drvs.push(ingest(&root, "final", &root_deps, true));

    client
        .ingest_batch(job.id, &IngestBatchRequest { drvs })
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Track ordering: root should be last.
    let order = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let client = CoordinatorClient::new(&base);
        let job_id = job.id;
        let order = order.clone();
        tasks.spawn(async move {
            loop {
                let c = client.claim(job_id, "x86_64-linux", &[], 2).await.unwrap();
                let Some(c) = c else {
                    return;
                };
                order.lock().push(c.drv_path.clone());
                client
                    .complete(
                        job_id,
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

    // Timeout-safe wait for terminal.
    let mut status = client.status(job.id).await.unwrap();
    for _ in 0..20 {
        if status.status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        status = client.status(job.id).await.unwrap();
    }
    assert_eq!(status.status, JobStatus::Done);

    // The root must have been built AFTER every leaf.
    let order = order.lock();
    let root_idx = order
        .iter()
        .position(|p| p == &root)
        .expect("root was built");
    // Every leaf must appear before the root.
    for (i, entry) in order.iter().enumerate().take(root_idx) {
        assert!(
            leaf_paths.iter().any(|lp| lp == entry),
            "non-leaf built before root at index {i}: {entry}"
        );
    }
    // Every leaf must appear exactly once.
    let mut leaf_counts: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    for p in order.iter() {
        *leaf_counts.entry(p.clone()).or_insert(0) += 1;
    }
    for leaf in &leaf_paths {
        assert_eq!(leaf_counts.get(leaf).copied().unwrap_or(0), 1);
    }
}
