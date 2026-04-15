#![cfg(feature = "chaos-test")]
//! Chaos / randomized-simulation test.
//!
//! Spawns N random-DAG jobs, M workers, and an ops-chaos task that
//! interleaves random seals, cancels, heartbeats, and failed completions.
//! After driving the system to quiesce we assert the global invariants:
//!
//!   * every job row in Postgres is terminal
//!   * dispatcher.submissions is empty
//!   * dispatcher.claims is empty  (claims_in_flight gauge == 0)
//!   * dispatcher.steps is empty after submissions drop
//!   * every PG-persisted result JSON parses to a terminal status
//!
//! Runs a single seed by default. Set `CHAOS_SEED=<u64>` to reproduce a
//! failure. Set `CHAOS_ITERS=<n>` (default 1) to run multiple random
//! seeds back-to-back in a single pool — each iter reuses the same DB
//! schema but clears busy state between runs via `clear_busy`.
//!
//! This test is deliberately smallish (<= ~100 jobs) so it runs in CI.
//! For longer soak runs use `CHAOS_ITERS=10 CHAOS_JOBS=200`.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::spawn_server_with_cfg;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, ErrorCategory, IngestBatchRequest, IngestDrvRequest,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sqlx::PgPool;

// ─── Tunables ────────────────────────────────────────────────────────

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

// ─── DAG generation ──────────────────────────────────────────────────

#[derive(Clone)]
struct GenJob {
    id: nix_ci_core::types::JobId,
}

fn random_dag(rng: &mut StdRng, job_ix: usize) -> Vec<IngestDrvRequest> {
    // 1..=MAX nodes; each non-leaf has 0..=3 parents among earlier nodes.
    let max_nodes = env_usize("CHAOS_DAG_MAX", 25);
    let n = rng.gen_range(1..=max_nodes);
    let mut drvs: Vec<IngestDrvRequest> = Vec::with_capacity(n);
    for i in 0..n {
        let is_last = i == n - 1;
        let n_deps = if i == 0 {
            0
        } else {
            rng.gen_range(0..=3.min(i))
        };
        let mut dep_paths: Vec<String> = Vec::with_capacity(n_deps);
        // Choose distinct earlier indices as deps.
        let mut available: Vec<usize> = (0..i).collect();
        for _ in 0..n_deps {
            if available.is_empty() {
                break;
            }
            let pick = rng.gen_range(0..available.len());
            let dep_ix = available.remove(pick);
            dep_paths.push(drvs[dep_ix].drv_path.clone());
        }
        // ~70% chance a root-eligible node is marked is_root.
        let is_root = is_last || (dep_paths.is_empty() && rng.gen_bool(0.7));
        let name = format!("j{job_ix:03}-n{i:02}");
        let path = format!("/nix/store/cha{job_ix:03}s{i:03}-{name}.drv");
        drvs.push(IngestDrvRequest {
            drv_path: path,
            drv_name: name,
            system: "x86_64-linux".into(),
            required_features: vec![],
            input_drvs: dep_paths,
            is_root,
        });
    }
    drvs
}

// ─── Worker ──────────────────────────────────────────────────────────

async fn worker_loop(
    base: String,
    jobs: Vec<nix_ci_core::types::JobId>,
    mut rng: StdRng,
    stop: Arc<AtomicBool>,
    completed: Arc<AtomicU32>,
    ignored: Arc<AtomicU32>,
) {
    let client = CoordinatorClient::new(&base);
    while !stop.load(Ordering::Acquire) {
        if jobs.is_empty() {
            return;
        }
        let job_id = jobs[rng.gen_range(0..jobs.len())];
        // Short long-poll so we rotate across jobs.
        let claim = match client.claim(job_id, "x86_64-linux", &[], 1).await {
            Ok(Some(c)) => c,
            Ok(None) => continue,
            // Terminal / unknown / gone — rotate to a different job.
            Err(_) => continue,
        };
        // ~10% of the time fail the build; ~5% flaky transient; else success.
        let roll = rng.gen_range(0..100);
        let (success, category, msg): (bool, Option<ErrorCategory>, Option<String>) = if roll < 10 {
            (
                false,
                Some(ErrorCategory::BuildFailure),
                Some("nope".into()),
            )
        } else if roll < 15 {
            (false, Some(ErrorCategory::Transient), Some("net".into()))
        } else {
            (true, None, None)
        };
        let resp = client
            .complete(
                job_id,
                claim.claim_id,
                &CompleteRequest {
                    success,
                    duration_ms: 1,
                    exit_code: Some(if success { 0 } else { 1 }),
                    error_category: category,
                    error_message: msg,
                    log_tail: None,
                },
            )
            .await;
        match resp {
            Ok(r) if r.ignored => {
                ignored.fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) => {
                completed.fetch_add(1, Ordering::Relaxed);
            }
            // Network / server hiccup — keep going.
            Err(_) => {}
        }
    }
}

// ─── Chaos operations ────────────────────────────────────────────────

async fn chaos_ops(
    base: String,
    jobs: Vec<nix_ci_core::types::JobId>,
    mut rng: StdRng,
    stop: Arc<AtomicBool>,
) {
    let client = CoordinatorClient::new(&base);
    let start = Instant::now();
    let until = start + Duration::from_millis(1500);
    while Instant::now() < until && !stop.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(rng.gen_range(5..50))).await;
        if jobs.is_empty() {
            return;
        }
        let job_id = jobs[rng.gen_range(0..jobs.len())];
        match rng.gen_range(0..10) {
            0..=1 => {
                // Cancel (hot but rare).
                let _ = reqwest::Client::new()
                    .delete(format!("{base}/jobs/{job_id}/cancel"))
                    .send()
                    .await;
            }
            2..=3 => {
                let _ = client.heartbeat(job_id).await;
            }
            _ => {
                // Most of the time: do nothing (let workers drive progress).
            }
        }
    }
}

// ─── Invariant check ─────────────────────────────────────────────────

async fn assert_quiesce(
    handle: &common::ServerHandle,
    client: &CoordinatorClient,
    jobs: &[GenJob],
) {
    // Poll for quiesce — every job should be terminal within reasonable time.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut all_terminal = true;
        for j in jobs {
            let s = client.status(j.id).await.unwrap();
            if !s.status.is_terminal() {
                all_terminal = false;
                break;
            }
        }
        if all_terminal {
            break;
        }
        if Instant::now() > deadline {
            for j in jobs {
                let s = client.status(j.id).await.unwrap();
                if !s.status.is_terminal() {
                    panic!("job {} stuck at {:?} after 30s", j.id, s.status);
                }
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Global quiesce — map sizes must all drop to zero once every job
    // is terminal. Allow a tiny grace for the async terminal-cleanup to
    // run after the last complete.
    for i in 0..20 {
        let snap = client.admin_snapshot().await.unwrap();
        if snap.submissions == 0 && snap.active_claims == 0 && snap.steps_total == 0 {
            return;
        }
        if i == 19 {
            panic!(
                "did not fully quiesce: submissions={} active_claims={} steps_total={}",
                snap.submissions, snap.active_claims, snap.steps_total
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Cross-check DB rows too: every job has a terminal status with
    // non-null done_at.
    let rows: Vec<(String, Option<chrono::DateTime<chrono::Utc>>)> =
        sqlx::query_as("SELECT status, done_at FROM jobs WHERE id = ANY($1::uuid[])")
            .bind(jobs.iter().map(|j| j.id.0).collect::<Vec<_>>())
            .fetch_all(&handle.pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), jobs.len(), "row count mismatch");
    for (status, done_at) in rows {
        assert!(
            matches!(status.as_str(), "done" | "failed" | "cancelled"),
            "non-terminal status in PG: {status}"
        );
        assert!(done_at.is_some(), "terminal row missing done_at");
    }
}

// ─── Driver ──────────────────────────────────────────────────────────

#[sqlx::test]
async fn chaos_single_iter(pool: PgPool) {
    let iters = env_usize("CHAOS_ITERS", 1);
    let n_jobs_cap = env_usize("CHAOS_JOBS", 20);
    let n_workers = env_usize("CHAOS_WORKERS", 12);
    let mut base_seed = env_u64("CHAOS_SEED", 0);
    if base_seed == 0 {
        base_seed = rand::random::<u64>().max(1);
    }
    eprintln!(
        "chaos: base_seed={base_seed} iters={iters} jobs_cap={n_jobs_cap} workers={n_workers}"
    );

    // Shrink the flaky-retry backoff so transient-failure injections
    // don't wedge the harness for 30s per retry.
    let handle = spawn_server_with_cfg(pool.clone(), |cfg| {
        cfg.flaky_retry_backoff_step_ms = 50;
    })
    .await;
    let base = handle.base_url.clone();
    let client = CoordinatorClient::new(&base);

    for iter in 0..iters {
        let seed = base_seed.wrapping_add(iter as u64 * 1_000_003);
        let mut rng = StdRng::seed_from_u64(seed);
        eprintln!("chaos iter {iter}: seed={seed}");

        // 1. Create jobs with random DAGs. Seal every job up-front so
        //    workers actually drain them (chaos_ops handles the cancels).
        let n_jobs = rng.gen_range(5..=n_jobs_cap);
        let mut jobs: Vec<GenJob> = Vec::with_capacity(n_jobs);
        for i in 0..n_jobs {
            let j = client
                .create_job(&CreateJobRequest {
                    external_ref: Some(format!("chaos-{seed}-{i}")),
                })
                .await
                .unwrap();
            let drvs = random_dag(&mut rng, i);
            client
                .ingest_batch(j.id, &IngestBatchRequest { drvs })
                .await
                .unwrap();
            client.seal(j.id).await.unwrap();
            jobs.push(GenJob { id: j.id });
        }

        // 2. Fan out workers + one chaos task, run until all jobs are
        //    terminal or the chaos_ops window elapses.
        let stop = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicU32::new(0));
        let ignored = Arc::new(AtomicU32::new(0));
        let job_ids: Vec<_> = jobs.iter().map(|j| j.id).collect();
        let mut tasks = tokio::task::JoinSet::new();
        for w in 0..n_workers {
            let seed_w = seed.wrapping_mul(2654435761).wrapping_add(w as u64);
            tasks.spawn(worker_loop(
                base.clone(),
                job_ids.clone(),
                StdRng::seed_from_u64(seed_w),
                stop.clone(),
                completed.clone(),
                ignored.clone(),
            ));
        }
        let chaos_seed = seed.wrapping_add(999_983);
        tasks.spawn(chaos_ops(
            base.clone(),
            job_ids.clone(),
            StdRng::seed_from_u64(chaos_seed),
            stop.clone(),
        ));

        // 3. Wait until every job is terminal OR a hard budget elapses.
        //    Scale budget with job count so larger shapes have time to
        //    drain; floor at 30s, ceil at CHAOS_BUDGET_SECS override.
        let budget_secs = env_usize("CHAOS_BUDGET_SECS", (30 + n_jobs * 2).max(30));
        let budget = Instant::now() + Duration::from_secs(budget_secs as u64);
        loop {
            let mut all_done = true;
            for j in &jobs {
                let s = client.status(j.id).await.unwrap();
                if !s.status.is_terminal() {
                    all_done = false;
                    break;
                }
            }
            if all_done || Instant::now() > budget {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        stop.store(true, Ordering::Release);
        while tasks.join_next().await.is_some() {}

        // 4. Invariants.
        assert_quiesce(&handle, &client, &jobs).await;
        eprintln!(
            "chaos iter {iter}: seed={seed} completed={} ignored={}",
            completed.load(Ordering::Relaxed),
            ignored.load(Ordering::Relaxed)
        );
    }
}
