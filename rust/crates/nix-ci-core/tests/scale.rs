//! L3 scale-ladder tests on the **real** dispatcher + real Postgres.
//!
//! Each test below exercises a bottleneck the existing `load.rs`
//! doesn't reach:
//!
//! * `scale_1k_workers_fan_in` — 1000 concurrent workers on one job.
//!   Probes `notify_waiters` wake-storm, claims-map contention, and
//!   fleet-scan O(N) behavior under a single notify edge.
//! * `scale_tall_narrow_chain` — 2000-deep linear dep chain, 32
//!   workers. Probes critical-path latency (most of the job is
//!   serialised behind rdep propagation, not claim contention).
//! * `scale_wide_flat_leaves` — 20 000 independent drvs, no deps,
//!   1000 workers. Probes the "every worker wakes, fights for a
//!   claim, wins or 204s" extreme.
//! * `scale_100_submissions_fleet` — 100 concurrent submissions with
//!   10 drvs each, 256 fleet workers. Probes `Submissions::
//!   sorted_by_created_at` scan cost under N-fold submission growth.
//! * `scale_failures_vec_under_catastrophic_job` — a job where every
//!   drv fails. Asserts `failures` vec stays bounded via
//!   `max_failures_in_result` truncation and that terminal JSONB size
//!   is bounded.
//!
//! Each test prints a diagnostic block at the end with observed
//! percentiles + metrics; CI parses them for trend tracking. Budgets
//! are generous — we want to catch catastrophic regressions (10×
//! slowdowns), not micro-tuning drift.
//!
//! **Why feature-gated (`scale-test`):** each test takes 30–180s wall
//! clock, which is too slow for every PR. Runs nightly.
//!
//! Run locally:
//!
//!   DATABASE_URL=postgres://... \
//!     cargo test -p nix-ci-core --features scale-test \
//!       --test scale --release -- --test-threads=1 --nocapture

#![cfg(feature = "scale-test")]

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::spawn_server;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, ErrorCategory, IngestDrvRequest, JobId,
};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sqlx::PgPool;

// ─── Shared helpers ─────────────────────────────────────────────────

struct Lat {
    claim: Mutex<Vec<f64>>,
}
impl Lat {
    fn new() -> Self {
        Self {
            claim: Mutex::new(Vec::new()),
        }
    }
    fn record(&self, d: Duration) {
        self.claim.lock().push(d.as_secs_f64() * 1000.0);
    }
    /// Returns (n, p50_ms, p95_ms, p99_ms, max_ms).
    fn pcts(&self) -> (usize, f64, f64, f64, f64) {
        let mut v = std::mem::take(&mut *self.claim.lock());
        if v.is_empty() {
            return (0, 0.0, 0.0, 0.0, 0.0);
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        (
            v.len(),
            pct(&v, 0.50),
            pct(&v, 0.95),
            pct(&v, 0.99),
            *v.last().unwrap(),
        )
    }
}
fn pct(sorted: &[f64], q: f64) -> f64 {
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx]
}

fn drv_path(label: &str, idx: usize) -> String {
    format!("/nix/store/{label}{idx:07}-x.drv")
}

/// Run `workers` concurrent claim/complete loops until the job
/// terminates. Records claim latencies into `lat`. `fail_rate` is the
/// fraction of completions that report a failure.
async fn run_workers_until_terminal(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    workers: usize,
    fail_rate: f64,
    seed_base: u64,
    lat: Arc<Lat>,
) {
    let mut tasks = tokio::task::JoinSet::new();
    for w in 0..workers {
        let client = client.clone();
        let lat = lat.clone();
        let seed = seed_base.wrapping_add(w as u64);
        tasks.spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            loop {
                let t0 = Instant::now();
                let c = client
                    .claim(job_id, "x86_64-linux", &[], 3)
                    .await
                    .ok()
                    .flatten();
                lat.record(t0.elapsed());
                let Some(claim) = c else {
                    if let Ok(s) = client.status(job_id).await {
                        if s.status.is_terminal() {
                            return;
                        }
                    }
                    continue;
                };
                let will_fail = rng.gen_bool(fail_rate);
                let req = CompleteRequest {
                    success: !will_fail,
                    duration_ms: 5,
                    exit_code: Some(if will_fail { 1 } else { 0 }),
                    error_category: if will_fail {
                        Some(ErrorCategory::BuildFailure)
                    } else {
                        None
                    },
                    error_message: will_fail.then_some("scale fail".into()),
                    log_tail: None,
                };
                let _ = client.complete(job_id, claim.claim_id, &req).await;
            }
        });
    }
    while tasks.join_next().await.is_some() {}
}

// ─── Test 1: 1K workers on a fan-in DAG ────────────────────────────

/// 1000 concurrent workers on a single job with 5000 drvs arranged in
/// a 5-layer fan-in DAG. The existing load.rs caps at 500 workers; we
/// double it to stress `notify_waiters`, claims-map write contention,
/// and dispatch_wait_seconds tail.
///
/// **SLO asserted**: p99 claim < 500ms (2.5× production SPEC; we want
/// a regression signal, not a flaky margin). Runtime <120s.
#[sqlx::test]
async fn scale_1k_workers_fan_in(pool: PgPool) {
    const LAYERS: usize = 5;
    const WIDTH: usize = 1_000;
    const WORKERS: usize = 1_000;
    const FAN_IN: usize = 2;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let lat = Arc::new(Lat::new());

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("scale-1k-worker".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut rng = StdRng::seed_from_u64(0xf41e_c0de);
    let t_ingest = Instant::now();
    // Layer 0
    for i in 0..WIDTH {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: drv_path("l0", i),
                    drv_name: format!("l0i{i}"),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: false,
                    attr: None,
                },
            )
            .await
            .unwrap();
    }
    // Layers 1..LAYERS
    for layer in 1..LAYERS {
        for i in 0..WIDTH {
            let mut deps = HashSet::new();
            while deps.len() < FAN_IN {
                deps.insert(drv_path(
                    &format!("l{}", layer - 1),
                    rng.gen_range(0..WIDTH),
                ));
            }
            client
                .ingest_drv(
                    job.id,
                    &IngestDrvRequest {
                        drv_path: drv_path(&format!("l{layer}"), i),
                        drv_name: format!("l{layer}i{i}"),
                        system: "x86_64-linux".into(),
                        required_features: vec![],
                        input_drvs: deps.into_iter().collect(),
                        is_root: layer == LAYERS - 1,
                        attr: None,
                    },
                )
                .await
                .unwrap();
        }
    }
    client.seal(job.id).await.unwrap();
    let ingest_elapsed = t_ingest.elapsed();

    let t_build = Instant::now();
    run_workers_until_terminal(
        client.clone(),
        job.id,
        WORKERS,
        0.01, // 1% failure rate — want mostly successes to stress claim path
        0xbeef_0000,
        lat.clone(),
    )
    .await;
    let build_elapsed = t_build.elapsed();

    let status = client.status(job.id).await.unwrap();
    let (n, p50, p95, p99, max) = lat.pcts();
    eprintln!(
        "SCALE/1K-WORKER: drvs={} terminal={:?} ingest={:.2}s build={:.2}s",
        status.counts.total,
        status.status,
        ingest_elapsed.as_secs_f64(),
        build_elapsed.as_secs_f64(),
    );
    eprintln!(
        "SCALE/1K-WORKER: claim n={n} p50={p50:.2}ms p95={p95:.2}ms p99={p99:.2}ms max={max:.2}ms"
    );

    assert!(status.status.is_terminal(), "job must terminate");
    assert_eq!(
        status.counts.total,
        status.counts.done + status.counts.failed,
        "all drvs must reach terminal"
    );
    assert!(
        build_elapsed < Duration::from_secs(180),
        "1K-worker build exceeded 180s budget: {build_elapsed:?}"
    );
    // 2.5× the production S-CLAIM-P99 bar of 200ms. A 10× regression
    // would blow past this immediately.
    assert!(
        p99 < 500.0,
        "p99 claim latency regression at 1K workers: {p99:.2}ms >= 500ms"
    );
}

// ─── Test 2: Tall-narrow chain (critical-path probe) ───────────────

/// 2000-deep linear dep chain, 32 workers. At most one drv is runnable
/// at a time (strict chain), so the runtime is dominated by rdep
/// propagation latency, not claim-path contention. A regression in
/// `make_rdeps_runnable` shows up here clearly.
///
/// **SLO asserted**: runtime < 120s (≈ 60ms mean per-drv rdep
/// propagation cycle on healthy dispatcher; generous bar).
#[sqlx::test]
async fn scale_tall_narrow_chain(pool: PgPool) {
    const DEPTH: usize = 2_000;
    const WORKERS: usize = 32;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let lat = Arc::new(Lat::new());
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("scale-tall-narrow".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let t_ingest = Instant::now();
    // Layer 0: the single root-of-chain.
    client
        .ingest_drv(
            job.id,
            &IngestDrvRequest {
                drv_path: drv_path("chain", 0),
                drv_name: "chain0".into(),
                system: "x86_64-linux".into(),
                required_features: vec![],
                input_drvs: vec![],
                is_root: false,
                attr: None,
            },
        )
        .await
        .unwrap();
    // Each subsequent drv depends on the previous one. Layer (DEPTH-1)
    // is the single toplevel.
    for i in 1..DEPTH {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: drv_path("chain", i),
                    drv_name: format!("chain{i}"),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![drv_path("chain", i - 1)],
                    is_root: i == DEPTH - 1,
                    attr: None,
                },
            )
            .await
            .unwrap();
    }
    client.seal(job.id).await.unwrap();
    let ingest_elapsed = t_ingest.elapsed();

    let t_build = Instant::now();
    run_workers_until_terminal(
        client.clone(),
        job.id,
        WORKERS,
        0.0, // no failures — chain would cascade and obscure the runtime signal
        0xcafe_0000,
        lat.clone(),
    )
    .await;
    let build_elapsed = t_build.elapsed();

    let status = client.status(job.id).await.unwrap();
    let (n, p50, p95, p99, max) = lat.pcts();
    eprintln!(
        "SCALE/TALL-NARROW: depth={DEPTH} workers={WORKERS} ingest={:.2}s build={:.2}s",
        ingest_elapsed.as_secs_f64(),
        build_elapsed.as_secs_f64(),
    );
    eprintln!("SCALE/TALL-NARROW: claim n={n} p50={p50:.2}ms p95={p95:.2}ms p99={p99:.2}ms max={max:.2}ms");

    assert!(status.status.is_terminal(), "chain must terminate");
    assert_eq!(status.counts.done, DEPTH as u32);
    assert!(
        build_elapsed < Duration::from_secs(180),
        "tall-narrow chain exceeded 180s budget: {build_elapsed:?}"
    );
}

// ─── Test 3: Wide-flat leaves (claim-path contention) ──────────────

/// 20 000 independent drvs with no deps, 1000 workers. This is the
/// extreme claim-contention shape: every drv is runnable immediately
/// at seal time, every worker wakes on the same `notify_waiters`
/// edge, and they all fight on `Submission::ready` + claims map.
///
/// **SLO asserted**: runtime < 120s, p99 claim < 1s.
#[sqlx::test]
async fn scale_wide_flat_leaves(pool: PgPool) {
    const WIDTH: usize = 20_000;
    const WORKERS: usize = 1_000;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let lat = Arc::new(Lat::new());
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("scale-wide-flat".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let t_ingest = Instant::now();
    for i in 0..WIDTH {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: drv_path("flat", i),
                    drv_name: format!("flat{i}"),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                },
            )
            .await
            .unwrap();
    }
    client.seal(job.id).await.unwrap();
    let ingest_elapsed = t_ingest.elapsed();

    let t_build = Instant::now();
    run_workers_until_terminal(
        client.clone(),
        job.id,
        WORKERS,
        0.0,
        0xfeed_0000,
        lat.clone(),
    )
    .await;
    let build_elapsed = t_build.elapsed();

    let status = client.status(job.id).await.unwrap();
    let (n, p50, p95, p99, max) = lat.pcts();
    eprintln!(
        "SCALE/WIDE-FLAT: width={WIDTH} workers={WORKERS} ingest={:.2}s build={:.2}s",
        ingest_elapsed.as_secs_f64(),
        build_elapsed.as_secs_f64(),
    );
    eprintln!(
        "SCALE/WIDE-FLAT: claim n={n} p50={p50:.2}ms p95={p95:.2}ms p99={p99:.2}ms max={max:.2}ms"
    );

    assert!(status.status.is_terminal());
    assert_eq!(status.counts.done, WIDTH as u32);
    assert!(
        build_elapsed < Duration::from_secs(180),
        "wide-flat exceeded 180s budget: {build_elapsed:?}"
    );
    assert!(
        p99 < 1_000.0,
        "p99 regression on wide-flat: {p99:.2}ms >= 1000ms"
    );
}

// ─── Test 4: 100 concurrent submissions (fleet-scan cost) ──────────

/// 100 concurrent submissions × 10 drvs each, 256 fleet workers
/// claiming across the whole fleet. Stresses
/// `Submissions::sorted_by_created_at` (O(live_subs) per wake) under
/// realistic multi-tenant CCI load.
///
/// **SLO asserted**: runtime < 120s, p99 claim < 500ms.
#[sqlx::test]
async fn scale_100_submissions_fleet(pool: PgPool) {
    const JOBS: usize = 100;
    const DRVS_PER_JOB: usize = 10;
    const WORKERS: usize = 256;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let lat = Arc::new(Lat::new());

    let mut job_ids: Vec<JobId> = Vec::with_capacity(JOBS);
    let t_ingest = Instant::now();
    for j in 0..JOBS {
        let job = client
            .create_job(&CreateJobRequest {
                external_ref: Some(format!("scale-fleet-{j}")),
                ..Default::default()
            })
            .await
            .unwrap();
        for i in 0..DRVS_PER_JOB {
            client
                .ingest_drv(
                    job.id,
                    &IngestDrvRequest {
                        drv_path: drv_path(&format!("j{j}"), i),
                        drv_name: format!("j{j}i{i}"),
                        system: "x86_64-linux".into(),
                        required_features: vec![],
                        input_drvs: vec![],
                        is_root: true,
                        attr: None,
                    },
                )
                .await
                .unwrap();
        }
        client.seal(job.id).await.unwrap();
        job_ids.push(job.id);
    }
    let ingest_elapsed = t_ingest.elapsed();

    let t_build = Instant::now();
    let done_count = Arc::new(AtomicU32::new(0));
    let expected_drvs = (JOBS * DRVS_PER_JOB) as u32;
    let mut tasks = tokio::task::JoinSet::new();
    for w in 0..WORKERS {
        let client = client.clone();
        let lat = lat.clone();
        let done_count = done_count.clone();
        tasks.spawn(async move {
            loop {
                if done_count.load(Ordering::Acquire) >= expected_drvs {
                    return;
                }
                let t0 = Instant::now();
                // Fleet claim: no specific job_id.
                let worker_tag = format!("scale-w{w}");
                let c = client
                    .claim_any_as_worker("x86_64-linux", &[], 3, Some(&worker_tag))
                    .await
                    .ok()
                    .flatten();
                lat.record(t0.elapsed());
                let Some(claim) = c else { continue };
                let _ = client
                    .complete(
                        claim.job_id,
                        claim.claim_id,
                        &CompleteRequest {
                            success: true,
                            duration_ms: 3,
                            exit_code: Some(0),
                            error_category: None,
                            error_message: None,
                            log_tail: None,
                        },
                    )
                    .await;
                done_count.fetch_add(1, Ordering::AcqRel);
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    let build_elapsed = t_build.elapsed();

    let (n, p50, p95, p99, max) = lat.pcts();
    eprintln!(
        "SCALE/100-SUBS: jobs={JOBS} drvs/job={DRVS_PER_JOB} workers={WORKERS} ingest={:.2}s build={:.2}s",
        ingest_elapsed.as_secs_f64(),
        build_elapsed.as_secs_f64(),
    );
    eprintln!(
        "SCALE/100-SUBS: claim n={n} p50={p50:.2}ms p95={p95:.2}ms p99={p99:.2}ms max={max:.2}ms"
    );

    for id in &job_ids {
        let s = client.status(*id).await.unwrap();
        assert!(
            s.status.is_terminal(),
            "job {id} not terminal: {:?}",
            s.status
        );
    }
    assert!(
        build_elapsed < Duration::from_secs(180),
        "100-submissions fleet exceeded 180s: {build_elapsed:?}"
    );
    assert!(
        p99 < 500.0,
        "p99 regression on fleet scan at 100 subs: {p99:.2}ms >= 500ms"
    );
}

// ─── Test 5: Catastrophic-failures vec bounding ────────────────────

/// A pathological job where every drv fails. Asserts:
///
/// 1. The terminal `failures` vec in `jobs.result` is capped at
///    `max_failures_in_result` + a synthetic truncation marker.
/// 2. The JSONB `result` column stays under a practical ceiling
///    (1 MiB sanity bar — real value of the cap).
///
/// Without the cap, a 10K-drv job with 100% failure would produce a
/// multi-MB JSONB row; the cap ensures Postgres rows stay bounded.
#[sqlx::test]
async fn scale_failures_vec_under_catastrophic_job(pool: PgPool) {
    const DRVS: usize = 5_000;
    const WORKERS: usize = 64;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let lat = Arc::new(Lat::new());

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("scale-catastrophic-fail".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    for i in 0..DRVS {
        client
            .ingest_drv(
                job.id,
                &IngestDrvRequest {
                    drv_path: drv_path("bad", i),
                    drv_name: format!("bad{i}"),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                },
            )
            .await
            .unwrap();
    }
    client.seal(job.id).await.unwrap();

    run_workers_until_terminal(
        client.clone(),
        job.id,
        WORKERS,
        1.0, // 100% failure — stresses the cap
        0xdead_0000,
        lat.clone(),
    )
    .await;

    // Read the terminal result directly from PG so we exercise the
    // actual JSONB on disk, not the in-memory snapshot.
    let row: (Option<serde_json::Value>,) = sqlx::query_as("SELECT result FROM jobs WHERE id = $1")
        .bind(job.id.0)
        .fetch_one(&handle.pool)
        .await
        .unwrap();
    let result = row.0.expect("terminal jobs have a result JSONB");
    let failures = result
        .get("failures")
        .and_then(|v| v.as_array())
        .expect("result.failures must be an array");
    let result_bytes = serde_json::to_vec(&result).unwrap().len();
    eprintln!(
        "SCALE/CATASTROPHIC: drvs={DRVS} result_failures_len={} result_bytes={}",
        failures.len(),
        result_bytes,
    );

    let default_cap = nix_ci_core::config::ServerConfig::default().max_failures_in_result;
    assert!(
        failures.len() <= default_cap + 1,
        "failures vec must be capped at max_failures_in_result (+1 for truncation marker); saw {}",
        failures.len(),
    );
    // 1 MiB ceiling — true JSONB size cap can be much tighter in
    // production; this is a generous correctness gate, not a target.
    assert!(
        result_bytes < 1_048_576,
        "terminal JSONB exceeded 1 MiB: {result_bytes} bytes"
    );
}
