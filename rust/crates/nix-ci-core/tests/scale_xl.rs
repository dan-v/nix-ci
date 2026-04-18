//! Scale-finder tests. Unlike `scale.rs` (which asserts SLO budgets and
//! runs in ≤180s), these tests push past the known working envelope to
//! find the first dominant bottleneck in the single-writer, in-memory
//! dispatcher. They emit a diagnostic report and intentionally do NOT
//! assert SLOs — the point is to discover where things break, then
//! decide what to fix based on evidence.
//!
//! Gated behind `scale-test` (same as `scale.rs`). Iterate one scenario
//! at a time:
//!
//!   DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
//!     cargo test -p nix-ci-core --features scale-test \
//!       --test scale_xl --release -- xl_a_deep_wide --test-threads=1 --nocapture
//!
//! Fixes the known `scale.rs` artifact where `lat.record(t0.elapsed())`
//! is called even when the claim returned `None` (long-poll timed out),
//! which polluted p99 with the long-poll wait duration. Here we record
//! SUCCESSFUL claims and EMPTY claims into separate histograms so the
//! "claim hot path" number is clean.
//!
//! Scenarios:
//!
//! - `xl_a_deep_wide` — 100k drvs, 5 layers × 20k wide, 1500 workers on
//!   one job. Stresses: memory, claim throughput, rdep propagation,
//!   ingest throughput. Closest analog: "nixpkgs-scale single build".
//! - `xl_b_fleet_many_jobs` — 500 concurrent jobs × 100 drvs each,
//!   1500 fleet workers. Stresses: fleet-scan cost, per-submission
//!   lock contention, Submissions map size.
//! - `xl_c_monster_single_job` — 50k drvs in one job, 1500 workers.
//!   Stresses: Submission.ready lock contention.
//!
//! **Harness reality check:** macOS ephemeral-port range is ~16k ports
//! and HTTP/1.1 long-poll holds a connection for the whole wait_secs.
//! 3000+ in-process workers × long-poll = TIME_WAIT storm exhausts the
//! box. Worker counts here are capped at what the single-host harness
//! can actually drive; production in k8s sees independent pods with
//! their own port pools.

#![cfg(feature = "scale-test")]

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::spawn_server;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, ErrorCategory, IngestBatchRequest, IngestDrvRequest, JobId,
};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool};

/// sqlx::test hands us a pool with a tiny default pool size (5) that
/// saturates instantly under 3k+ workers. For bottleneck-hunting we
/// want to take that pool's already-migrated database and reopen it
/// with a production-sized pool so PG-pool contention doesn't mask
/// everything else. Returns a pool connected to the same database
/// but with `size` max_connections.
async fn resize_pool(existing: &PgPool, size: u32) -> PgPool {
    // sqlx::test gives us an anonymous ephemeral database. The URL
    // isn't directly accessible, but the connect options on the
    // existing pool are, so we rebuild from those.
    let conn_opts = existing.connect_options();
    let opts: PgConnectOptions = (*conn_opts).clone();
    PgPoolOptions::new()
        .max_connections(size)
        .min_connections(4)
        .acquire_timeout(Duration::from_secs(30))
        .connect_with(opts.disable_statement_logging())
        .await
        .expect("resized pool")
}

// ─── Latency instrumentation ───────────────────────────────────────

/// Two histograms: one for successful claims (returned Some), one for
/// long-poll timeouts (returned None). Tail reporting on just the
/// successful bucket gives a clean view of the hot path.
struct LatBuckets {
    success: Mutex<Vec<f64>>,
    empty: Mutex<Vec<f64>>,
    complete: Mutex<Vec<f64>>,
}
impl LatBuckets {
    fn new() -> Self {
        Self {
            success: Mutex::new(Vec::new()),
            empty: Mutex::new(Vec::new()),
            complete: Mutex::new(Vec::new()),
        }
    }
    fn success(&self, d: Duration) {
        self.success.lock().push(d.as_secs_f64() * 1000.0);
    }
    fn empty(&self, d: Duration) {
        self.empty.lock().push(d.as_secs_f64() * 1000.0);
    }
    fn complete(&self, d: Duration) {
        self.complete.lock().push(d.as_secs_f64() * 1000.0);
    }
    fn report(&self, label: &str, bucket: &str, v: &Mutex<Vec<f64>>) {
        let mut vals = std::mem::take(&mut *v.lock());
        if vals.is_empty() {
            eprintln!("{label} {bucket}: n=0");
            return;
        }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = vals.len();
        let p = |q: f64| vals[(((n - 1) as f64) * q).round() as usize];
        eprintln!(
            "{label} {bucket}: n={n} p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
            p(0.50),
            p(0.95),
            p(0.99),
            vals[n - 1]
        );
    }
    fn report_all(&self, label: &str) {
        self.report(label, "claim/success", &self.success);
        self.report(label, "claim/empty  ", &self.empty);
        self.report(label, "complete     ", &self.complete);
    }
}

/// Lightweight periodic RSS sampler. Pushed into a Vec; reported at the
/// end to see memory growth over a run.
fn spawn_rss_sampler(label: String, interval: Duration) -> (tokio::task::JoinHandle<()>, Arc<AtomicU64>) {
    let peak = Arc::new(AtomicU64::new(0));
    let peak_for_task = peak.clone();
    let handle = tokio::spawn(async move {
        let pid = std::process::id();
        let mut last_sample = 0u64;
        loop {
            tokio::time::sleep(interval).await;
            let Ok(rss) = read_rss_kb(pid) else { break };
            peak_for_task.fetch_max(rss, Ordering::Relaxed);
            if rss > last_sample + 50_000 || last_sample == 0 {
                eprintln!("{label} RSS: {:.2} GiB", rss as f64 / 1_048_576.0);
                last_sample = rss;
            }
        }
    });
    (handle, peak)
}

/// Scrape `/metrics` and print the buckets for a handful of histograms
/// whose totals point at the dominant hot-path cost: PG pool wait,
/// fleet scan, rdep propagation, per-route HTTP, and lock wait. We
/// print bucket counts rather than computed percentiles because the
/// Prom text format has cumulative buckets and we care more about
/// shape ("where did most samples land") than an exact pXX.
async fn dump_key_histograms(base_url: &str, label: &str) {
    let body = match reqwest::get(format!("{base_url}/metrics")).await {
        Ok(r) => match r.text().await {
            Ok(t) => t,
            Err(_) => return,
        },
        Err(_) => return,
    };
    let interesting = [
        "nix_ci_pg_pool_acquire_duration_seconds",
        "nix_ci_fleet_scan_duration_seconds",
        "nix_ci_rdep_propagation_duration_seconds",
        "nix_ci_dispatch_wait_seconds",
        // per-route HTTP duration: want to see ALL routes to tell
        // whether claim / complete / status are the offenders.
        "nix_ci_http_request_duration_seconds",
        "nix_ci_lock_wait_seconds",
        "nix_ci_claim_age_seconds",
    ];
    eprintln!("=== {label} metrics dump ===");
    for metric in interesting.iter() {
        // Print ALL `_sum` and `_count` lines matching this metric
        // family — http_request_duration has per-route labels, so we
        // want to see every route's numbers, not just the first one.
        for line in body.lines() {
            if line.starts_with('#') {
                continue;
            }
            let prefix_sum = format!("{metric}_sum");
            let prefix_count = format!("{metric}_count");
            if line.starts_with(&prefix_sum) || line.starts_with(&prefix_count) {
                eprintln!("  {line}");
            }
        }
    }
}

/// Read RSS in KB for the given PID. Works on Linux via /proc and on
/// macOS via `ps -o rss=`. Returns 0 on parse failure rather than
/// erroring (we'd rather see an underestimate than crash the bench).
fn read_rss_kb(pid: u32) -> Result<u64, std::io::Error> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: u64 = rest
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                return Ok(kb);
            }
        }
        Ok(0)
    }
    #[cfg(target_os = "macos")]
    {
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()?;
        let s = String::from_utf8_lossy(&out.stdout);
        Ok(s.trim().parse().unwrap_or(0))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = pid;
        Ok(0)
    }
}

fn drv_path(label: &str, idx: usize) -> String {
    format!("/nix/store/{label}{idx:07}-x.drv")
}

/// Per-job worker loop that cleanly separates successful-claim latency
/// from long-poll-empty latency. Workers return as soon as the job is
/// terminal (status probe after any empty result).
async fn run_workers_per_job(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    workers: usize,
    wait_secs: u64,
    fail_rate: f64,
    seed_base: u64,
    lat: Arc<LatBuckets>,
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
                    .claim(job_id, "x86_64-linux", &[], wait_secs)
                    .await
                    .ok()
                    .flatten();
                let elapsed = t0.elapsed();
                match c {
                    Some(claim) => {
                        lat.success(elapsed);
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
                            error_message: will_fail.then_some("xl fail".into()),
                            log_tail: None,
                        };
                        let tc = Instant::now();
                        let _ = client.complete(job_id, claim.claim_id, &req).await;
                        lat.complete(tc.elapsed());
                    }
                    None => {
                        lat.empty(elapsed);
                        if let Ok(s) = client.status(job_id).await {
                            if s.status.is_terminal() {
                                return;
                            }
                        }
                    }
                }
            }
        });
    }
    while tasks.join_next().await.is_some() {}
}

/// Fleet-mode worker loop: claim any job, pay attention to returned
/// job_id. Terminates when `done` is flipped externally.
async fn run_workers_fleet(
    client: Arc<CoordinatorClient>,
    workers: usize,
    wait_secs: u64,
    fail_rate: f64,
    seed_base: u64,
    lat: Arc<LatBuckets>,
    done: Arc<std::sync::atomic::AtomicBool>,
) {
    let mut tasks = tokio::task::JoinSet::new();
    for w in 0..workers {
        let client = client.clone();
        let lat = lat.clone();
        let done = done.clone();
        let seed = seed_base.wrapping_add(w as u64);
        tasks.spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            loop {
                if done.load(Ordering::Relaxed) {
                    return;
                }
                let t0 = Instant::now();
                let c = client
                    .claim_any("x86_64-linux", &[], wait_secs)
                    .await
                    .ok()
                    .flatten();
                let elapsed = t0.elapsed();
                match c {
                    Some(claim) => {
                        lat.success(elapsed);
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
                            error_message: will_fail.then_some("xl fail".into()),
                            log_tail: None,
                        };
                        let tc = Instant::now();
                        let _ = client.complete(claim.job_id, claim.claim_id, &req).await;
                        lat.complete(tc.elapsed());
                    }
                    None => {
                        lat.empty(elapsed);
                    }
                }
            }
        });
    }
    while tasks.join_next().await.is_some() {}
}

// ─── Scenario A: deep wide DAG, one big job ────────────────────────

/// 100k drvs in one job, 5 layers × 20k wide, fan-in 2. 5k concurrent
/// workers. This is "nixpkgs-scale single build, lots of parallelism."
/// Stresses: memory, claim throughput, rdep propagation, ingest
/// throughput, per-submission lock contention.
#[sqlx::test]
async fn xl_a_deep_wide(pool: PgPool) {
    const LAYERS: usize = 5;
    const WIDTH: usize = 20_000;
    const WORKERS: usize = 1_500;
    const FAN_IN: usize = 2;

    let pool = resize_pool(&pool, 64).await;
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let lat = Arc::new(LatBuckets::new());
    let (rss_task, peak_rss) = spawn_rss_sampler("xl-a".into(), Duration::from_secs(5));

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("xl-a-deep-wide".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut rng = StdRng::seed_from_u64(0xa_a_a_a);
    let t_ingest = Instant::now();
    eprintln!("xl-a INGEST begin layers={LAYERS} width={WIDTH} fan_in={FAN_IN} via batch");
    // Batch ingest: 2000 drvs per POST /drvs/batch. Ingest throughput
    // is already measured by xl_c_* and isn't the bottleneck we're
    // hunting here — we want to get into the dispatch phase quickly.
    const BATCH: usize = 2_000;
    for layer in 0..LAYERS {
        let t_layer = Instant::now();
        let mut buf: Vec<IngestDrvRequest> = Vec::with_capacity(BATCH);
        for i in 0..WIDTH {
            let deps: Vec<String> = if layer == 0 {
                Vec::new()
            } else {
                let mut ds = HashSet::new();
                while ds.len() < FAN_IN {
                    ds.insert(drv_path(
                        &format!("l{}", layer - 1),
                        rng.gen_range(0..WIDTH),
                    ));
                }
                ds.into_iter().collect()
            };
            buf.push(IngestDrvRequest {
                drv_path: drv_path(&format!("l{layer}"), i),
                drv_name: format!("l{layer}i{i}"),
                system: "x86_64-linux".into(),
                required_features: vec![],
                input_drvs: deps,
                is_root: layer == LAYERS - 1,
                attr: None,
            });
            if buf.len() == BATCH {
                let batch = std::mem::take(&mut buf);
                client
                    .ingest_batch(
                        job.id,
                        &IngestBatchRequest {
                            drvs: batch,
                            eval_errors: vec![],
                        },
                    )
                    .await
                    .unwrap();
            }
        }
        if !buf.is_empty() {
            client
                .ingest_batch(
                    job.id,
                    &IngestBatchRequest {
                        drvs: std::mem::take(&mut buf),
                        eval_errors: vec![],
                    },
                )
                .await
                .unwrap();
        }
        eprintln!(
            "xl-a INGEST layer {} done in {:.2}s",
            layer,
            t_layer.elapsed().as_secs_f64()
        );
    }
    client.seal(job.id).await.unwrap();
    let ingest_elapsed = t_ingest.elapsed();

    let t_build = Instant::now();
    run_workers_per_job(
        client.clone(),
        job.id,
        WORKERS,
        3,
        0.01,
        0xaa_0000,
        lat.clone(),
    )
    .await;
    let build_elapsed = t_build.elapsed();

    let status = client.status(job.id).await.unwrap();
    eprintln!(
        "xl-a RESULT drvs={} terminal={:?} ingest={:.2}s build={:.2}s total={:.2}s peak_rss={:.2} GiB",
        status.counts.total,
        status.status,
        ingest_elapsed.as_secs_f64(),
        build_elapsed.as_secs_f64(),
        (ingest_elapsed + build_elapsed).as_secs_f64(),
        peak_rss.load(Ordering::Relaxed) as f64 / 1_048_576.0
    );
    lat.report_all("xl-a");
    dump_key_histograms(&handle.base_url, "xl-a").await;

    rss_task.abort();
    assert!(status.status.is_terminal(), "job must terminate");
    assert_eq!(
        status.counts.total,
        status.counts.done + status.counts.failed,
        "all drvs must reach terminal"
    );
}

// ─── Scenario B: 1000 concurrent jobs × 100 drvs, fleet workers ────

/// Stresses fleet-scan cost + per-submission lock contention. Each job
/// is tiny (100 drvs) but there are a lot of them (1000) and a lot of
/// fleet workers (5k).
#[sqlx::test]
async fn xl_b_fleet_many_jobs(pool: PgPool) {
    const JOBS: usize = 500;
    const DRVS_PER_JOB: usize = 100;
    const WORKERS: usize = 1_500;

    let pool = resize_pool(&pool, 64).await;
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let lat = Arc::new(LatBuckets::new());
    let (rss_task, peak_rss) = spawn_rss_sampler("xl-b".into(), Duration::from_secs(5));

    // Pre-create all jobs + ingest their drvs BEFORE starting workers,
    // so measured claim/complete times aren't mixed with ingest.
    let t_ingest = Instant::now();
    let mut job_ids: Vec<JobId> = Vec::with_capacity(JOBS);
    for j in 0..JOBS {
        let job = client
            .create_job(&CreateJobRequest {
                external_ref: Some(format!("xl-b-job-{j}")),
                ..Default::default()
            })
            .await
            .unwrap();
        job_ids.push(job.id);
        for i in 0..DRVS_PER_JOB {
            client
                .ingest_drv(
                    job.id,
                    &IngestDrvRequest {
                        drv_path: format!("/nix/store/b{j:04}d{i:03}-x.drv"),
                        drv_name: format!("b{j}d{i}"),
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
        if j % 100 == 0 {
            eprintln!(
                "xl-b INGEST {j}/{JOBS} jobs in {:.2}s",
                t_ingest.elapsed().as_secs_f64()
            );
        }
    }
    let ingest_elapsed = t_ingest.elapsed();
    eprintln!("xl-b INGEST done in {:.2}s", ingest_elapsed.as_secs_f64());

    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let total_drvs = (JOBS * DRVS_PER_JOB) as u32;
    let t_build = Instant::now();
    let workers_task = {
        let client = client.clone();
        let lat = lat.clone();
        let done = done.clone();
        tokio::spawn(async move {
            run_workers_fleet(client, WORKERS, 3, 0.01, 0xbb_0000, lat, done).await;
        })
    };

    // Poll until all jobs terminal.
    let client2 = client.clone();
    let job_ids2 = job_ids.clone();
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let mut all_terminal = true;
        for jid in &job_ids2 {
            let s = client2.status(*jid).await.unwrap();
            if !s.status.is_terminal() {
                all_terminal = false;
                break;
            }
        }
        if all_terminal {
            break;
        }
    }
    done.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(30), workers_task).await;
    let build_elapsed = t_build.elapsed();

    eprintln!(
        "xl-b RESULT jobs={JOBS} drvs/job={DRVS_PER_JOB} total={total_drvs} \
         ingest={:.2}s build={:.2}s peak_rss={:.2} GiB",
        ingest_elapsed.as_secs_f64(),
        build_elapsed.as_secs_f64(),
        peak_rss.load(Ordering::Relaxed) as f64 / 1_048_576.0
    );
    lat.report_all("xl-b");
    dump_key_histograms(&handle.base_url, "xl-b").await;
    rss_task.abort();
}

// ─── Scenario C: monster single job, heavy parallelism ─────────────

/// One job, 50k drvs fan-in-2, 3k workers pointed at just that job.
/// Designed to hit the `Submission.ready` RwLock contention ceiling.
#[sqlx::test]
async fn xl_c_monster_single_job(pool: PgPool) {
    const LAYERS: usize = 5;
    const WIDTH: usize = 10_000;
    const WORKERS: usize = 1_500;
    const FAN_IN: usize = 2;

    let pool = resize_pool(&pool, 64).await;
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let lat = Arc::new(LatBuckets::new());
    let (rss_task, peak_rss) = spawn_rss_sampler("xl-c".into(), Duration::from_secs(5));

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("xl-c-monster".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut rng = StdRng::seed_from_u64(0xc_c_c);
    let t_ingest = Instant::now();
    for layer in 0..LAYERS {
        for i in 0..WIDTH {
            let deps: Vec<String> = if layer == 0 {
                Vec::new()
            } else {
                let mut ds = HashSet::new();
                while ds.len() < FAN_IN {
                    ds.insert(drv_path(
                        &format!("cl{}", layer - 1),
                        rng.gen_range(0..WIDTH),
                    ));
                }
                ds.into_iter().collect()
            };
            client
                .ingest_drv(
                    job.id,
                    &IngestDrvRequest {
                        drv_path: drv_path(&format!("cl{layer}"), i),
                        drv_name: format!("cl{layer}i{i}"),
                        system: "x86_64-linux".into(),
                        required_features: vec![],
                        input_drvs: deps,
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
    run_workers_per_job(
        client.clone(),
        job.id,
        WORKERS,
        3,
        0.01,
        0xcc_0000,
        lat.clone(),
    )
    .await;
    let build_elapsed = t_build.elapsed();

    let status = client.status(job.id).await.unwrap();
    eprintln!(
        "xl-c RESULT drvs={} terminal={:?} ingest={:.2}s build={:.2}s peak_rss={:.2} GiB",
        status.counts.total,
        status.status,
        ingest_elapsed.as_secs_f64(),
        build_elapsed.as_secs_f64(),
        peak_rss.load(Ordering::Relaxed) as f64 / 1_048_576.0
    );
    lat.report_all("xl-c");
    dump_key_histograms(&handle.base_url, "xl-c").await;
    rss_task.abort();
}
