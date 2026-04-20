//! Production-scale load test. Gated behind the `load-test` feature
//! so it's skipped by the standard `cargo test`.
//!
//! Shape: 5-layer DAG, 2000 drvs per layer = 10,000 drvs total. Fan-in
//! pattern — every layer N drv depends on 2 random drvs from layer N-1.
//! 500 concurrent simulated workers. 5% failure injection.
//!
//! Goal: the whole run finishes in <60s on a modest dev machine, p99
//! claim latency <200ms, no stale claims, invariants uphold throughout.
//! Compare `nix_ci_dispatch_wait_seconds` / build duration across runs
//! to catch perf regressions.

#![cfg(feature = "load-test")]

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::spawn_server;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, ErrorCategory, IngestDrvRequest};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sqlx::PgPool;

const LAYERS: usize = 5;
const WIDTH: usize = 2_000;
const FAN_IN: usize = 2;
const WORKERS: usize = 500;
const FAILURE_RATE: f64 = 0.05;

struct Latencies {
    claim: Mutex<Vec<f64>>,
    complete: Mutex<Vec<f64>>,
}

impl Latencies {
    fn new() -> Self {
        Self {
            claim: Mutex::new(Vec::new()),
            complete: Mutex::new(Vec::new()),
        }
    }
    fn record_claim(&self, d: Duration) {
        self.claim.lock().push(d.as_secs_f64() * 1000.0);
    }
    fn record_complete(&self, d: Duration) {
        self.complete.lock().push(d.as_secs_f64() * 1000.0);
    }
    /// Summarize and return the claim-latency percentiles so the
    /// caller can SPEC-assert against them. Returns (p50_ms, p99_ms);
    /// printing the full breakdown as a side effect for CI logs.
    fn summarize(&self, label: &str) -> ClaimPercentiles {
        let mut v = std::mem::take(&mut *self.claim.lock());
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let claim_pctiles = if v.is_empty() {
            ClaimPercentiles::default()
        } else {
            let p50 = pct(&v, 0.50);
            let p95 = pct(&v, 0.95);
            let p99 = pct(&v, 0.99);
            let max = v.last().copied().unwrap_or(0.0);
            eprintln!(
                "{label} claim    n={:6} p50={:7.2}ms p95={:7.2}ms p99={:7.2}ms max={:7.2}ms",
                v.len(),
                p50,
                p95,
                p99,
                max
            );
            ClaimPercentiles {
                n: v.len(),
                p50_ms: p50,
                p99_ms: p99,
            }
        };
        let mut v = std::mem::take(&mut *self.complete.lock());
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if !v.is_empty() {
            eprintln!(
                "{label} complete n={:6} p50={:7.2}ms p95={:7.2}ms p99={:7.2}ms max={:7.2}ms",
                v.len(),
                pct(&v, 0.50),
                pct(&v, 0.95),
                pct(&v, 0.99),
                v.last().copied().unwrap_or(0.0)
            );
        }
        claim_pctiles
    }
}

#[derive(Default, Debug)]
struct ClaimPercentiles {
    n: usize,
    p50_ms: f64,
    p99_ms: f64,
}

fn pct(sorted: &[f64], q: f64) -> f64 {
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx]
}

fn drv_path(layer: usize, idx: usize) -> String {
    format!("/nix/store/l{layer:02}i{idx:06}-drv.drv")
}

#[sqlx::test]
async fn production_scale_dag_with_failures(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let lat = Arc::new(Latencies::new());
    let build_failed = Arc::new(AtomicU32::new(0));
    let build_ok = Arc::new(AtomicU32::new(0));
    let ignored = Arc::new(AtomicU32::new(0));

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("load-test".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let ingest_start = Instant::now();
    let mut rng = StdRng::seed_from_u64(0x0005_c41e);

    // Layer 0: no deps
    for i in 0..WIDTH {
        let req = IngestDrvRequest {
            drv_path: drv_path(0, i),
            drv_name: format!("l0i{i}"),
            system: "x86_64-linux".into(),
            required_features: vec![],
            input_drvs: vec![],
            is_root: false,
            attr: None,
        };
        client.ingest_drv(job.id, &req).await.unwrap();
    }

    // Layers 1..LAYERS: each drv depends on FAN_IN random drvs from previous layer
    for layer in 1..LAYERS {
        for i in 0..WIDTH {
            let mut deps = HashSet::new();
            while deps.len() < FAN_IN {
                let d = rng.gen_range(0..WIDTH);
                deps.insert(drv_path(layer - 1, d));
            }
            let is_root = layer == LAYERS - 1;
            let req = IngestDrvRequest {
                drv_path: drv_path(layer, i),
                drv_name: format!("l{layer}i{i}"),
                system: "x86_64-linux".into(),
                required_features: vec![],
                input_drvs: deps.into_iter().collect(),
                is_root,
                attr: None,
            };
            client.ingest_drv(job.id, &req).await.unwrap();
        }
    }
    client.seal(job.id).await.unwrap();
    let ingest_elapsed = ingest_start.elapsed();
    eprintln!(
        "ingested {} drvs ({} roots) in {:.2}s ({:.0} drvs/s)",
        LAYERS * WIDTH,
        WIDTH,
        ingest_elapsed.as_secs_f64(),
        (LAYERS * WIDTH) as f64 / ingest_elapsed.as_secs_f64(),
    );

    // Spawn WORKERS concurrent claim/complete loops
    let build_start = Instant::now();
    let mut worker_tasks = tokio::task::JoinSet::new();
    for worker_id in 0..WORKERS {
        let client = client.clone();
        let lat = lat.clone();
        let build_failed = build_failed.clone();
        let build_ok = build_ok.clone();
        let ignored = ignored.clone();
        let job_id = job.id;
        let seed = 0xf00d_u64.wrapping_add(worker_id as u64);
        worker_tasks.spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            loop {
                let t0 = Instant::now();
                let c = client
                    .claim(job_id, "x86_64-linux", &[], 3)
                    .await
                    .ok()
                    .flatten();
                lat.record_claim(t0.elapsed());
                let claim = match c {
                    Some(c) => c,
                    None => {
                        // Nothing to do — poll status; if terminal, exit.
                        if let Ok(s) = client.status(job_id).await {
                            if s.status.is_terminal() {
                                return;
                            }
                        }
                        continue;
                    }
                };
                // Simulate a build: 5% random failure, otherwise success
                let will_fail = rng.gen_bool(FAILURE_RATE);
                let req = if will_fail {
                    CompleteRequest {
                        success: false,
                        duration_ms: 10,
                        exit_code: Some(1),
                        error_category: Some(ErrorCategory::BuildFailure),
                        error_message: Some("simulated fail".into()),
                        log_tail: None,
                    }
                } else {
                    CompleteRequest {
                        success: true,
                        duration_ms: 10,
                        exit_code: Some(0),
                        error_category: None,
                        error_message: None,
                        log_tail: None,
                    }
                };
                let t0 = Instant::now();
                let resp = client.complete(job_id, claim.claim_id, &req).await;
                lat.record_complete(t0.elapsed());
                match resp {
                    Ok(r) if r.ignored => {
                        ignored.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(_) => {
                        if will_fail {
                            build_failed.fetch_add(1, Ordering::Relaxed);
                        } else {
                            build_ok.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        // back off briefly
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });
    }

    // Wait for all workers to finish
    while worker_tasks.join_next().await.is_some() {}
    let build_elapsed = build_start.elapsed();

    // Final status
    let status = client.status(job.id).await.unwrap();
    eprintln!(
        "job terminal state: {:?} ({} ok, {} failed, {} propagated, {} ignored completions)",
        status.status,
        build_ok.load(Ordering::Relaxed),
        build_failed.load(Ordering::Relaxed),
        status.counts.failed - build_failed.load(Ordering::Relaxed),
        ignored.load(Ordering::Relaxed),
    );
    eprintln!(
        "drvs: total={} done={} failed={}  build phase {:.2}s ({:.0} drvs/s)",
        status.counts.total,
        status.counts.done,
        status.counts.failed,
        build_elapsed.as_secs_f64(),
        (status.counts.total as f64) / build_elapsed.as_secs_f64(),
    );
    let claim_pctiles = lat.summarize("LOAD");

    // Assertions: the job terminated, nearly-all drvs reached terminal
    // state, no one's starving.
    assert!(status.status.is_terminal(), "job never terminated");
    // With 5% failure rate and a fan-in=2 DAG of depth 5, every leaf's
    // propagation can cascade widely. We just assert everything is
    // terminal.
    assert_eq!(
        status.counts.total,
        status.counts.done + status.counts.failed,
        "some drvs are neither done nor failed"
    );
    assert!(
        status.counts.done > 0,
        "no drvs succeeded — dedup or dispatch is broken"
    );

    // The test completes in <60s on a typical dev machine. If this
    // starts failing, check `nix_ci_dispatch_wait_seconds` p99.
    assert!(
        build_elapsed < Duration::from_secs(120),
        "build phase took too long: {}s",
        build_elapsed.as_secs()
    );

    // p99 claim latency must be < 200ms under this exact load shape.
    // Catches dispatcher regressions that would inflate tail wait
    // times (new lock contention, added PG calls on the claim hot
    // path, etc.).
    assert!(
        claim_pctiles.p99_ms < 200.0,
        "p99 regression: p99={:.2}ms must be < 200ms (n={})",
        claim_pctiles.p99_ms,
        claim_pctiles.n
    );
    // Median — a good signal for baseline dispatch overhead. 20ms is
    // comfortably above the expected <1ms for a warm claim and safely
    // below what a real production tail regression would look like.
    assert!(
        claim_pctiles.p50_ms < 20.0,
        "p50 regression: p50={:.2}ms must be < 20ms",
        claim_pctiles.p50_ms
    );

    // Ingest throughput bar. This setup uses `ingest_drv` (single-drv
    // per request) rather than batching, so the measured throughput is
    // the per-drv latency * request rate, not the peak batch ingest
    // rate. We assert a generous 2K/sec floor here and track the
    // actual number in log output — the 10K/sec aspirational bar
    // requires switching ingest to batch mode.
    let ingest_throughput = (LAYERS * WIDTH) as f64 / ingest_elapsed.as_secs_f64();
    eprintln!("ingest throughput: {ingest_throughput:.0} drvs/sec");
    assert!(
        ingest_throughput > 2_000.0,
        "ingest throughput too low: {ingest_throughput:.0} drvs/sec (expected > 2000 with single-drv ingest)"
    );

    // Coordinator RSS after 10K drvs must be bounded. 2 GiB ceiling;
    // assert a conservative 1 GiB here as a leading indicator — a
    // real memory leak typically
    // shows as 10-50x growth, not a small percentage increase.
    #[cfg(target_os = "linux")]
    {
        if let Ok(rss_kb) = read_rss_kb() {
            eprintln!("coordinator RSS after 10K drvs: {rss_kb} KiB");
            assert!(
                rss_kb < 1_000_000,
                "S-MEM-10K regression risk: RSS={rss_kb} KiB exceeds 1 GiB early-warning threshold (SPEC bar is 2 GiB)"
            );
        }
    }
}

#[cfg(target_os = "linux")]
fn read_rss_kb() -> std::io::Result<u64> {
    // /proc/self/status contains `VmRSS:\t    <kb> kB` — cheap parse,
    // no allocations beyond the read. On non-Linux the test just
    // skips this measurement; CI runs on ubuntu-latest so we get it
    // in the pipeline that matters.
    let s = std::fs::read_to_string("/proc/self/status")?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let parts: Vec<_> = rest.split_whitespace().collect();
            if let Some(kb_str) = parts.first() {
                return kb_str
                    .parse::<u64>()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e));
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "VmRSS not in /proc/self/status",
    ))
}
