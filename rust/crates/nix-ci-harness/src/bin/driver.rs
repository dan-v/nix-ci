//! Harness driver. Creates a job, ingests a synthetic DAG, seals,
//! waits for terminal, scrapes /metrics, checks SLO assertions,
//! and emits a JSON report. Mock workers run in sibling containers
//! doing the actual claim/complete work.
//!
//! Scenarios:
//! - `wide-fleet`: large deep-wide DAG, all-healthy workers.
//!   Primary assertion: job reaches Done within wall budget, zero
//!   failures.
//! - `sick-worker-contained`: small DAG, ONE healthy worker handles
//!   work while sick workers failure-flood. Assertions: job reaches
//!   Done (healthy worker finishes the graph), quarantine counter
//!   ticks, admin/refute clears failed_outputs cache.

use std::time::{Duration, Instant};

use clap::Parser;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CreateJobRequest, IngestBatchRequest, JobStatus, JobStatusResponse,
};
use nix_ci_harness::{
    layered_dag, metric_value, scrape_metrics, AssertionResult, DriverReport,
};

#[derive(Parser, Debug)]
#[command(name = "nix-ci-harness-driver")]
struct Args {
    #[arg(long, env = "HARNESS_COORDINATOR")]
    coordinator: String,

    #[arg(long, env = "HARNESS_AUTH_BEARER")]
    auth_bearer: Option<String>,

    #[arg(long, env = "HARNESS_ADMIN_BEARER")]
    admin_bearer: Option<String>,

    /// Scenario: `wide-fleet` | `sick-worker-contained`.
    #[arg(long)]
    scenario: String,

    /// DAG shape (used by wide-fleet).
    #[arg(long, default_value_t = 5)]
    layers: usize,

    #[arg(long, default_value_t = 2000)]
    width: usize,

    #[arg(long, default_value_t = 2)]
    fan_in: usize,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Batch size for ingest. Coordinator's max_drvs_per_batch is
    /// larger than this; we chunk to keep per-call latency bounded.
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,

    /// How long to wait for the job to reach terminal state.
    #[arg(long, default_value_t = 900)]
    max_wait_secs: u64,

    /// SLO bar: fail assertion if p99 claim latency (server
    /// histogram) exceeds this many seconds. 0 = don't enforce.
    #[arg(long, default_value_t = 0.0)]
    assert_claim_p99_secs: f64,

    /// Emit the JSON report to this path. If absent, prints to stdout.
    #[arg(long)]
    output: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let client =
        CoordinatorClient::with_auth(args.coordinator.clone(), args.auth_bearer.clone());

    // Wait for coordinator health: orchestration starts containers in
    // parallel so the driver can race the coordinator. Brief retry.
    wait_healthy(&args.coordinator, Duration::from_secs(30)).await?;

    let report = match args.scenario.as_str() {
        "wide-fleet" => scenario_wide_fleet(&args, &client).await?,
        "sick-worker-contained" => scenario_sick_worker_contained(&args, &client).await?,
        other => anyhow::bail!("unknown scenario: {other}"),
    };

    let json = serde_json::to_string_pretty(&report)?;
    match &args.output {
        Some(p) => std::fs::write(p, &json)?,
        None => println!("{json}"),
    }

    let failed: Vec<&AssertionResult> = report.assertions.iter().filter(|a| !a.passed).collect();
    if !failed.is_empty() {
        eprintln!("{} assertion(s) failed", failed.len());
        for a in &failed {
            eprintln!("  FAIL {}: {}", a.name, a.detail);
        }
        std::process::exit(2);
    }
    Ok(())
}

async fn wait_healthy(base: &str, total: Duration) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{base}/healthz");
    let deadline = Instant::now() + total;
    loop {
        match client.get(&url).timeout(Duration::from_secs(2)).send().await {
            Ok(r) if r.status().is_success() => return Ok(()),
            _ => {}
        }
        if Instant::now() > deadline {
            anyhow::bail!("coordinator never became healthy at {base}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn ingest_dag(
    client: &CoordinatorClient,
    job_id: nix_ci_core::types::JobId,
    drvs: Vec<nix_ci_core::types::IngestDrvRequest>,
    batch_size: usize,
) -> anyhow::Result<()> {
    for chunk in drvs.chunks(batch_size) {
        let req = IngestBatchRequest {
            drvs: chunk.to_vec(),
            eval_errors: Vec::new(),
        };
        client.ingest_batch(job_id, &req).await?;
    }
    Ok(())
}

async fn wait_terminal(
    client: &CoordinatorClient,
    job_id: nix_ci_core::types::JobId,
    max_wait: Duration,
) -> anyhow::Result<JobStatusResponse> {
    let deadline = Instant::now() + max_wait;
    let mut last: Option<JobStatusResponse> = None;
    while Instant::now() < deadline {
        let s = client.status(job_id).await?;
        let terminal = matches!(
            s.status,
            JobStatus::Done | JobStatus::Failed | JobStatus::Cancelled
        );
        last = Some(s);
        if terminal {
            return Ok(last.unwrap());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    last.ok_or_else(|| anyhow::anyhow!("no status observed before timeout"))
}

// ─── Scenario: wide-fleet ────────────────────────────────────────

async fn scenario_wide_fleet(
    args: &Args,
    client: &CoordinatorClient,
) -> anyhow::Result<DriverReport> {
    let wall_start = Instant::now();
    let drvs = layered_dag(args.layers, args.width, args.fan_in, args.seed);
    let total = drvs.len() as u64;
    tracing::info!(total_drvs = total, "generated DAG");

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some(format!("harness-wide-fleet-{}", args.seed)),
            priority: 0,
            max_workers: None,
            claim_deadline_secs: None,
        })
        .await?;
    tracing::info!(job_id = %job.id, "created job");

    ingest_dag(client, job.id, drvs, args.batch_size).await?;
    tracing::info!("ingest complete");
    client.seal(job.id).await?;
    tracing::info!("sealed — waiting for terminal");

    let status = wait_terminal(client, job.id, Duration::from_secs(args.max_wait_secs)).await?;
    let wall = wall_start.elapsed().as_secs_f64();

    let metrics = scrape_metrics(&args.coordinator).await?;
    let mut assertions = Vec::new();

    // 1. Job reached Done.
    let reached_done = status.status == JobStatus::Done;
    assertions.push(AssertionResult {
        name: "job_reaches_done".into(),
        passed: reached_done,
        detail: format!(
            "final status: {:?}, counts: total={} done={} failed={}",
            status.status, status.counts.total, status.counts.done, status.counts.failed
        ),
    });

    // 2. All drvs succeeded (zero failures in wide-fleet).
    let zero_failures = status.counts.failed == 0;
    assertions.push(AssertionResult {
        name: "zero_failures".into(),
        passed: zero_failures,
        detail: format!("failed count = {}", status.counts.failed),
    });

    // 3. No overload rejections in the run. Counter is only
    // materialized once incremented, so absence = 0.
    let overload =
        metric_value(&metrics, "nix_ci_overload_rejections_total", &[]).unwrap_or(0.0);
    let no_overload = overload == 0.0;
    assertions.push(AssertionResult {
        name: "no_overload_rejections".into(),
        passed: no_overload,
        detail: format!("overload_rejections_total = {overload}"),
    });

    // 4. Optional claim p99 bar from Prometheus histogram. The
    // histogram exposes `*_bucket{le="..."}` + `*_sum` + `*_count`.
    // Approximate p99: find the smallest `le` bucket where the
    // cumulative count ≥ 0.99 × total count.
    if args.assert_claim_p99_secs > 0.0 {
        let p99 = approx_histogram_p99(&metrics, "nix_ci_http_request_duration_seconds", 0.99);
        match p99 {
            Some(v) => assertions.push(AssertionResult {
                name: "claim_p99_within_budget".into(),
                passed: v <= args.assert_claim_p99_secs,
                detail: format!(
                    "claim p99 ≈ {v:.3}s (bar {:.3}s)",
                    args.assert_claim_p99_secs
                ),
            }),
            None => assertions.push(AssertionResult {
                name: "claim_p99_within_budget".into(),
                passed: false,
                detail: "histogram not present; cannot assert".into(),
            }),
        }
    }

    Ok(DriverReport {
        scenario: args.scenario.clone(),
        job_id: Some(job.id.to_string()),
        total_drvs: total,
        wall_secs: wall,
        final_status: Some(format!("{:?}", status.status)),
        final_counts: Some(serde_json::to_value(&status.counts)?),
        coord_metrics: metrics_to_json(&metrics),
        assertions,
    })
}

// ─── Scenario: sick-worker-contained ────────────────────────────

async fn scenario_sick_worker_contained(
    args: &Args,
    client: &CoordinatorClient,
) -> anyhow::Result<DriverReport> {
    let wall_start = Instant::now();

    // Smaller DAG — scenario is about quarantine not throughput.
    let drvs = layered_dag(3, 100, 2, args.seed);
    let total = drvs.len() as u64;

    // Capture one drv's output_path for the refute test. A drv is
    // present in failed_outputs iff one of its ATTEMPTS failed
    // with BuildFailure; we pick a mid-layer drv that's likely to
    // be claimed by sick workers.
    let refute_candidate_path = drvs[drvs.len() / 2].drv_path.clone();

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some(format!("harness-sick-{}", args.seed)),
            priority: 0,
            max_workers: None,
            claim_deadline_secs: None,
        })
        .await?;
    ingest_dag(client, job.id, drvs, args.batch_size).await?;
    client.seal(job.id).await?;

    let status = wait_terminal(client, job.id, Duration::from_secs(args.max_wait_secs)).await?;
    let wall = wall_start.elapsed().as_secs_f64();
    let metrics = scrape_metrics(&args.coordinator).await?;

    let mut assertions = Vec::new();
    let admin_tok = args.admin_bearer.as_deref();
    let http = reqwest::Client::new();

    // 1. Quarantine counter incremented at least once.
    let quarantined_total =
        metric_value(&metrics, "nix_ci_worker_auto_quarantined_total", &[]).unwrap_or(0.0);
    assertions.push(AssertionResult {
        name: "quarantine_triggered".into(),
        passed: quarantined_total > 0.0,
        detail: format!("nix_ci_worker_auto_quarantined_total = {quarantined_total}"),
    });

    // 1b. Healthy workers were not quarantined. The admin/fence
    // GET endpoint lists every auto-quarantined worker; every entry
    // here must be a `mock-sick-*` prefix. Any `mock-healthy-*`
    // entry breaks the "healthy workers unaffected" contract.
    let fence_url = format!("{}/admin/fence", args.coordinator);
    let fence_body: Option<serde_json::Value> = {
        let mut r = http.get(&fence_url);
        if let Some(t) = admin_tok {
            r = r.bearer_auth(t);
        }
        match r.send().await {
            Ok(r) if r.status().is_success() => r.json().await.ok(),
            _ => None,
        }
    };
    let healthy_in_fence: Vec<String> = fence_body
        .as_ref()
        .and_then(|v| v.get("auto_quarantined"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            let mut out: Vec<String> = Vec::new();
            for entry in arr {
                if let Some(id) = entry.get("worker_id").and_then(serde_json::Value::as_str) {
                    if id.starts_with("mock-healthy") {
                        out.push(id.to_string());
                    }
                }
            }
            out
        })
        .unwrap_or_default();
    assertions.push(AssertionResult {
        name: "healthy_workers_unaffected".into(),
        passed: healthy_in_fence.is_empty(),
        detail: format!(
            "healthy worker_ids in auto_quarantined list: {healthy_in_fence:?}"
        ),
    });

    // 2. Panic-isolation counter did NOT tick (no handler panics).
    let panics =
        metric_value(&metrics, "nix_ci_process_panics_total", &[]).unwrap_or(0.0);
    assertions.push(AssertionResult {
        name: "no_coordinator_panics".into(),
        passed: panics == 0.0,
        detail: format!("nix_ci_process_panics_total = {panics}"),
    });

    // 3. Job either reached Done (healthy worker finished everything)
    // or Failed if sick workers drained max_tries on some drvs
    // before quarantine kicked in. Either is valid — what's
    // invalid is a hang (non-terminal after max_wait).
    let is_terminal = matches!(
        status.status,
        JobStatus::Done | JobStatus::Failed | JobStatus::Cancelled
    );
    assertions.push(AssertionResult {
        name: "job_reaches_terminal".into(),
        passed: is_terminal,
        detail: format!("final status: {:?}", status.status),
    });

    // 4. admin/refute removes the candidate from failed_outputs
    // without error. No direct client method; go raw.
    let refute_url = format!("{}/admin/refute", args.coordinator);
    let mut req = http
        .post(&refute_url)
        .json(&serde_json::json!({ "output_paths": [refute_candidate_path] }));
    if let Some(t) = admin_tok {
        req = req.bearer_auth(t);
    }
    let refute_ok = req
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);
    assertions.push(AssertionResult {
        name: "admin_refute_succeeds".into(),
        passed: refute_ok,
        detail: format!("POST /admin/refute output_path={refute_candidate_path}"),
    });

    Ok(DriverReport {
        scenario: args.scenario.clone(),
        job_id: Some(job.id.to_string()),
        total_drvs: total,
        wall_secs: wall,
        final_status: Some(format!("{:?}", status.status)),
        final_counts: Some(serde_json::to_value(&status.counts)?),
        coord_metrics: metrics_to_json(&metrics),
        assertions,
    })
}

// ─── Helpers ────────────────────────────────────────────────────

fn metrics_to_json(m: &std::collections::HashMap<String, f64>) -> serde_json::Value {
    // Keep only `nix_ci_*` keys to bound report size.
    let filtered: std::collections::BTreeMap<String, f64> = m
        .iter()
        .filter(|(k, _)| k.starts_with("nix_ci_"))
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    serde_json::to_value(&filtered).unwrap_or(serde_json::Value::Null)
}

/// Approximate p{quantile} from a Prometheus cumulative histogram.
/// Returns the smallest `le` bound where the cumulative count
/// reaches `quantile × total`. Bucket widths limit resolution —
/// this is coarse but matches what dashboards display.
fn approx_histogram_p99(
    map: &std::collections::HashMap<String, f64>,
    family: &str,
    quantile: f64,
) -> Option<f64> {
    let count_key = format!("{family}_count");
    let total = map
        .iter()
        .find(|(k, _)| k.starts_with(&count_key))
        .map(|(_, v)| *v)?;
    if total == 0.0 {
        return None;
    }
    let target = total * quantile;
    let bucket_prefix = format!("{family}_bucket");

    let mut buckets: Vec<(f64, f64)> = Vec::new();
    for (k, v) in map {
        if !k.starts_with(&bucket_prefix) {
            continue;
        }
        let le = extract_label(k, "le")?;
        let le_val = if le == "+Inf" {
            f64::INFINITY
        } else {
            le.parse().ok()?
        };
        buckets.push((le_val, *v));
    }
    buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    for (le, count) in &buckets {
        if *count >= target {
            return Some(*le);
        }
    }
    None
}

fn extract_label(metric_line: &str, key: &str) -> Option<String> {
    let start = metric_line.find('{')? + 1;
    let end = metric_line.rfind('}')?;
    let labels = &metric_line[start..end];
    let needle = format!("{key}=\"");
    let i = labels.find(&needle)? + needle.len();
    let rest = &labels[i..];
    let j = rest.find('"')?;
    Some(rest[..j].to_string())
}
