//! Mock fleet worker. One process spawns N independent claim→complete
//! loops against a coordinator. Modes:
//! - `healthy`: always reports success
//! - `sick`: always reports BuildFailure (exit 1). Used to drive the
//!   worker quarantine contract.
//!
//! On SIGTERM/SIGINT the process stops claiming new work, lets
//! in-flight completes finish (short grace), and emits a
//! `WorkerReport` JSON on stdout so the harness driver can roll
//! stats up across containers.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, ErrorCategory};
use nix_ci_harness::{LatSamples, WorkerReport};
use tokio::signal::unix::{signal, SignalKind};

#[derive(Parser, Debug)]
#[command(name = "nix-ci-mock-worker")]
struct Args {
    /// Coordinator base URL (e.g. http://coordinator:8080).
    #[arg(long, env = "HARNESS_COORDINATOR")]
    coordinator: String,

    /// Optional bearer token for coordinator auth.
    #[arg(long, env = "HARNESS_AUTH_BEARER")]
    auth_bearer: Option<String>,

    /// Number of parallel worker tasks inside this process. Each
    /// gets its own worker_id and its own claim-loop.
    #[arg(long, default_value_t = 50)]
    workers: u32,

    /// Unique prefix for worker_ids. Each task becomes
    /// `<prefix>-<idx>`. Use the container name in orchestration.
    #[arg(long, default_value = "mock")]
    worker_id_prefix: String,

    /// When set, all tasks in this process share the same
    /// worker_id (= the prefix, no suffix). Models "one sick host
    /// with N worker slots" for the quarantine contract — all
    /// slots on the host share identity and trip the circuit
    /// breaker together on aggregate failures.
    #[arg(long)]
    shared_worker_id: bool,

    /// Mode: `healthy` = always success, `sick` = always fail with
    /// BuildFailure. `panic-payload` appends a panic-trigger stderr
    /// but the coordinator should classify safely.
    #[arg(long, default_value = "healthy")]
    mode: String,

    /// System advertised to the coordinator.
    #[arg(long, default_value = "x86_64-linux")]
    system: String,

    /// Long-poll wait duration for claim, seconds.
    #[arg(long, default_value_t = 20)]
    wait_secs: u64,

    /// Simulated build duration, milliseconds. 0 = no sleep (busy
    /// loop shape; maximum pressure on claim/complete path).
    #[arg(long, default_value_t = 0)]
    sim_build_ms: u64,

    /// Run this long then exit with report. 0 = run until signaled.
    #[arg(long, default_value_t = 0)]
    duration_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Healthy,
    Sick,
}

impl Mode {
    fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "healthy" => Ok(Mode::Healthy),
            "sick" => Ok(Mode::Sick),
            _ => anyhow::bail!("unknown mode: {s} (healthy|sick)"),
        }
    }
}

struct SharedState {
    claims_success: AtomicU64,
    claims_empty: AtomicU64,
    completes_ok: AtomicU64,
    completes_err: AtomicU64,
    claim_lat: LatSamples,
    complete_lat: LatSamples,
    stop: AtomicBool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Keep logs on stderr so stdout stays reserved for the final
    // JSON report. Default to WARN so signal/container noise
    // doesn't drown the report in operator output.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = Args::parse();
    let mode = Mode::parse(&args.mode)?;
    let client = CoordinatorClient::with_auth(args.coordinator.clone(), args.auth_bearer.clone());

    let state = Arc::new(SharedState {
        claims_success: AtomicU64::new(0),
        claims_empty: AtomicU64::new(0),
        completes_ok: AtomicU64::new(0),
        completes_err: AtomicU64::new(0),
        claim_lat: LatSamples::new(),
        complete_lat: LatSamples::new(),
        stop: AtomicBool::new(false),
    });

    let wall_start = Instant::now();

    // Spawn N claim-loop tasks.
    let mut handles = Vec::with_capacity(args.workers as usize);
    for idx in 0..args.workers {
        let state = state.clone();
        let client = client.clone();
        let worker_id = if args.shared_worker_id {
            args.worker_id_prefix.clone()
        } else {
            format!("{}-{idx:04}", args.worker_id_prefix)
        };
        let system = args.system.clone();
        let wait_secs = args.wait_secs;
        let sim_build_ms = args.sim_build_ms;
        let h = tokio::spawn(async move {
            worker_loop(state, client, worker_id, system, wait_secs, sim_build_ms, mode).await
        });
        handles.push(h);
    }

    // Shutdown: SIGTERM, SIGINT, or elapsed duration.
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let deadline = if args.duration_secs > 0 {
        Some(tokio::time::sleep(Duration::from_secs(args.duration_secs)))
    } else {
        None
    };

    tokio::select! {
        _ = sigterm.recv() => { tracing::info!("SIGTERM — stopping"); }
        _ = sigint.recv() => { tracing::info!("SIGINT — stopping"); }
        _ = async {
            if let Some(d) = deadline {
                d.await;
            } else {
                std::future::pending::<()>().await;
            }
        } => { tracing::info!("duration elapsed — stopping"); }
    }

    state.stop.store(true, Ordering::Relaxed);

    // Best-effort join; don't block forever on a stuck long-poll.
    let join_deadline = Duration::from_secs(args.wait_secs + 5);
    let join_until = tokio::time::Instant::now() + join_deadline;
    for h in handles {
        let remaining = join_until.saturating_duration_since(tokio::time::Instant::now());
        let _ = tokio::time::timeout(remaining, h).await;
    }

    let report = WorkerReport {
        container_id: args.worker_id_prefix.clone(),
        mode: args.mode.clone(),
        worker_count: args.workers,
        wall_secs: wall_start.elapsed().as_secs_f64(),
        claims_success: state.claims_success.load(Ordering::Relaxed),
        claims_empty: state.claims_empty.load(Ordering::Relaxed),
        completes_ok: state.completes_ok.load(Ordering::Relaxed),
        completes_err: state.completes_err.load(Ordering::Relaxed),
        claim_lat: state.claim_lat.summary(),
        complete_lat: state.complete_lat.summary(),
    };
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

async fn worker_loop(
    state: Arc<SharedState>,
    client: CoordinatorClient,
    worker_id: String,
    system: String,
    wait_secs: u64,
    sim_build_ms: u64,
    mode: Mode,
) {
    let features: Vec<String> = Vec::new();
    while !state.stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();
        let claim_res = client
            .claim_any_as_worker(&system, &features, wait_secs, Some(&worker_id))
            .await;
        let claim_elapsed = t0.elapsed();

        match claim_res {
            Ok(Some(claim)) => {
                state.claim_lat.record(claim_elapsed);
                state.claims_success.fetch_add(1, Ordering::Relaxed);

                if sim_build_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(sim_build_ms)).await;
                }

                let req = match mode {
                    Mode::Healthy => CompleteRequest {
                        success: true,
                        duration_ms: sim_build_ms,
                        exit_code: Some(0),
                        error_category: None,
                        error_message: None,
                        log_tail: None,
                    },
                    Mode::Sick => CompleteRequest {
                        success: false,
                        duration_ms: 1,
                        exit_code: Some(1),
                        error_category: Some(ErrorCategory::BuildFailure),
                        error_message: Some("mock sick worker".into()),
                        log_tail: Some(
                            "error: builder for mock failed: mock sick worker".into(),
                        ),
                    },
                };

                let tc = Instant::now();
                match client.complete(claim.job_id, claim.claim_id, &req).await {
                    Ok(_) => {
                        state.completes_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        state.completes_err.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(worker = %worker_id, error = %e, "complete failed");
                    }
                }
                state.complete_lat.record(tc.elapsed());
            }
            Ok(None) => {
                // 204 no work within wait window. Normal when fleet
                // idle or when this worker is auto-quarantined.
                state.claims_empty.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                // Transient network / auth / deserialization error.
                // Don't tight-loop the coordinator: small backoff.
                tracing::warn!(worker = %worker_id, error = %e, "claim failed");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}
