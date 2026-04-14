//! End-to-end `nix-ci run` orchestrator. Uses `watch<bool>` for
//! shutdown so every task reliably sees the signal.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::client::CoordinatorClient;
use crate::config::RunnerConfig;
use crate::error::{Error, Result};
use crate::runner::eval_jobs::EvalMode;
use crate::runner::worker::WorkerConfig;
use crate::runner::{fallback, sse, submitter, worker};
use crate::types::{CreateJobRequest, JobId, JobStatus};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

pub struct RunArgs {
    pub mode: EvalMode,
    pub cfg: RunnerConfig,
    pub external_ref: Option<String>,
}

pub struct RunOutcome {
    pub status: JobStatus,
}

pub async fn run(args: RunArgs) -> Result<RunOutcome> {
    let client = Arc::new(CoordinatorClient::new(args.cfg.coordinator_url.clone()));

    // Step 1: try to create the job. Fall back on transport failure at startup.
    let job = match client
        .create_job(&CreateJobRequest {
            external_ref: args.external_ref.clone(),
        })
        .await
    {
        Ok(r) => r,
        Err(e) if args.cfg.fallback_on_unreachable => {
            tracing::warn!(error = %e, "coordinator unreachable — local fallback");
            fallback::run_local(args.mode, args.cfg.eval_workers).await?;
            return Ok(RunOutcome {
                status: JobStatus::Done,
            });
        }
        Err(e) => return Err(e),
    };
    let job_id = job.id;
    tracing::info!(%job_id, "job created");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // SSE listener
    let sse_handle = {
        let client = client.clone();
        let rx = shutdown_rx.clone();
        tokio::spawn(async move { sse::print_events(client, job_id, rx).await })
    };

    // Worker loop
    let worker_handle = {
        let client = client.clone();
        let rx = shutdown_rx.clone();
        let cfg = WorkerConfig {
            job_id,
            system: args.cfg.system.clone(),
            supported_features: args.cfg.supported_features.clone(),
            max_parallel: args.cfg.max_parallel,
            dry_run: args.cfg.dry_run,
        };
        tokio::spawn(async move { worker::run(client, cfg, rx).await })
    };

    // Heartbeat
    let heartbeat_handle = {
        let client = client.clone();
        let rx = shutdown_rx.clone();
        tokio::spawn(async move { run_heartbeat(client, job_id, rx).await })
    };

    // Drive the submitter in-line
    let crate::runner::eval_jobs::Spawned {
        rx: eval_rx,
        handle: eval_handle,
        kill: eval_kill,
    } = crate::runner::eval_jobs::spawn(args.mode, args.cfg.eval_workers)?;

    // On shutdown mid-eval, kill the child so `nix-ci run` doesn't
    // hang waiting for nix-eval-jobs to finish.
    let shutdown_kill_rx = shutdown_rx.clone();
    let eval_kill_guard = tokio::spawn(async move {
        let mut rx = shutdown_kill_rx;
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                eval_kill.kill().await;
                break;
            }
        }
    });

    let submit_stats = submitter::run(client.clone(), job_id, eval_rx).await?;
    tracing::info!(
        new = submit_stats.new_drvs,
        deduped = submit_stats.dedup_skipped,
        cached = submit_stats.cached_skipped,
        eval_errors = submit_stats.eval_errors,
        errors = submit_stats.errors,
        "submitter done"
    );

    eval_handle
        .await
        .map_err(|e| Error::Internal(format!("eval task panic: {e}")))??;
    eval_kill_guard.abort();

    client.seal(job_id).await.ok();

    let status = sse_handle
        .await
        .map_err(|e| Error::Internal(format!("sse task panic: {e}")))??;

    // Signal shutdown; worker and heartbeat wind down promptly.
    let _ = shutdown_tx.send(true);
    let _ = worker_handle.await;
    let _ = heartbeat_handle.await;

    Ok(RunOutcome { status })
}

async fn run_heartbeat(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
    ticker.tick().await; // immediate
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }
        match client.heartbeat(job_id).await {
            Ok(_) => {}
            Err(Error::Gone(_)) => {
                tracing::info!("job gone; stopping heartbeat");
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "heartbeat failed");
            }
        }
    }
}
