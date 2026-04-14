//! Worker loop: self-regulating claim → build → complete cycle.
//!
//! Shutdown is a `tokio::sync::watch<bool>` so we never lose a signal
//! to the `Notify::notified().now_or_never()` race — shutdown is a
//! level-triggered value, not an edge-triggered notification.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::process::Command;
use tokio::sync::watch;

use crate::client::CoordinatorClient;
use crate::error::Result;
use crate::types::{ClaimResponse, CompleteRequest, ErrorCategory, JobId, MAX_LOG_TAIL_BYTES};

/// Seconds the server-side claim long-poll holds open.
const CLAIM_LONG_POLL_SECS: u64 = 30;
/// Sleep between retries of a failed HTTP call.
const TRANSIENT_BACKOFF: Duration = Duration::from_secs(1);
/// How many times the worker retries a completion POST before giving up.
const COMPLETE_MAX_ATTEMPTS: u32 = 5;
/// Sleep between completion-POST retries.
const COMPLETE_RETRY_DELAY: Duration = Duration::from_secs(2);
/// Hard cap on build_and_report recursion (via `next_build`).
const NEXT_BUILD_CHAIN_CAP: u32 = 64;
/// Stderr read chunk.
const STDERR_READ_CHUNK: usize = 4096;

pub struct WorkerConfig {
    pub job_id: JobId,
    pub system: String,
    pub supported_features: Vec<String>,
    pub max_parallel: u32,
    pub dry_run: bool,
}

pub async fn run(
    client: Arc<CoordinatorClient>,
    cfg: WorkerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let active = Arc::new(AtomicU32::new(0));
    let mut join_set = tokio::task::JoinSet::new();

    loop {
        if *shutdown.borrow() {
            tracing::info!("worker: shutdown signalled");
            break;
        }

        let in_flight = active.load(Ordering::Acquire);
        if in_flight >= cfg.max_parallel {
            // Wait for a task slot or a shutdown edge.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                _ = shutdown.changed() => {},
            }
            continue;
        }

        // Claim (long-poll). Shutdown aborts the poll.
        let claim_fut = client.claim(
            cfg.job_id,
            &cfg.system,
            &cfg.supported_features,
            CLAIM_LONG_POLL_SECS,
        );
        let claim_result = tokio::select! {
            r = claim_fut => r,
            _ = shutdown.changed() => {
                tracing::info!("worker: shutdown during claim");
                break;
            }
        };
        let claim = match claim_result {
            Ok(Some(c)) => c,
            Ok(None) => continue,
            Err(crate::Error::Gone(_)) => {
                tracing::info!("worker: job gone");
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "worker: claim failed");
                tokio::time::sleep(TRANSIENT_BACKOFF).await;
                continue;
            }
        };

        active.fetch_add(1, Ordering::AcqRel);
        let active_cloned = active.clone();
        let client_cloned = client.clone();
        let dry_run = cfg.dry_run;
        let job_id = cfg.job_id;

        join_set.spawn(async move {
            let outcome = build_and_report(client_cloned, job_id, claim, dry_run).await;
            active_cloned.fetch_sub(1, Ordering::AcqRel);
            if let Err(e) = outcome {
                tracing::warn!(error = %e, "worker: build failed to report");
            }
        });

        while join_set.try_join_next().is_some() {}
    }

    while join_set.join_next().await.is_some() {}
    Ok(())
}

async fn build_and_report(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    mut claim: ClaimResponse,
    dry_run: bool,
) -> Result<()> {
    let mut chain_depth = 0u32;
    loop {
        let start = Instant::now();
        let outcome = if dry_run {
            BuildOutcome::success(0)
        } else {
            build(&claim.drv_path).await
        };
        let duration_ms = start.elapsed().as_millis() as u64;

        let req = CompleteRequest {
            success: outcome.success,
            duration_ms,
            exit_code: outcome.exit_code,
            error_category: outcome.category,
            error_message: outcome.message.clone(),
            log_tail: outcome.log_tail.clone(),
        };

        let mut remaining = COMPLETE_MAX_ATTEMPTS;
        let response = loop {
            match client.complete(job_id, claim.claim_id, &req).await {
                Ok(resp) => break Some(resp),
                Err(e) => {
                    remaining -= 1;
                    if remaining == 0 {
                        tracing::error!(
                            error = %e,
                            drv = %claim.drv_hash,
                            "worker: exhausted completion retries — build result lost; \
                             server's heartbeat reaper will reclaim"
                        );
                        break None;
                    }
                    tracing::warn!(
                        error = %e,
                        attempts_left = remaining,
                        "complete POST failed; retrying"
                    );
                    tokio::time::sleep(COMPLETE_RETRY_DELAY).await;
                }
            }
        };

        let Some(response) = response else {
            return Ok(());
        };
        if response.ignored {
            tracing::debug!(drv = %claim.drv_hash, "completion ignored (stale claim)");
        }
        match response.next_build {
            Some(next) if !next.drv_path.is_empty() && chain_depth < NEXT_BUILD_CHAIN_CAP => {
                chain_depth += 1;
                claim = next;
                continue;
            }
            _ => return Ok(()),
        }
    }
}

struct BuildOutcome {
    success: bool,
    exit_code: Option<i32>,
    category: Option<ErrorCategory>,
    message: Option<String>,
    log_tail: Option<String>,
}

impl BuildOutcome {
    fn success(exit_code: i32) -> Self {
        Self {
            success: true,
            exit_code: Some(exit_code),
            category: None,
            message: None,
            log_tail: None,
        }
    }

    fn failed(
        exit_code: Option<i32>,
        category: ErrorCategory,
        message: impl Into<String>,
        log_tail: Option<String>,
    ) -> Self {
        Self {
            success: false,
            exit_code,
            category: Some(category),
            message: Some(message.into()),
            log_tail,
        }
    }
}

async fn build(drv_path: &str) -> BuildOutcome {
    let mut cmd = Command::new("nix");
    cmd.arg("build")
        .arg("--no-link")
        .arg("--print-out-paths")
        .arg("--keep-going")
        .arg(format!("{drv_path}^*"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return BuildOutcome::failed(
                None,
                ErrorCategory::Transient,
                format!("spawn nix build: {e}"),
                None,
            );
        }
    };
    let stderr = child.stderr.take();
    let tail_handle = tokio::spawn(stderr_ring_tail(stderr));

    let status = child.wait().await;
    let tail = tail_handle.await.unwrap_or_default();
    match status {
        Ok(s) if s.success() => BuildOutcome::success(s.code().unwrap_or(0)),
        Ok(s) => BuildOutcome::failed(
            s.code(),
            classify_stderr(&tail),
            "nix build failed",
            Some(tail),
        ),
        Err(e) => BuildOutcome::failed(
            None,
            ErrorCategory::Transient,
            format!("wait nix build: {e}"),
            Some(tail),
        ),
    }
}

/// Ring-buffered stderr tail. Keeps at most `MAX_LOG_TAIL_BYTES` and
/// converts to UTF-8 (lossy) at close. Never allocates more than
/// `2 * MAX_LOG_TAIL_BYTES + STDERR_READ_CHUNK`.
async fn stderr_ring_tail(stderr: Option<tokio::process::ChildStderr>) -> String {
    use tokio::io::AsyncReadExt;
    let Some(mut pipe) = stderr else {
        return String::new();
    };
    let mut tail: Vec<u8> = Vec::with_capacity(MAX_LOG_TAIL_BYTES);
    let mut buf = [0u8; STDERR_READ_CHUNK];
    while let Ok(n) = pipe.read(&mut buf).await {
        if n == 0 {
            break;
        }
        tail.extend_from_slice(&buf[..n]);
        if tail.len() > MAX_LOG_TAIL_BYTES {
            let excess = tail.len() - MAX_LOG_TAIL_BYTES;
            tail.drain(..excess);
        }
    }
    String::from_utf8_lossy(&tail).into_owned()
}

fn classify_stderr(tail: &str) -> ErrorCategory {
    let t = tail.to_ascii_lowercase();
    if t.contains("no space left on device")
        || t.contains("disk full")
        || t.contains("out of memory")
    {
        ErrorCategory::DiskFull
    } else if t.contains("temporary failure")
        || t.contains("network is unreachable")
        || t.contains("connection reset")
        || t.contains("could not connect")
    {
        ErrorCategory::Transient
    } else {
        ErrorCategory::BuildFailure
    }
}
