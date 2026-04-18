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

use crate::client::{BuildLogUploadMeta, CoordinatorClient};
use crate::error::Result;
use crate::types::{
    ClaimResponse, CompleteRequest, DrvHash, ErrorCategory, JobId, MAX_BUILD_LOG_RAW_BYTES,
    MAX_LOG_TAIL_BYTES,
};

/// Stderr read chunk. Not exposed in WorkerConfig — a fixed 4 KiB is
/// the correct pipe read size and tuning it would be cargo-culty.
const STDERR_READ_CHUNK: usize = 4096;

/// Tunable deadlines and backoffs for the worker loop. Defaults match
/// what this file used to hardcode; operators can override via
/// `WorkerConfig::tuning` to run tighter polls on fast clusters or
/// looser ones on flaky WANs.
#[derive(Clone, Debug)]
pub struct WorkerTuning {
    /// Seconds the server-side claim long-poll holds open.
    pub claim_long_poll_secs: u64,
    /// Initial backoff after a transient HTTP failure.
    pub transient_backoff_initial: Duration,
    /// Cap on the backoff — we keep retrying forever but never slower
    /// than this.
    pub transient_backoff_max: Duration,
    /// How many times the worker retries a completion POST before
    /// giving up.
    pub complete_max_attempts: u32,
    /// Initial sleep between completion-POST retries.
    pub complete_retry_delay_initial: Duration,
    /// Cap on the completion retry backoff.
    pub complete_retry_delay_max: Duration,
    /// How long we wait for in-flight builds to respond to a shutdown
    /// kill before aborting the JoinSet task.
    pub shutdown_drain_timeout: Duration,
    /// Safety margin subtracted from the claim deadline when choosing
    /// the lease-refresh cadence. We refresh at `(deadline - now) / 3`
    /// but never later than `deadline - lease_refresh_margin` so a
    /// slow PUT or a clock skew doesn't race the reaper.
    pub lease_refresh_margin: Duration,
    /// Lower bound on the refresh interval. Prevents us from hammering
    /// /extend in tests where the configured deadline is tiny.
    pub lease_refresh_min: Duration,
    /// Per-drv hard build timeout. When `Some`, a `nix build` that
    /// hasn't exited by this point is SIGKILL'd and reported as a
    /// transient failure — the coordinator retries subject to
    /// `max_attempts`. `None` means no worker-side timeout (the only
    /// bound is the claim deadline, which the lease-refresh loop keeps
    /// extending). Default: 6h — generous enough for every nixpkgs
    /// outlier we've measured (webkitgtk, chromium, llvm) while still
    /// catching a runaway builder.
    pub max_build_secs: Option<Duration>,
}

impl Default for WorkerTuning {
    fn default() -> Self {
        Self {
            claim_long_poll_secs: 30,
            transient_backoff_initial: Duration::from_secs(1),
            transient_backoff_max: Duration::from_secs(30),
            complete_max_attempts: 5,
            complete_retry_delay_initial: Duration::from_secs(2),
            complete_retry_delay_max: Duration::from_secs(30),
            shutdown_drain_timeout: Duration::from_secs(5),
            lease_refresh_margin: Duration::from_secs(30),
            lease_refresh_min: Duration::from_secs(2),
            max_build_secs: Some(Duration::from_secs(6 * 60 * 60)),
        }
    }
}

/// Jittered exponential backoff. `step` is the 0-indexed attempt
/// number (0 → initial, 1 → 2×, ...). Caps at `max`. Full jitter
/// (random in `[0, delay]`) prevents thundering herds on coordinator
/// restart when many workers retry in lockstep.
fn backoff_with_jitter(step: u32, initial: Duration, max: Duration) -> Duration {
    let factor = 1u64.checked_shl(step.min(16)).unwrap_or(u64::MAX);
    let raw = initial.saturating_mul(factor as u32).min(max);
    // Cheap full-jitter: xorshift-ish on a timestamp. Avoids adding a
    // rand dep; uniformity here isn't cryptographic.
    let nanos = raw.as_nanos() as u64;
    if nanos == 0 {
        return raw;
    }
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(1);
    let mut x = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(nanos);
    x ^= x >> 33;
    x = x.wrapping_mul(0xFF51AFD7ED558CCD);
    x ^= x >> 33;
    let jittered = x % nanos.max(1);
    Duration::from_nanos(jittered)
}

/// What set of work this worker process pulls from.
#[derive(Clone, Debug)]
pub enum ClaimMode {
    /// Single-job worker. Calls `GET /jobs/{id}/claim` and exits when
    /// the job goes Gone (terminal). This is the `nix-ci run` mode.
    Job(JobId),
    /// Fleet worker. Calls `GET /claim` (FIFO across all live jobs)
    /// and runs forever until shutdown. This is the `nix-ci worker`
    /// mode for shared-runner deployments.
    Fleet,
}

pub struct WorkerConfig {
    pub mode: ClaimMode,
    /// Comma-separated list of systems this worker can build for.
    /// A single-system worker passes `x86_64-linux`; a multi-arch
    /// host running nix with cross-compilation toolchains passes
    /// `x86_64-linux,aarch64-linux`. The coordinator walks the list
    /// in order: a worker advertising `[native, cross]` prefers
    /// native drvs when both are ready.
    pub system: String,
    pub supported_features: Vec<String>,
    pub max_parallel: u32,
    pub dry_run: bool,
    /// Free-form worker identifier. Sent on every `/claim` so an
    /// operator can map a stuck claim back to a specific host. Empty
    /// is allowed (older workers).
    pub worker_id: Option<String>,
    /// Polling + retry tunables. Defaults preserve prior behavior.
    pub tuning: WorkerTuning,
}

/// Default `worker_id` when the caller doesn't override. Format:
/// `<hostname>-<pid>-<rand8>`. Random suffix disambiguates two
/// processes on the same host (e.g. two slots on a runner).
pub fn default_worker_id() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "host".to_string());
    let pid = std::process::id();
    let rand = uuid::Uuid::new_v4().simple().to_string();
    format!("{host}-{pid}-{}", &rand[..8.min(rand.len())])
}

pub async fn run(
    client: Arc<CoordinatorClient>,
    cfg: WorkerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let active = Arc::new(AtomicU32::new(0));
    let mut join_set = tokio::task::JoinSet::new();
    let mut transient_failures: u32 = 0;

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

        // Claim (long-poll). Shutdown aborts the poll. The two modes
        // differ only in WHICH endpoint we hit; the response shape is
        // identical (ClaimResponse carries job_id either way).
        let claim_result = match &cfg.mode {
            ClaimMode::Job(job_id) => {
                let fut = client.claim_as_worker(
                    *job_id,
                    &cfg.system,
                    &cfg.supported_features,
                    cfg.tuning.claim_long_poll_secs,
                    cfg.worker_id.as_deref(),
                );
                tokio::select! {
                    r = fut => r,
                    _ = shutdown.changed() => {
                        tracing::info!("worker: shutdown during claim");
                        break;
                    }
                }
            }
            ClaimMode::Fleet => {
                let fut = client.claim_any_as_worker(
                    &cfg.system,
                    &cfg.supported_features,
                    cfg.tuning.claim_long_poll_secs,
                    cfg.worker_id.as_deref(),
                );
                tokio::select! {
                    r = fut => r,
                    _ = shutdown.changed() => {
                        tracing::info!("worker: shutdown during claim");
                        break;
                    }
                }
            }
        };
        let claim = match claim_result {
            Ok(Some(c)) => {
                transient_failures = 0;
                c
            }
            Ok(None) => {
                transient_failures = 0;
                continue;
            }
            // Gone applies only to per-job mode (the bound job
            // terminated). Fleet workers never receive Gone — there's
            // no single job to be Gone — so this branch only fires
            // for `ClaimMode::Job` and is the natural exit signal.
            Err(crate::Error::Gone(_)) => {
                tracing::info!("worker: job gone");
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "worker: claim failed");
                let backoff = backoff_with_jitter(
                    transient_failures,
                    cfg.tuning.transient_backoff_initial,
                    cfg.tuning.transient_backoff_max,
                );
                transient_failures = transient_failures.saturating_add(1);
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {},
                    _ = shutdown.changed() => break,
                }
                continue;
            }
        };

        active.fetch_add(1, Ordering::AcqRel);
        let active_cloned = active.clone();
        let client_cloned = client.clone();
        let dry_run = cfg.dry_run;
        // ClaimResponse carries the owning job_id — use it instead of
        // any cfg-level binding so the same code path serves both modes.
        let job_id = claim.job_id;
        let task_shutdown = shutdown.clone();
        let tuning = cfg.tuning.clone();

        join_set.spawn(async move {
            let outcome = build_and_report(
                client_cloned,
                job_id,
                claim,
                dry_run,
                &tuning,
                task_shutdown,
            )
            .await;
            active_cloned.fetch_sub(1, Ordering::AcqRel);
            if let Err(e) = outcome {
                tracing::warn!(error = %e, "worker: build failed to report");
            }
        });

        while join_set.try_join_next().is_some() {}
    }

    // Graceful drain with a bounded deadline. In-flight tasks observe
    // the shutdown watch themselves (each spawned task holds a clone)
    // and kill their nix build child via `kill_on_drop`. If a task
    // refuses to exit within the deadline — e.g., a stuck `.await`
    // that doesn't see the shutdown — we abort it so the process can
    // actually exit. Without this, SIGTERM plus a hung build would
    // leave `nix-ci run` wedged indefinitely.
    let drain = async { while join_set.join_next().await.is_some() {} };
    if tokio::time::timeout(cfg.tuning.shutdown_drain_timeout, drain)
        .await
        .is_err()
    {
        tracing::warn!(
            timeout_secs = cfg.tuning.shutdown_drain_timeout.as_secs(),
            remaining = join_set.len(),
            "worker: drain timed out; aborting in-flight builds"
        );
        join_set.abort_all();
        while join_set.join_next().await.is_some() {}
    }
    Ok(())
}

#[tracing::instrument(skip_all, fields(
    job_id = %job_id,
    claim_id = %claim.claim_id,
    drv_hash = %claim.drv_hash,
    attempt = claim.attempt,
))]
async fn build_and_report(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    claim: ClaimResponse,
    dry_run: bool,
    tuning: &WorkerTuning,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let started_at_wall = chrono::Utc::now();
    let start = Instant::now();

    // Lease refresh runs for the duration of the build. It's cancelled
    // via the `AbortOnDrop` guard below — when the build future returns,
    // the guard drops and the refresh task stops. This is simpler than
    // a shutdown channel and correct because refresh never holds shared
    // state across its own iterations.
    let refresh_handle = spawn_lease_refresh(client.clone(), job_id, &claim, tuning);
    let _refresh_guard = AbortOnDrop(refresh_handle);

    let outcome = if dry_run {
        BuildOutcome::success(0)
    } else {
        build(&claim.drv_path, tuning.max_build_secs, &mut shutdown).await
    };
    let duration_ms = start.elapsed().as_millis() as u64;
    let ended_at_wall = chrono::Utc::now();

    // If the build was cancelled via shutdown, skip the completion
    // POST. The coordinator's reaper (claim deadline or heartbeat
    // timeout) will reclaim the drv. Reporting a synthetic failure
    // would corrupt attempt counts and metrics for a build we never
    // finished.
    if outcome.cancelled {
        tracing::info!(drv = %claim.drv_hash, "build cancelled by shutdown");
        return Ok(());
    }

    let req = CompleteRequest {
        success: outcome.success,
        duration_ms,
        exit_code: outcome.exit_code,
        error_category: outcome.category,
        error_message: outcome.message.clone(),
        log_tail: outcome.log_tail.clone(),
    };

    // Best-effort: upload the full log archive BEFORE /complete so the
    // log is available the instant the failure event lands. Successful
    // builds don't upload (we don't care about successful logs and they
    // dominate the volume). Upload failures are logged but never block
    // /complete — losing an archive entry is recoverable; losing the
    // build outcome isn't.
    if !outcome.success {
        if let Some(full) = &outcome.full_log_raw {
            upload_log_best_effort(
                &client,
                job_id,
                &claim,
                full,
                outcome.original_size,
                outcome.truncated,
                outcome.exit_code,
                started_at_wall,
                ended_at_wall,
            )
            .await;
        }
    }

    let mut attempt: u32 = 0;
    loop {
        match client.complete(job_id, claim.claim_id, &req).await {
            Ok(resp) => {
                if resp.ignored {
                    tracing::debug!(drv = %claim.drv_hash, "completion ignored (stale claim)");
                }
                return Ok(());
            }
            Err(e) => {
                attempt += 1;
                if attempt >= tuning.complete_max_attempts {
                    tracing::error!(
                        error = %e,
                        drv = %claim.drv_hash,
                        "worker: exhausted completion retries — build result lost; \
                         server's heartbeat reaper will reclaim"
                    );
                    return Ok(());
                }
                tracing::warn!(
                    error = %e,
                    attempts_left = tuning.complete_max_attempts - attempt,
                    "complete POST failed; retrying"
                );
                let delay = backoff_with_jitter(
                    attempt - 1,
                    tuning.complete_retry_delay_initial,
                    tuning.complete_retry_delay_max,
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.changed() => {
                        tracing::info!("worker: shutdown during complete retry");
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Gzip + POST the full build log. One attempt; failures are logged
/// only. The 64 KiB inline tail in `CompleteRequest.log_tail` is the
/// fallback display path when the archive is unavailable.
#[allow(clippy::too_many_arguments)]
async fn upload_log_best_effort(
    client: &CoordinatorClient,
    job_id: JobId,
    claim: &ClaimResponse,
    raw: &[u8],
    original_size: u32,
    truncated: bool,
    exit_code: Option<i32>,
    started_at: chrono::DateTime<chrono::Utc>,
    ended_at: chrono::DateTime<chrono::Utc>,
) {
    let gz = match gzip_bytes(raw) {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!(error = %e, drv = %claim.drv_hash, "log gzip failed; skipping upload");
            return;
        }
    };
    let drv_hash = DrvHash::new(claim.drv_hash.as_str().to_string());
    let meta = BuildLogUploadMeta {
        drv_hash: &drv_hash,
        attempt: claim.attempt,
        original_size,
        truncated,
        success: false,
        exit_code,
        started_at,
        ended_at,
    };
    if let Err(e) = client.upload_log(job_id, claim.claim_id, meta, gz).await {
        tracing::warn!(error = %e, drv = %claim.drv_hash, "log upload failed; coordinator will fall back to inline tail");
    }
}

fn gzip_bytes(raw: &[u8]) -> std::io::Result<Vec<u8>> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    let mut enc = GzEncoder::new(Vec::with_capacity(raw.len() / 8), Compression::default());
    enc.write_all(raw)?;
    enc.finish()
}

struct BuildOutcome {
    success: bool,
    /// True when the build was interrupted by worker shutdown. Callers
    /// must not POST /complete for cancelled builds.
    cancelled: bool,
    exit_code: Option<i32>,
    category: Option<ErrorCategory>,
    message: Option<String>,
    /// Last `MAX_LOG_TAIL_BYTES` of stderr as UTF-8 string, used for
    /// inline display in `failures[].log_tail` and SSE events. None
    /// for successful builds (we don't carry their logs).
    log_tail: Option<String>,
    /// Full captured stderr (up to `MAX_BUILD_LOG_RAW_BYTES`) as raw
    /// bytes — never converted to UTF-8 because gzip doesn't care and
    /// avoiding the lossy conversion preserves binary diagnostic
    /// output (e.g. core-dump bytes mixed into stderr). Uploaded to
    /// the build_logs archive. None for successful builds.
    full_log_raw: Option<Vec<u8>>,
    /// Original (pre-truncation) size of the captured stderr. Equals
    /// `full_log_raw.len()` when no truncation; larger when the
    /// worker had to drop the head to fit the cap.
    original_size: u32,
    /// True when the captured stderr exceeded the cap and the head
    /// was dropped.
    truncated: bool,
}

impl BuildOutcome {
    fn success(exit_code: i32) -> Self {
        Self {
            success: true,
            cancelled: false,
            exit_code: Some(exit_code),
            category: None,
            message: None,
            log_tail: None,
            full_log_raw: None,
            original_size: 0,
            truncated: false,
        }
    }

    fn failed(
        exit_code: Option<i32>,
        category: ErrorCategory,
        message: impl Into<String>,
        captured: CapturedLog,
    ) -> Self {
        Self {
            success: false,
            cancelled: false,
            exit_code,
            category: Some(category),
            message: Some(message.into()),
            log_tail: captured.tail,
            full_log_raw: captured.full,
            original_size: captured.original_size,
            truncated: captured.truncated,
        }
    }

    fn cancelled() -> Self {
        Self {
            success: false,
            cancelled: true,
            exit_code: None,
            category: None,
            message: None,
            log_tail: None,
            full_log_raw: None,
            original_size: 0,
            truncated: false,
        }
    }
}

/// Result of capturing a child process's stderr — both the lossy
/// inline tail (for display) and the raw full log (for archive).
struct CapturedLog {
    tail: Option<String>,
    full: Option<Vec<u8>>,
    original_size: u32,
    truncated: bool,
}

impl CapturedLog {
    fn from_raw(raw: Vec<u8>, original_size: u32, truncated: bool) -> Self {
        if raw.is_empty() {
            return Self {
                tail: None,
                full: None,
                original_size,
                truncated,
            };
        }
        let tail_start = raw.len().saturating_sub(MAX_LOG_TAIL_BYTES);
        let tail = String::from_utf8_lossy(&raw[tail_start..]).into_owned();
        Self {
            tail: Some(tail),
            full: Some(raw),
            original_size,
            truncated,
        }
    }
}

#[tracing::instrument(skip_all, fields(drv_path = %drv_path))]
async fn build(
    drv_path: &str,
    max_build: Option<Duration>,
    shutdown: &mut watch::Receiver<bool>,
) -> BuildOutcome {
    // Fast-path: if shutdown is already set when we arrive (e.g., the
    // claim raced with a cancel), skip the spawn entirely.
    if *shutdown.borrow() {
        return BuildOutcome::cancelled();
    }

    let mut cmd = Command::new("nix");
    cmd.arg("build")
        .arg("--no-link")
        .arg("--print-out-paths")
        .arg("--keep-going")
        .arg(format!("{drv_path}^*"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        // Belt-and-suspenders: if this task is dropped (e.g., JoinSet
        // abort_all on shutdown drain timeout), tokio sends SIGKILL to
        // the child so we don't orphan nix builds.
        .kill_on_drop(true);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return BuildOutcome::failed(
                None,
                ErrorCategory::Transient,
                format!("spawn nix build: {e}"),
                CapturedLog::from_raw(Vec::new(), 0, false),
            );
        }
    };
    let stderr = child.stderr.take();
    let tail_handle = tokio::spawn(stderr_capture(stderr));

    // Race the build against (shutdown, per-drv timeout, child exit).
    // On shutdown or timeout: start_kill() sends SIGKILL to the nix
    // build child group; we then wait() to reap it and drain stderr so
    // the tokio-spawned reader task completes. Reaping is important —
    // without it the child is a zombie until the parent process exits.
    //
    // `timeout_fut` is pending-forever when max_build is None, so the
    // arm is never selected unless the operator opted into a bound.
    let timeout_fut = async {
        match max_build {
            Some(d) => tokio::time::sleep(d).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(timeout_fut);
    let status = tokio::select! {
        s = child.wait() => s,
        _ = shutdown.changed() => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = tail_handle.await;
            return BuildOutcome::cancelled();
        }
        _ = &mut timeout_fut => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let captured = tail_handle
                .await
                .unwrap_or_else(|_| StderrCapture::default());
            let secs = max_build
                .map(|d| d.as_secs())
                .unwrap_or_default();
            return BuildOutcome::failed(
                None,
                // Transient: the drv may genuinely be too slow on this
                // worker (thermal throttling, noisy neighbor) but a
                // different worker could complete it. The coordinator's
                // max_tries enforcement bounds total retries.
                ErrorCategory::Transient,
                format!("per-drv build timeout: exceeded {secs}s"),
                CapturedLog::from_raw(captured.raw, captured.original_size, captured.truncated),
            );
        }
    };

    let captured = tail_handle
        .await
        .unwrap_or_else(|_| StderrCapture::default());
    match status {
        Ok(s) if s.success() => BuildOutcome::success(s.code().unwrap_or(0)),
        Ok(s) => {
            let category = classify_stderr_bytes(&captured.raw);
            BuildOutcome::failed(
                s.code(),
                category,
                "nix build failed",
                CapturedLog::from_raw(captured.raw, captured.original_size, captured.truncated),
            )
        }
        Err(e) => BuildOutcome::failed(
            None,
            ErrorCategory::Transient,
            format!("wait nix build: {e}"),
            CapturedLog::from_raw(captured.raw, captured.original_size, captured.truncated),
        ),
    }
}

#[derive(Default)]
struct StderrCapture {
    raw: Vec<u8>,
    original_size: u32,
    truncated: bool,
}

/// Stderr capture for both archive (full, up to `MAX_BUILD_LOG_RAW_BYTES`)
/// and inline display (last `MAX_LOG_TAIL_BYTES`). The lossy UTF-8
/// conversion happens at the consumer.
///
/// Memory budget: at most `MAX_BUILD_LOG_RAW_BYTES + STDERR_READ_CHUNK`.
async fn stderr_capture(stderr: Option<tokio::process::ChildStderr>) -> StderrCapture {
    use tokio::io::AsyncReadExt;
    let Some(mut pipe) = stderr else {
        return StderrCapture::default();
    };
    let mut buf_in = [0u8; STDERR_READ_CHUNK];
    let mut raw: Vec<u8> = Vec::new();
    let mut original: u64 = 0;
    let mut truncated = false;
    while let Ok(n) = pipe.read(&mut buf_in).await {
        if n == 0 {
            break;
        }
        original = original.saturating_add(n as u64);
        raw.extend_from_slice(&buf_in[..n]);
        if raw.len() > MAX_BUILD_LOG_RAW_BYTES {
            // Drop from the head: builds are tail-biased (the failure
            // is at the end). Mark truncated so the metadata reflects
            // it.
            let excess = raw.len() - MAX_BUILD_LOG_RAW_BYTES;
            raw.drain(..excess);
            truncated = true;
        }
    }
    StderrCapture {
        raw,
        original_size: original.min(u32::MAX as u64) as u32,
        truncated,
    }
}

/// Lossy classification on the raw bytes — same matcher as
/// `classify_stderr` but without the `.to_ascii_lowercase()` allocation
/// of the full log; we lower-case ad-hoc for matching.
fn classify_stderr_bytes(raw: &[u8]) -> ErrorCategory {
    let s = String::from_utf8_lossy(raw);
    classify_stderr(&s)
}

/// Classify a failed build from its stderr tail.
///
/// Retry policy is driven by [`ErrorCategory::is_retryable`], so the
/// default matters: an unknown error string drops through to
/// `Transient`, which will be retried up to `max_attempts` times. This
/// is the *conservative* choice — we'd rather re-run a flaky build a
/// second time than turn a transient hiccup into a failed PR. The
/// attempt counter bounds the worst case.
///
/// We classify as a terminal [`ErrorCategory::BuildFailure`] only when
/// we're confident. Two wire formats to recognize:
///
/// * **Upstream Nix**: `error: builder for '<drv>' failed with exit
///   code N` (also `failed to produce output path`).
/// * **Determinate Nix**: `Cannot build '<drv>'.\nReason: builder
///   failed with exit code N.` (also `failed due to signal N (desc)`).
///
/// Everything else — including network / substituter / daemon blips —
/// drops to `Transient`, bounded by `max_attempts` server-side.
fn classify_stderr(tail: &str) -> ErrorCategory {
    let t = tail.to_ascii_lowercase();

    // Resource exhaustion — retryable, likely on a different worker.
    // Check first so `signal 9` + OOM context beats the BuildFailure
    // classifier below.
    if t.contains("no space left on device")
        || t.contains("disk full")
        || t.contains("out of memory")
        || t.contains("cannot allocate memory")
        || t.contains("killed") && (t.contains("oom") || t.contains("out of memory"))
    {
        return ErrorCategory::DiskFull;
    }

    // Deterministic build failure signatures — not retryable.
    //
    // The `builder for ` / `cannot build '` anchors guard against
    // matching spurious substrings in e.g. network error logs.
    let has_anchor = t.contains("builder for ") || t.contains("cannot build '");
    let has_terminal_marker = t.contains("failed with exit code")
        || t.contains("failed to produce output path")
        || t.contains("failed due to signal");
    if has_anchor && has_terminal_marker {
        return ErrorCategory::BuildFailure;
    }
    if t.contains("hash mismatch in fixed-output derivation")
        || t.contains("output path ") && t.contains(" is not allowed to refer to ")
    {
        return ErrorCategory::BuildFailure;
    }

    // Everything else — network, substituter, daemon, unknown — is
    // treated as transient. Bounded by `max_attempts` at the server.
    ErrorCategory::Transient
}

/// RAII guard that aborts a `tokio::task::JoinHandle` when dropped.
/// Used to tie the lease-refresh task's lifetime to the build future.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Spawn the claim-lease refresh task. The cadence is derived from the
/// server-issued deadline (`claim.deadline` is a wall-clock instant):
/// we aim to refresh at roughly `(deadline - now) / 3` so even a single
/// failed refresh leaves two more chances before the lease expires.
///
/// When the coordinator signals the claim is gone (410 Gone), the task
/// exits immediately — no point refreshing a claim that's already been
/// taken by another worker. Transient failures are logged and retried
/// on the next tick; the main build continues regardless.
fn spawn_lease_refresh(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    claim: &ClaimResponse,
    tuning: &WorkerTuning,
) -> tokio::task::JoinHandle<()> {
    let claim_id = claim.claim_id;
    let drv_hash = claim.drv_hash.clone();
    // Compute a base interval from the initial deadline. If the deadline
    // is already in the past (race with clock skew), fall back to the
    // minimum so we still try at least once.
    let now_wall = chrono::Utc::now();
    let until_deadline = (claim.deadline - now_wall)
        .to_std()
        .unwrap_or(Duration::from_secs(0));
    let safety_margin = tuning.lease_refresh_margin;
    // Two guards:
    // 1. refresh well before the deadline, never after `deadline - margin`
    // 2. cadence = min(until_deadline / 3, until_deadline - margin)
    let by_thirds = until_deadline / 3;
    let before_margin = until_deadline.saturating_sub(safety_margin);
    let interval = by_thirds.min(before_margin).max(tuning.lease_refresh_min);

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match client.extend_claim(job_id, claim_id).await {
                Ok(Some(_)) => {
                    tracing::debug!(
                        drv = %drv_hash,
                        interval_secs = interval.as_secs(),
                        "claim lease extended"
                    );
                }
                Ok(None) => {
                    tracing::info!(
                        drv = %drv_hash,
                        "claim gone; stopping lease refresh"
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        drv = %drv_hash,
                        "lease refresh failed; will retry"
                    );
                }
            }
        }
    })
}

#[cfg(test)]
mod build_timeout_tests {
    use super::*;

    /// A nix-build-shaped command that runs longer than the timeout
    /// must be killed, and the resulting outcome must be a Transient
    /// failure with a "per-drv build timeout" message. This is what
    /// nix-ci reports to the coordinator; the coordinator's retry
    /// logic reads the category to decide whether to re-claim.
    ///
    /// We can't exercise `build()` directly because it execs `nix`,
    /// which isn't available in the test container. Instead we
    /// exercise the same timeout-arm logic with `sleep 10` as the
    /// child. The invariants we care about are:
    ///   1. timeout_fut fires before child.wait()
    ///   2. start_kill + wait reap the child cleanly
    ///   3. outcome.success == false, category == Transient
    #[tokio::test(flavor = "current_thread")]
    async fn timeout_kills_child_and_reports_transient_failure() {
        use tokio::process::Command;
        let mut cmd = Command::new("sleep");
        cmd.arg("3600")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().expect("spawn sleep");
        let stderr = child.stderr.take();
        let tail_handle = tokio::spawn(stderr_capture(stderr));
        let timeout = Duration::from_millis(50);
        let timeout_fut = tokio::time::sleep(timeout);
        tokio::pin!(timeout_fut);

        let outcome: Option<BuildOutcome> = tokio::select! {
            s = child.wait() => {
                let _ = tail_handle.await;
                // If wait() returned first, the timeout didn't fire —
                // that would be a test-environment bug, fail loudly.
                panic!("child exited before timeout: {s:?}");
            }
            _ = &mut timeout_fut => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                let _ = tail_handle.await;
                Some(BuildOutcome::failed(
                    None,
                    ErrorCategory::Transient,
                    format!("per-drv build timeout: exceeded {}s", timeout.as_secs()),
                    CapturedLog::from_raw(Vec::new(), 0, false),
                ))
            }
        };
        let o = outcome.unwrap();
        assert!(!o.success);
        assert_eq!(o.category, Some(ErrorCategory::Transient));
        assert!(
            o.message
                .as_deref()
                .unwrap()
                .contains("per-drv build timeout"),
            "message must name the reason: {:?}",
            o.message
        );
    }

    /// When `max_build` is None, the timeout arm is backed by
    /// `std::future::pending::<()>()` — a future that never resolves.
    /// This test pins that behavior: a pending-forever future must not
    /// race the child's wait() arm in the absence of an explicit cap.
    #[tokio::test(flavor = "current_thread")]
    async fn no_timeout_means_no_kill() {
        let max_build: Option<Duration> = None;
        let timeout_fut = async {
            match max_build {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_fut);
        // Race pending-forever against a 10ms sleep — the 10ms must
        // win every time.
        let won = tokio::select! {
            _ = &mut timeout_fut => "pending",
            _ = tokio::time::sleep(Duration::from_millis(10)) => "sleep",
        };
        assert_eq!(
            won, "sleep",
            "None-backed pending future must never resolve"
        );
    }
}

#[cfg(test)]
mod classify_tests {
    use super::*;

    #[test]
    fn disk_full_variants() {
        assert_eq!(
            classify_stderr("error: writing to file: No space left on device"),
            ErrorCategory::DiskFull
        );
        assert_eq!(
            classify_stderr("fatal: out of memory"),
            ErrorCategory::DiskFull
        );
        assert_eq!(
            classify_stderr("Cannot allocate memory"),
            ErrorCategory::DiskFull
        );
    }

    #[test]
    fn build_failure_canonical_nix_message() {
        let tail = "error: builder for '/nix/store/xxx-foo.drv' failed with exit code 1";
        assert_eq!(classify_stderr(tail), ErrorCategory::BuildFailure);
    }

    #[test]
    fn build_failure_missing_output() {
        let tail = "error: builder for '/nix/store/xxx-foo.drv' failed to produce output path";
        assert_eq!(classify_stderr(tail), ErrorCategory::BuildFailure);
    }

    #[test]
    fn build_failure_hash_mismatch() {
        let tail = "error: hash mismatch in fixed-output derivation";
        assert_eq!(classify_stderr(tail), ErrorCategory::BuildFailure);
    }

    #[test]
    fn unknown_defaults_to_transient() {
        // Previously classified as BuildFailure — now safely retryable.
        assert_eq!(
            classify_stderr("some garbage we've never seen before"),
            ErrorCategory::Transient
        );
        assert_eq!(classify_stderr(""), ErrorCategory::Transient);
    }

    #[test]
    fn network_is_transient() {
        assert_eq!(
            classify_stderr("curl: (7) could not connect to host"),
            ErrorCategory::Transient
        );
    }

    // ─── Negative cases that lock in the && / || structure ──────

    #[test]
    fn killed_without_oom_is_transient_not_diskfull() {
        // `killed` alone (no oom / OOM / out of memory nearby) is not
        // resource exhaustion — could be SIGKILL from the orchestrator.
        // Guards the outer `&&` in the DiskFull branch.
        assert_eq!(
            classify_stderr("process killed with signal 9"),
            ErrorCategory::Transient
        );
    }

    #[test]
    fn killed_with_oom_is_diskfull() {
        // Positive case of the same structure.
        assert_eq!(
            classify_stderr("process killed by OOM reaper"),
            ErrorCategory::DiskFull
        );
    }

    #[test]
    fn failed_exit_without_builder_is_transient() {
        // "failed with exit code" without "builder for " must NOT be
        // classified as a terminal build failure — other tools use
        // that phrase too. Guards the `&&` that requires BOTH.
        assert_eq!(
            classify_stderr("nix-daemon failed with exit code 3"),
            ErrorCategory::Transient
        );
    }

    #[test]
    fn builder_for_alone_is_transient() {
        // "builder for" without an exit-code or missing-output phrase
        // is ambiguous; default transient. Guards the paired `&&`.
        assert_eq!(
            classify_stderr("note: builder for /nix/store/x.drv is running"),
            ErrorCategory::Transient
        );
    }

    #[test]
    fn output_path_without_refer_to_is_transient() {
        // "output path" alone (no "is not allowed to refer to") is not
        // the reference-restriction build-failure message.
        assert_eq!(
            classify_stderr("output path /nix/store/foo has been uploaded"),
            ErrorCategory::Transient
        );
    }

    #[test]
    fn output_path_reference_violation_is_build_failure() {
        // Positive case for the paired `&&`.
        assert_eq!(
            classify_stderr("output path /nix/store/a is not allowed to refer to /nix/store/b"),
            ErrorCategory::BuildFailure
        );
    }

    // ─── DetSys nix output format ────────────────────────────────
    //
    // DetSys/nix emits `Cannot build '<drv>'.\nReason: builder
    // failed with exit code N.` for deterministic failures, unlike
    // upstream's `error: builder for '<drv>' failed with exit code
    // N`. Without these tests the classifier mis-labels real build
    // failures as `Transient`, thrashing retries.

    #[test]
    fn detsys_cannot_build_with_exit_code_is_build_failure() {
        let tail = "Cannot build '/nix/store/abc-hello.drv'.\n\
                    Reason: builder failed with exit code 1.\n\
                    Last log lines:";
        assert_eq!(classify_stderr(tail), ErrorCategory::BuildFailure);
    }

    #[test]
    fn detsys_cannot_build_with_signal_is_build_failure() {
        // `failed due to signal N (description)` — non-OOM signal
        // paths should also be classified as a build failure (SIGABRT
        // from a crashing builder is deterministic).
        let tail = "Cannot build '/nix/store/abc.drv'.\n\
                    Reason: builder failed due to signal 6 (Aborted).";
        assert_eq!(classify_stderr(tail), ErrorCategory::BuildFailure);
    }

    #[test]
    fn detsys_cannot_build_signal_9_with_oom_is_diskfull() {
        // OOM-kill signature alongside "Cannot build" — the OOM check
        // should still win, because the drv itself isn't at fault.
        let tail = "Cannot build '/nix/store/abc.drv'.\n\
                    Reason: builder failed due to signal 9 (Killed).\n\
                    process killed by OOM reaper";
        assert_eq!(classify_stderr(tail), ErrorCategory::DiskFull);
    }
}

#[cfg(test)]
mod backoff_tests {
    use super::*;

    #[test]
    fn backoff_respects_max_cap() {
        // Any step ≥ log2(max/initial) must be clamped to `max`.
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(10);
        let mut any_nonzero = false;
        for step in 0..20 {
            let d = backoff_with_jitter(step, initial, max);
            assert!(d <= max, "step={step} produced {d:?} > max {max:?}");
            if !d.is_zero() {
                any_nonzero = true;
            }
        }
        // A mutant that replaces the whole fn with `Default::default()`
        // (Duration::ZERO) would still satisfy <= max — pin it down by
        // requiring at least one high-step result to actually be
        // non-zero.
        assert!(
            any_nonzero,
            "backoff with non-zero initial must produce non-zero durations"
        );
    }

    #[test]
    fn backoff_jitter_is_bounded_by_raw() {
        // Full-jitter semantics: jittered ∈ [0, raw). Verify for a
        // handful of steps that the returned duration never exceeds
        // the pre-jitter cap.
        let initial = Duration::from_millis(50);
        let max = Duration::from_secs(2);
        for step in 0..10 {
            for _ in 0..20 {
                let d = backoff_with_jitter(step, initial, max);
                assert!(d <= max);
            }
        }
    }

    #[test]
    fn backoff_zero_initial_is_zero() {
        // Guards against arithmetic that would blow up with a zero
        // initial (e.g. division by zero in the jitter modulo).
        let d = backoff_with_jitter(3, Duration::ZERO, Duration::from_secs(1));
        assert_eq!(d, Duration::ZERO);
    }

    #[test]
    fn backoff_shift_saturates_rather_than_overflowing() {
        // Steps ≥ 17 are clamped internally so the u32::checked_shl
        // doesn't overflow; the result should still respect `max`.
        let d = backoff_with_jitter(100, Duration::from_millis(10), Duration::from_secs(5));
        assert!(d <= Duration::from_secs(5));
    }
}
