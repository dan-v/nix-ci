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
/// Initial backoff after a transient HTTP failure.
const TRANSIENT_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
/// Cap on the backoff — we keep retrying forever but never slower than this.
const TRANSIENT_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// How many times the worker retries a completion POST before giving up.
const COMPLETE_MAX_ATTEMPTS: u32 = 5;
/// Initial sleep between completion-POST retries.
const COMPLETE_RETRY_DELAY_INITIAL: Duration = Duration::from_secs(2);
/// Cap on the completion retry backoff.
const COMPLETE_RETRY_DELAY_MAX: Duration = Duration::from_secs(30);
/// Stderr read chunk.
const STDERR_READ_CHUNK: usize = 4096;
/// How long we wait for in-flight builds to respond to a shutdown kill
/// before aborting the JoinSet task. A well-behaved child should exit
/// within a second of SIGKILL; this gives it headroom for stderr drain.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

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
            Ok(Some(c)) => {
                transient_failures = 0;
                c
            }
            Ok(None) => {
                transient_failures = 0;
                continue;
            }
            Err(crate::Error::Gone(_)) => {
                tracing::info!("worker: job gone");
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "worker: claim failed");
                let backoff = backoff_with_jitter(
                    transient_failures,
                    TRANSIENT_BACKOFF_INITIAL,
                    TRANSIENT_BACKOFF_MAX,
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
        let job_id = cfg.job_id;
        let task_shutdown = shutdown.clone();

        join_set.spawn(async move {
            let outcome =
                build_and_report(client_cloned, job_id, claim, dry_run, task_shutdown).await;
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
    if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, drain)
        .await
        .is_err()
    {
        tracing::warn!(
            timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
            remaining = join_set.len(),
            "worker: drain timed out; aborting in-flight builds"
        );
        join_set.abort_all();
        while join_set.join_next().await.is_some() {}
    }
    Ok(())
}

async fn build_and_report(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    claim: ClaimResponse,
    dry_run: bool,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let start = Instant::now();
    let outcome = if dry_run {
        BuildOutcome::success(0)
    } else {
        build(&claim.drv_path, &mut shutdown).await
    };
    let duration_ms = start.elapsed().as_millis() as u64;

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
                if attempt >= COMPLETE_MAX_ATTEMPTS {
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
                    attempts_left = COMPLETE_MAX_ATTEMPTS - attempt,
                    "complete POST failed; retrying"
                );
                let delay = backoff_with_jitter(
                    attempt - 1,
                    COMPLETE_RETRY_DELAY_INITIAL,
                    COMPLETE_RETRY_DELAY_MAX,
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

struct BuildOutcome {
    success: bool,
    /// True when the build was interrupted by worker shutdown. Callers
    /// must not POST /complete for cancelled builds.
    cancelled: bool,
    exit_code: Option<i32>,
    category: Option<ErrorCategory>,
    message: Option<String>,
    log_tail: Option<String>,
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
            cancelled: false,
            exit_code,
            category: Some(category),
            message: Some(message.into()),
            log_tail,
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
        }
    }
}

async fn build(drv_path: &str, shutdown: &mut watch::Receiver<bool>) -> BuildOutcome {
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
                None,
            );
        }
    };
    let stderr = child.stderr.take();
    let tail_handle = tokio::spawn(stderr_ring_tail(stderr));

    // Race the build against shutdown. On shutdown, start_kill() sends
    // SIGKILL to the nix build child group; we then wait() to reap it
    // and drain stderr so the tokio-spawned reader task completes.
    // Reaping is important — without it the child is a zombie until
    // the parent process exits.
    let status = tokio::select! {
        s = child.wait() => s,
        _ = shutdown.changed() => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = tail_handle.await;
            return BuildOutcome::cancelled();
        }
    };

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
/// we're confident: Nix prints "error: builder for ... failed with
/// exit code ..." for an actual build-script failure, and similar for
/// hash/output mismatches. Those are reproducibility-deterministic and
/// retrying won't change the outcome.
fn classify_stderr(tail: &str) -> ErrorCategory {
    let t = tail.to_ascii_lowercase();

    // Resource exhaustion — retryable, likely on a different worker.
    if t.contains("no space left on device")
        || t.contains("disk full")
        || t.contains("out of memory")
        || t.contains("cannot allocate memory")
        || t.contains("killed") && (t.contains("oom") || t.contains("out of memory"))
    {
        return ErrorCategory::DiskFull;
    }

    // Deterministic build failure signatures — not retryable.
    // These are Nix's canonical terminal-failure messages.
    if t.contains("builder for ")
        && (t.contains("failed with exit code") || t.contains("failed to produce output path"))
    {
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
