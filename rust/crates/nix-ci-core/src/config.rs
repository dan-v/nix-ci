//! Runtime configuration. Parsed by the CLI, consumed by the server
//! and runner.
//!
//! Layering for `ServerConfig`:
//!   defaults  <  JSON config file  <  env vars  <  CLI flags
//!
//! All knobs are surfaced via the JSON file (operator-friendly central
//! tuning); the historical CLI/env-var subset still works (clap-managed)
//! and overrides whatever the file says. `ServerConfig::validate()`
//! catches obvious nonsense before the server boots.

use std::net::SocketAddr;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub database_url: String,
    pub listen: SocketAddr,
    /// Postgres advisory-lock key used to enforce single-writer. A
    /// deterministic constant so cross-host failover works.
    pub lock_key: i64,
    /// Per-drv hard build deadline (no matter how healthy heartbeats are).
    pub claim_deadline_secs: u64,
    /// Reap stale jobs whose last_heartbeat is older than this.
    pub job_heartbeat_timeout_secs: u64,
    /// Interval between reaper ticks.
    pub reaper_interval_secs: u64,
    /// Interval between cleanup ticks.
    pub cleanup_interval_secs: u64,
    /// Retention for done jobs + drvs (dedup cache window).
    pub retention_days: u32,
    /// SSE broadcast channel capacity per submission. Larger = more
    /// memory per job but tolerance of slower clients. A `Lagged`
    /// event is emitted when this overflows.
    pub submission_event_capacity: usize,
    /// How often SSE emits a `Progress` event with live counts.
    pub progress_tick_secs: u64,
    /// HTTP/1.1 keepalive ticks for SSE streams.
    pub sse_keepalive_secs: u64,
    /// Cap on the `wait` parameter of `/jobs/{id}/claim`.
    pub max_claim_wait_secs: u64,
    /// Backoff step for flaky retries. Total backoff is
    /// `step_ms × attempt`.
    pub flaky_retry_backoff_step_ms: i64,
    /// Maximum build attempts per drv before a retryable failure becomes
    /// terminal. Applies to fresh Step creations; in-flight Steps keep
    /// the value captured at ingest.
    pub max_attempts: i32,
    /// Max HTTP request body in bytes. A deliberately generous default
    /// (64 MiB) for large-DAG batch ingests; returns 413 above this.
    /// Primarily guards against mis-configured or hostile clients.
    pub max_request_body_bytes: usize,
    /// Max `drv_path` length accepted on ingest. Nix store paths are
    /// bounded by filesystem constraints in practice; a very long path
    /// is almost always a bug. Over-long drvs return 400.
    pub max_drv_path_bytes: usize,
    /// Max `drv_name` length accepted on ingest.
    pub max_drv_name_bytes: usize,
    /// Cap on the `failures` vector inside a terminal `jobs.result`
    /// snapshot. Prevents a catastrophic job from producing a multi-MB
    /// JSONB row. A truncated marker is appended if exceeded.
    pub max_failures_in_result: usize,
    /// How long to wait for axum to finish draining in-flight requests
    /// and for background tasks (reaper / cleanup) to observe the
    /// shutdown signal before we force the process to exit. Without a
    /// bound, a stuck handler can wedge SIGTERM indefinitely; systemd
    /// would eventually SIGKILL, leaving Postgres transactions to
    /// roll back uncleanly.
    pub graceful_shutdown_secs: u64,
    /// Per-attempt build log retention (days). Logs are pruned by the
    /// cleanup loop independently of `retention_days` because they're
    /// the fattest bytes per row — typically you want shorter log
    /// retention than job-metadata retention.
    pub build_log_retention_days: u32,
    /// Soft warning threshold for per-submission member count. When a
    /// live submission's member count crosses this, the dispatcher
    /// emits a `WARN` log line so an operator can spot a runaway job
    /// before it OOMs the coordinator. Not a hard cap.
    pub submission_warn_threshold: u32,
    /// Hard cap on per-submission member count. Ingest that would cross
    /// this rejects the entire batch with 413 and fails the job with
    /// `eval_too_large`. The dispatcher is then drained, so runaway
    /// evaluations can never OOM the coordinator. Set `None` to disable
    /// the hard cap (not recommended in production). Defaults to 2M —
    /// headroom over nixpkgs-scale full evals (~150K drvs today)
    /// while still catching true bugs.
    pub max_drvs_per_job: Option<u32>,
    /// TTL (in seconds) applied to newly-inserted `failed_outputs`
    /// rows. Concurrent / subsequent jobs that ingest the same output
    /// path within the TTL skip rebuilding. Lower = retry flaky drvs
    /// sooner; higher = avoid thrashing on a known-broken drv. Only
    /// affects new inserts; existing rows keep their original TTL.
    pub failed_outputs_ttl_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/nix_ci".to_string(),
            listen: "127.0.0.1:8080".parse().unwrap(),
            lock_key: NIX_CI_COORDINATOR_LOCK_KEY,
            claim_deadline_secs: 60 * 60, // 1h — dramatically faster tail recovery than the old 2h, still generous for the long nixpkgs outliers (webkitgtk, chromium). Per-job overridable via CreateJobRequest.claim_deadline_secs.
            job_heartbeat_timeout_secs: 30,
            reaper_interval_secs: 15,
            cleanup_interval_secs: 5 * 60,
            retention_days: 7,
            submission_event_capacity: 4096,
            progress_tick_secs: 10,
            sse_keepalive_secs: 15,
            max_claim_wait_secs: 60,
            flaky_retry_backoff_step_ms: 30_000,
            max_attempts: 2,
            max_request_body_bytes: 64 * 1024 * 1024, // 64 MiB
            max_drv_path_bytes: 4096,
            max_drv_name_bytes: 1024,
            max_failures_in_result: 500,
            graceful_shutdown_secs: 30,
            build_log_retention_days: 14,
            submission_warn_threshold: 200_000,
            max_drvs_per_job: Some(2_000_000),
            failed_outputs_ttl_secs: 60 * 60, // 1h
        }
    }
}

/// `b'n' 'i' 'x' 'c' 'i' 0 0 1` interpreted as a big-endian i64 —
/// deterministic constant that won't collide with other apps using
/// pg advisory locks on the same database.
pub const NIX_CI_COORDINATOR_LOCK_KEY: i64 = 0x6e69_7863_6900_0001_u64 as i64;

/// Errors produced when validating a `ServerConfig`. Multiple problems
/// can be present in one config; we report all of them in one go so an
/// operator doesn't have to fix-rerun-fix-rerun.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigErrors {
    pub errors: Vec<String>,
}

impl std::fmt::Display for ConfigErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for e in &self.errors {
            writeln!(f, "  - {e}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigErrors {}

impl ServerConfig {
    /// Parse a JSON config file. Missing fields fall back to
    /// `Default::default()` thanks to `#[serde(default)]` on the
    /// struct. Unknown fields are rejected (`deny_unknown_fields`)
    /// so a typo like `"max_attemps"` doesn't silently inherit
    /// the default — operators see a parse error pointing at the
    /// offending key.
    pub fn load_json(path: &Path) -> Result<Self, ConfigErrors> {
        let bytes = std::fs::read(path).map_err(|e| ConfigErrors {
            errors: vec![format!("read {}: {e}", path.display())],
        })?;
        serde_json::from_slice(&bytes).map_err(|e| ConfigErrors {
            errors: vec![format!("parse {}: {e}", path.display())],
        })
    }

    /// Sanity-check the config. Returns the full list of problems so
    /// a misconfigured deployment surfaces every issue in a single
    /// `--validate` invocation.
    ///
    /// We only validate things that would clearly break the
    /// coordinator (zero values for active counters, ordering
    /// invariants on timeouts). We deliberately don't validate
    /// "reasonable" ranges — operators sometimes need to push
    /// defaults aggressively.
    pub fn validate(&self) -> Result<(), ConfigErrors> {
        let mut errors = Vec::new();

        if self.database_url.trim().is_empty() {
            errors.push("database_url must be non-empty".into());
        }
        if self.claim_deadline_secs == 0 {
            errors.push("claim_deadline_secs must be > 0".into());
        }
        if self.job_heartbeat_timeout_secs == 0 {
            errors.push("job_heartbeat_timeout_secs must be > 0".into());
        }
        if self.reaper_interval_secs == 0 {
            errors.push("reaper_interval_secs must be > 0".into());
        }
        if self.cleanup_interval_secs == 0 {
            errors.push("cleanup_interval_secs must be > 0".into());
        }
        if self.retention_days == 0 {
            errors.push("retention_days must be > 0".into());
        }
        if self.build_log_retention_days == 0 {
            errors.push("build_log_retention_days must be > 0".into());
        }
        if self.submission_event_capacity == 0 {
            errors.push("submission_event_capacity must be > 0".into());
        }
        if self.progress_tick_secs == 0 {
            errors.push("progress_tick_secs must be > 0".into());
        }
        if self.sse_keepalive_secs == 0 {
            errors.push("sse_keepalive_secs must be > 0".into());
        }
        if self.max_claim_wait_secs == 0 {
            errors.push("max_claim_wait_secs must be > 0".into());
        }
        if self.max_attempts < 1 {
            errors.push("max_attempts must be >= 1".into());
        }
        if self.max_request_body_bytes < 1024 {
            errors.push("max_request_body_bytes must be >= 1024 (1 KiB)".into());
        }
        if self.max_drv_path_bytes < 32 {
            errors
                .push("max_drv_path_bytes must be >= 32 (a real Nix store path is longer)".into());
        }
        if self.max_drv_name_bytes < 1 {
            errors.push("max_drv_name_bytes must be >= 1".into());
        }
        if self.max_failures_in_result < 1 {
            errors.push("max_failures_in_result must be >= 1".into());
        }
        if self.graceful_shutdown_secs == 0 {
            errors.push("graceful_shutdown_secs must be > 0".into());
        }
        if self.flaky_retry_backoff_step_ms < 0 {
            errors.push("flaky_retry_backoff_step_ms must be >= 0".into());
        }
        if let Some(cap) = self.max_drvs_per_job {
            if cap == 0 {
                errors
                    .push("max_drvs_per_job, when set, must be > 0 (use null to disable)".into());
            } else if cap < self.submission_warn_threshold {
                errors.push(format!(
                    "max_drvs_per_job ({cap}) must be >= submission_warn_threshold ({})",
                    self.submission_warn_threshold
                ));
            }
        }
        if self.failed_outputs_ttl_secs == 0 {
            errors.push("failed_outputs_ttl_secs must be > 0".into());
        }

        // Ordering invariants: a reaper that fires faster than its
        // own timeout window will reap claims that were just issued.
        if self.reaper_interval_secs >= self.claim_deadline_secs {
            errors.push(format!(
                "reaper_interval_secs ({}) must be less than claim_deadline_secs ({})",
                self.reaper_interval_secs, self.claim_deadline_secs
            ));
        }
        if self.reaper_interval_secs >= self.job_heartbeat_timeout_secs {
            errors.push(format!(
                "reaper_interval_secs ({}) must be less than job_heartbeat_timeout_secs ({})",
                self.reaper_interval_secs, self.job_heartbeat_timeout_secs
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConfigErrors { errors })
        }
    }

    /// Pretty JSON dump of the merged effective config, for
    /// `nix-ci server --print-config`. Operators paste this into bug
    /// reports.
    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|e| {
            // Should be infallible for this struct shape; fall back to
            // Debug if it ever isn't, rather than panicking.
            format!("/* serialization error: {e} */\n{self:#?}")
        })
    }
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub coordinator_url: String,
    pub max_parallel: u32,
    pub system: String,
    pub supported_features: Vec<String>,
    pub eval_workers: u32,
    pub dry_run: bool,
    /// Verbose output: emit per-drv started/completed lines (the
    /// pre-redesign behavior). Default false; default mode shows
    /// periodic progress + immediate failure blocks + summary.
    pub verbose: bool,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            coordinator_url: "http://localhost:8080".to_string(),
            max_parallel: num_cpus(),
            system: default_system().to_string(),
            supported_features: Vec::new(),
            eval_workers: 4,
            dry_run: false,
            verbose: false,
        }
    }
}

fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(4)
}

fn default_system() -> &'static str {
    if cfg!(all(target_arch = "x86_64", target_os = "linux")) {
        "x86_64-linux"
    } else if cfg!(all(target_arch = "aarch64", target_os = "linux")) {
        "aarch64-linux"
    } else if cfg!(all(target_arch = "x86_64", target_os = "macos")) {
        "x86_64-darwin"
    } else if cfg!(all(target_arch = "aarch64", target_os = "macos")) {
        "aarch64-darwin"
    } else {
        "x86_64-linux"
    }
}
