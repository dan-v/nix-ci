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
    /// Max length of caller-supplied free-form identifiers:
    /// `external_ref` (CreateJobRequest), `worker_id` (claim query),
    /// and `attr` (IngestDrvRequest). These end up in log output,
    /// DB text columns, and JSONB result snapshots, so an unbounded
    /// string from a hostile or broken client would bloat every row
    /// and every log line it touches.
    pub max_identifier_bytes: usize,
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
    /// Per-connection `statement_timeout` (milliseconds). Set on every
    /// Postgres connection the pool hands out so a runaway query (the
    /// cleanup DELETE on a very large jobs table, a pathological
    /// ingest lookup) can't hold heap locks indefinitely. Default 60s —
    /// generous enough for the cold-cache paths, tight enough to
    /// bound blast radius. Set to 0 to disable (not recommended).
    pub pg_statement_timeout_ms: u64,
    /// Per-handler request timeout (seconds). Bounds the wall-clock a
    /// single non-long-poll HTTP handler can run before the server
    /// gives up on it and returns 503. Prevents a slow DB query or
    /// hung downstream from wedging a tokio task indefinitely and
    /// breaking graceful shutdown. Long-poll routes (`/jobs/{id}/claim`,
    /// `/claim`, `/jobs/{id}/events`) are exempt — they have their
    /// own explicit wait semantics bounded by `max_claim_wait_secs`.
    /// Default 30s; set to 0 to disable (not recommended in
    /// production — a single stuck handler can block shutdown).
    pub request_timeout_secs: u64,
    /// Optional bearer token. When set, every mutating endpoint
    /// (POST / DELETE / claim / complete / events) rejects requests
    /// without a matching `Authorization: Bearer <token>` header with
    /// 401. Monitoring endpoints (`/healthz`, `/readyz`, `/metrics`)
    /// remain unauthenticated so probes keep working. Expected to be
    /// set via env var or JSON config, not a CLI flag (leaks into
    /// process listings). Leave unset for deployments behind a trusted
    /// mesh / VPN.
    pub auth_bearer: Option<String>,
    /// Optional secondary bearer token, distinct from `auth_bearer`,
    /// required on admin-scoped endpoints (`/admin/*`, `/jobs/{id}/fail`,
    /// `/jobs/{id}/cancel`). Only takes effect when `auth_bearer` is
    /// also set. Lets operators issue a narrow "workers only" token
    /// for the fleet while keeping the break-glass admin surface
    /// behind a separate secret. When `None`, admin endpoints accept
    /// the worker `auth_bearer` (current behavior).
    pub admin_bearer: Option<String>,
    /// Overload-shedding threshold on `claims_in_flight`. When the
    /// gauge reaches this value, new claim requests (both per-job and
    /// fleet) return 503 with a `Retry-After: 1` header and the
    /// `overload_rejections` counter is incremented. The contract:
    /// clients see a clean rejection, not a hang or OOM; existing
    /// in-flight claims continue unaffected.
    ///
    /// This is the primary lever in the degradation contract. It
    /// protects the coordinator's memory under worker flood (e.g., a
    /// runaway fleet triple-spawning workers) without affecting
    /// legitimate traffic below the threshold.
    ///
    /// `None` (default) = shedding disabled; claims_in_flight can grow
    /// without coordinator-level limit (bounded only by worker fleet
    /// size). Set to ~2× expected peak in-flight for production.
    pub max_claims_in_flight: Option<u32>,
    /// Per-drv cap on `input_drvs` length. A single nix derivation
    /// with >4k direct inputs has never been observed in real nixpkgs
    /// evaluations — the highest stdenv closure sits in the low
    /// hundreds — so this bound is defensive. Drvs that exceed the
    /// cap are counted in `errored` and skipped; the rest of the
    /// batch proceeds. Paired with `catch_panic_layer` for the
    /// "one bad submission must not break the coordinator" rule.
    pub max_input_drvs_per_drv: u32,
    /// Per-drv cap on `required_features` length. Nix features are a
    /// short enumerated set (big-parallel, kvm, benchmark, …); even
    /// the most feature-demanding drvs use 5-10. Anything above
    /// `max_required_features_per_drv` is either malicious or a
    /// client bug. Over-limit drvs are counted in `errored` and
    /// skipped — the same contract as the other per-drv caps.
    pub max_required_features_per_drv: u32,
    /// Per-batch cap on `eval_errors` length. Bounds the worst-case
    /// ingest-phase memory cost and the final `eval_errors` snapshot
    /// shipped to `jobs.result`. A single broken attr can yield
    /// dozens of lines; 10k is dramatic headroom. Over the cap the
    /// whole batch is rejected with 413 so a runaway evaluator
    /// surfaces instead of silently truncating user-facing errors.
    pub max_eval_errors_per_batch: u32,
    /// Maximum total wall-clock time a single claim can live before
    /// the reaper forcibly re-arms the step, regardless of how often
    /// the worker has extended the lease. Protects against a worker
    /// that's "stuck but still heartbeating" — e.g. a build pinned on
    /// an NFS mount — from holding a drv forever while the fleet
    /// idles. The claim-deadline extension path still works within
    /// this ceiling; expiry on the ceiling is a distinct event from
    /// normal deadline expiry and the step re-arms for another
    /// worker to try fresh. `None` disables the ceiling (claims can
    /// extend indefinitely — the v3 default); set to ~2× the longest
    /// legitimate build to catch pathologies. Measured from
    /// `ActiveClaim.started_at`.
    pub max_claim_lifetime_secs: Option<u64>,
    /// Per-worker error-rate quarantine. When a worker reports more
    /// than `worker_quarantine_failure_threshold` failures inside
    /// `worker_quarantine_window_secs`, the coordinator auto-fences
    /// that worker for `worker_quarantine_cooldown_secs`. Designed
    /// to catch a single-host infra problem (bad `/nix/store`,
    /// corrupted sandbox, flaky network) from poisoning many drvs
    /// before an operator notices.
    ///
    /// **Scope: fleet mode only.** The per-job claim endpoint
    /// (`GET /jobs/{id}/claim`) deliberately skips this check —
    /// the per-job worker IS the CI run, and quarantining it
    /// mid-run would hang the run waiting for a worker that won't
    /// claim. Per-job failures stay bounded inside their own CI
    /// job; fleet failures otherwise spread across unrelated jobs,
    /// so fleet is where containment matters.
    ///
    /// `None` (default) = auto-quarantine disabled; fencing remains
    /// fully manual via `/admin/fence`. Operators opt in by setting
    /// a non-zero threshold.
    pub worker_quarantine_failure_threshold: Option<u32>,
    /// Sliding-window length for auto-quarantine failure counting.
    /// Worker events older than this are evicted from the per-worker
    /// counter. Default 300s — a single flaky minute won't trip
    /// quarantine, but a sustained hour won't be forgotten before
    /// cooldown either.
    pub worker_quarantine_window_secs: u64,
    /// Cooldown after auto-quarantine. The worker is unfenced once
    /// `worker_quarantine_cooldown_secs` have elapsed since the
    /// most recent failure that tripped the threshold. Operators
    /// who want a permanent ban should use `/admin/fence` manually.
    pub worker_quarantine_cooldown_secs: u64,
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
            retention_days: 14,
            submission_event_capacity: 4096,
            progress_tick_secs: 10,
            sse_keepalive_secs: 15,
            max_claim_wait_secs: 60,
            flaky_retry_backoff_step_ms: 30_000,
            max_attempts: 2,
            max_request_body_bytes: 64 * 1024 * 1024, // 64 MiB
            max_drv_path_bytes: 4096,
            max_drv_name_bytes: 1024,
            // 512 bytes: comfortably fits a UUID, a CCI build-URL
            // + PR number + worker hostname, etc. Far above any
            // legitimate value for the free-form identifier fields.
            max_identifier_bytes: 512,
            max_failures_in_result: 500,
            graceful_shutdown_secs: 30,
            build_log_retention_days: 7,
            submission_warn_threshold: 200_000,
            max_drvs_per_job: Some(2_000_000),
            failed_outputs_ttl_secs: 60 * 60, // 1h
            pg_statement_timeout_ms: 60_000,
            request_timeout_secs: 30,
            auth_bearer: None,
            admin_bearer: None,
            max_claims_in_flight: None,
            max_input_drvs_per_drv: 4096,
            max_required_features_per_drv: 32,
            max_eval_errors_per_batch: 10_000,
            max_claim_lifetime_secs: None,
            worker_quarantine_failure_threshold: None,
            worker_quarantine_window_secs: 300,
            worker_quarantine_cooldown_secs: 900,
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
        if self.max_identifier_bytes < 16 {
            errors
                .push("max_identifier_bytes must be >= 16 (a UUID + separator fits in 36)".into());
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
                errors.push("max_drvs_per_job, when set, must be > 0 (use null to disable)".into());
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
        if self.max_input_drvs_per_drv == 0 {
            errors.push("max_input_drvs_per_drv must be > 0".into());
        }
        if self.max_required_features_per_drv == 0 {
            errors.push("max_required_features_per_drv must be > 0".into());
        }
        if self.max_eval_errors_per_batch == 0 {
            errors.push("max_eval_errors_per_batch must be > 0".into());
        }
        if self.worker_quarantine_window_secs == 0 {
            errors.push("worker_quarantine_window_secs must be > 0".into());
        }
        if self.worker_quarantine_cooldown_secs == 0 {
            errors.push("worker_quarantine_cooldown_secs must be > 0".into());
        }
        if let Some(t) = self.worker_quarantine_failure_threshold {
            if t == 0 {
                errors.push(
                    "worker_quarantine_failure_threshold, when set, must be > 0 (use null to disable)"
                        .into(),
                );
            }
        }
        if let Some(m) = self.max_claim_lifetime_secs {
            if m == 0 {
                errors.push(
                    "max_claim_lifetime_secs, when set, must be > 0 (use null to disable)".into(),
                );
            } else if m < self.claim_deadline_secs {
                errors.push(format!(
                    "max_claim_lifetime_secs ({m}) must be >= claim_deadline_secs ({}) — otherwise the ceiling fires before the first lease extension",
                    self.claim_deadline_secs
                ));
            }
        }

        // Log retention must not outlive job retention: the
        // `build_logs_job_fk` cascade deletes logs whenever a job is
        // pruned. If `build_log_retention_days > retention_days`, the
        // cascade would fire before the log cleanup loop's cutoff,
        // defeating the separate retention knob. Keeping them ordered
        // here surfaces the misconfiguration at boot rather than as
        // unexplained missing logs in production.
        if self.build_log_retention_days > self.retention_days {
            errors.push(format!(
                "build_log_retention_days ({}) must be <= retention_days ({}) — job deletion cascades to build_logs",
                self.build_log_retention_days, self.retention_days
            ));
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
    /// reports. Bearer tokens are redacted so the dump is safe to
    /// paste into a ticket.
    pub fn to_json_pretty(&self) -> String {
        let mut redacted = self.clone();
        if redacted.auth_bearer.is_some() {
            redacted.auth_bearer = Some("<redacted>".to_string());
        }
        if redacted.admin_bearer.is_some() {
            redacted.admin_bearer = Some("<redacted>".to_string());
        }
        serde_json::to_string_pretty(&redacted).unwrap_or_else(|e| {
            // Should be infallible for this struct shape; fall back to
            // Debug if it ever isn't, rather than panicking.
            format!("/* serialization error: {e} */\n{redacted:#?}")
        })
    }

    /// Resolve a bearer token: prefer the `*_FILE` env var (points at
    /// a credential file written by systemd's `LoadCredential` — the
    /// token stays off `/proc/*/environ` and out of the config JSON),
    /// then fall back to the inline field. Trims trailing whitespace
    /// so operators can `echo token > file` without breaking the
    /// comparison. Empty contents become `None`.
    fn load_bearer_from_file_env(env_key: &str) -> Option<String> {
        let path = std::env::var(env_key)
            .ok()
            .filter(|s| !s.trim().is_empty())?;
        match std::fs::read_to_string(&path) {
            Ok(s) => {
                let trimmed = s.trim_end_matches(['\n', '\r', ' ', '\t']).to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            }
            Err(e) => {
                tracing::error!(
                    env = env_key,
                    path = %path,
                    error = %e,
                    "bearer token file unreadable; falling back to inline config"
                );
                None
            }
        }
    }

    /// Apply `*_FILE` env overrides to the bearer fields. Called by
    /// `nix-ci server` after config load + CLI / env overlay so that
    /// systemd `LoadCredential` paths (set as `NIX_CI_AUTH_BEARER_FILE`
    /// / `NIX_CI_ADMIN_BEARER_FILE`) take precedence over whatever
    /// the JSON file or inline env var said.
    pub fn apply_bearer_files(&mut self) {
        if let Some(tok) = Self::load_bearer_from_file_env("NIX_CI_AUTH_BEARER_FILE") {
            self.auth_bearer = Some(tok);
        }
        if let Some(tok) = Self::load_bearer_from_file_env("NIX_CI_ADMIN_BEARER_FILE") {
            self.admin_bearer = Some(tok);
        }
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
