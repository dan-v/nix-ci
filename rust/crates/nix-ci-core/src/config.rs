//! Runtime configuration. Parsed by the CLI, consumed by the server
//! and runner.

use std::net::SocketAddr;

#[derive(Debug, Clone)]
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/nix_ci".to_string(),
            listen: "127.0.0.1:8080".parse().unwrap(),
            lock_key: NIX_CI_COORDINATOR_LOCK_KEY,
            claim_deadline_secs: 2 * 60 * 60, // 2h
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
        }
    }
}

/// `b'n' 'i' 'x' 'c' 'i' 0 0 1` interpreted as a big-endian i64 —
/// deterministic constant that won't collide with other apps using
/// pg advisory locks on the same database.
pub const NIX_CI_COORDINATOR_LOCK_KEY: i64 = 0x6e69_7863_6900_0001_u64 as i64;

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub coordinator_url: String,
    pub max_parallel: u32,
    pub system: String,
    pub supported_features: Vec<String>,
    pub eval_workers: u32,
    pub dry_run: bool,
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
