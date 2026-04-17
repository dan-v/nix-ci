//! Wire types shared by server and client. All JSON, all stable-ish.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Cap on log_tail bytes we accept from workers.
pub const MAX_LOG_TAIL_BYTES: usize = 64 * 1024;

// ─── Identity ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub Uuid);

impl JobId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClaimId(pub Uuid);

impl ClaimId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ClaimId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ClaimId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A content-addressed derivation hash: the basename of its `.drv`,
/// e.g. `abc123…-hello-2.12.1.drv`. We key everything on this string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DrvHash(pub String);

impl DrvHash {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DrvHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Parse a `drv_hash` from a store path. Returns the basename and
/// validates that it's plausibly a derivation file (non-empty, ends
/// in `.drv`, contains a `-` separating the hash from the name).
/// Example: `/nix/store/abc…-hello-2.12.1.drv` → `abc…-hello-2.12.1.drv`
pub fn drv_hash_from_path(p: &str) -> Option<DrvHash> {
    let base = p.rsplit('/').next()?;
    if base.is_empty() || !base.ends_with(".drv") || !base.contains('-') {
        return None;
    }
    Some(DrvHash::new(base.to_string()))
}

// ─── Status ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Building,
    Done,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Building => "building",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DrvState {
    Pending,
    Building,
    Done,
    Failed,
}

impl DrvState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Building => "building",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }
}

// ─── Error categorization (worker → server) ───────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// Build itself failed deterministically; don't retry.
    BuildFailure,
    /// Transient: network, substituter, daemon blip. Retry.
    Transient,
    /// Disk full, OOM killed, resource exhaustion. Retry.
    DiskFull,
    /// Dep already failed. Don't retry — propagated.
    PropagatedFailure,
}

impl ErrorCategory {
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Transient | Self::DiskFull)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::BuildFailure => "build_failure",
            Self::Transient => "transient",
            Self::DiskFull => "disk_full",
            Self::PropagatedFailure => "propagated_failure",
        }
    }
}

// ─── API: create job ──────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateJobRequest {
    #[serde(default)]
    pub external_ref: Option<String>,
    /// Higher-priority jobs are scanned first by the fleet `/claim`
    /// endpoint. Within a priority tier, older jobs go first (FIFO).
    /// Default 0. Negative values deprioritize below ordinary jobs
    /// (e.g., batch rebuilds); positive values preempt ordinary ones
    /// (e.g., urgent hotfix). Not a strict preemption mechanism — an
    /// already-dispatched worker finishes its current drv before the
    /// next claim decision.
    #[serde(default)]
    pub priority: i32,
    /// Per-job max concurrent workers. When set, the fleet scheduler
    /// stops dispatching new claims for this job once this many
    /// claims are in flight, letting the remaining workers drain to
    /// other jobs. `None` = no cap. Defaults to no cap.
    #[serde(default)]
    pub max_workers: Option<u32>,
    /// Per-job override of `ServerConfig::claim_deadline_secs`. A stuck
    /// worker holds a drv for at most this duration before the reaper
    /// re-arms it for a different worker. Lower values give faster tail
    /// recovery (most nixpkgs drvs build in < 10 min, so a 1h job-level
    /// deadline is dramatically tighter than the 2h server default
    /// while still covering outliers like webkitgtk). `None` = inherit
    /// server default.
    #[serde(default)]
    pub claim_deadline_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateJobResponse {
    pub id: JobId,
}

// ─── API: ingest drv (streaming, one per request) ─────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestDrvRequest {
    pub drv_path: String,
    pub drv_name: String,
    pub system: String,
    #[serde(default)]
    pub required_features: Vec<String>,
    /// Direct input derivations by drv_path (from nix-eval-jobs
    /// `inputDrvs`). The server walks transitively via repeated
    /// submissions — it does NOT call `nix derivation show`.
    #[serde(default)]
    pub input_drvs: Vec<String>,
    /// True for top-level attrs emitted by nix-eval-jobs.
    #[serde(default)]
    pub is_root: bool,
    /// Optional human-readable attr name from nix-eval-jobs (e.g.
    /// `packages.x86_64-linux.hello`). Only meaningful when
    /// `is_root=true` — used for failure attribution
    /// ("FAILED: hello-2.12.1, used by: packages.x86_64-linux.hello").
    /// Older clients omit it; coordinator falls back to `drv_name`.
    #[serde(default)]
    pub attr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestDrvResponse {
    pub drv_hash: DrvHash,
    /// True if this call introduced a new step to the registry.
    /// False on duplicate (already ingested by this or another job).
    pub new_drv: bool,
    pub dedup_skipped: bool,
}

/// One broken attribute reported by the runner. nix-eval-jobs emits
/// `{"attr": "x", "error": "..."}` for attrs that fail to evaluate;
/// the submitter forwards these to the coordinator so they surface in
/// the terminal `JobStatusResponse.eval_errors` rather than being
/// visible only in the runner's stderr. Distinct from the singular
/// `eval_error: Option<String>` field which carries coordinator-
/// originated causes (cycle detection, per-job drv cap exceeded).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvalError {
    pub attr: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestBatchRequest {
    pub drvs: Vec<IngestDrvRequest>,
    /// Per-attribute eval errors from nix-eval-jobs. Optional;
    /// submitters that don't forward them (or older clients) leave
    /// this empty. Recorded on the submission; surfaced in the
    /// terminal `eval_errors` field.
    #[serde(default)]
    pub eval_errors: Vec<EvalError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestBatchResponse {
    pub new_drvs: u32,
    pub dedup_skipped: u32,
    pub errored: u32,
}

// ─── API: seal / fail / heartbeat ─────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SealJobResponse {
    pub status: JobStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailJobRequest {
    pub message: String,
}

// ─── API: claim & complete ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimQuery {
    /// Seconds to long-poll. Capped server-side to 60.
    #[serde(default = "ClaimQuery::default_wait")]
    pub wait: u64,
    /// Comma-separated list of systems this worker can build for
    /// (e.g. `x86_64-linux` or `x86_64-linux,aarch64-linux`). A host
    /// running nix with cross-compilation toolchains can advertise
    /// every target it genuinely supports; the coordinator walks the
    /// list in order and offers the first matching runnable drv.
    ///
    /// Single-system callers pass a bare `x86_64-linux` — backwards-
    /// compatible, no escaping required.
    pub system: String,
    /// Worker-supported features as a comma-separated string. The
    /// server only claims drvs whose `required_features` ⊆ this set.
    #[serde(default)]
    pub features: String,
    /// Optional worker identifier (e.g. `host42-pid12345-a3b91c`).
    /// Stored on the resulting `ActiveClaim` and surfaced via
    /// `GET /claims` so an operator can map "drv X claimed for 90 min"
    /// to a specific worker host. Free-form; cardinality is the
    /// operator's problem (the coordinator does not put it on a
    /// Prometheus label).
    #[serde(default)]
    pub worker: Option<String>,
}

impl ClaimQuery {
    fn default_wait() -> u64 {
        30
    }

    pub fn features_vec(&self) -> Vec<String> {
        self.features
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Parse `system` into a list of system strings, preserving order
    /// (used as a claim-priority hint by `pop_runnable`). Empty
    /// entries and whitespace are stripped. A single-system call
    /// (the 99% case) produces a one-element vec.
    pub fn systems_vec(&self) -> Vec<String> {
        self.system
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimResponse {
    /// Job that owns this claim. Fleet workers (no job binding at
    /// claim time) need this to address the subsequent `/complete`
    /// and `/heartbeat` endpoints. Per-job workers receive the same
    /// value they passed in — populated unconditionally for symmetry.
    pub job_id: JobId,
    pub claim_id: ClaimId,
    pub drv_hash: DrvHash,
    pub drv_path: String,
    pub attempt: i32,
    pub deadline: DateTime<Utc>,
}

/// Response shape for `POST /jobs/{id}/claims/{claim_id}/extend`.
/// Carries the new wall-clock deadline so the worker can log it and/or
/// adjust its next-refresh timer — though the simplest correct policy
/// is to refresh at a fixed cadence tied to the original deadline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendClaimResponse {
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub success: bool,
    pub duration_ms: u64,
    pub exit_code: Option<i32>,
    pub error_category: Option<ErrorCategory>,
    pub error_message: Option<String>,
    pub log_tail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteResponse {
    /// True when the claim_id was stale (coordinator restarted, or this
    /// claim was reclaimed). The worker should drop its result.
    pub ignored: bool,
}

// ─── API: status + events ─────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobCounts {
    pub total: u32,
    pub pending: u32,
    pub building: u32,
    pub done: u32,
    pub failed: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrvFailure {
    pub drv_hash: DrvHash,
    pub drv_name: String,
    pub error_category: ErrorCategory,
    pub error_message: Option<String>,
    pub log_tail: Option<String>,
    pub propagated_from: Option<DrvHash>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusResponse {
    pub id: JobId,
    pub status: JobStatus,
    pub sealed: bool,
    pub counts: JobCounts,
    pub failures: Vec<DrvFailure>,
    /// Coordinator-originated evaluation failure (e.g. `eval_too_large`
    /// ingest cap violation, cycle-at-seal detection). Mutually
    /// exclusive with successful eval: at most one is set.
    pub eval_error: Option<String>,
    /// Per-attribute eval errors reported by the runner's submitter.
    /// Bounded by `EVAL_ERRORS_CAP` on the coordinator with a synthetic
    /// `<truncated>` marker appended if exceeded. Default-empty for
    /// backward compatibility with older clients / test fixtures.
    #[serde(default)]
    pub eval_errors: Vec<EvalError>,
}

/// One in-flight build, included in `Progress` events so clients can
/// render "currently building: gcc-13.2.0 (5m 12s)".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrvInFlight {
    pub drv_name: String,
    /// When the claim was issued. Wall-clock unix milliseconds.
    pub started_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobEvent {
    DrvStarted {
        drv_hash: DrvHash,
        drv_name: String,
        claim_id: ClaimId,
        attempt: i32,
    },
    DrvCompleted {
        drv_hash: DrvHash,
        drv_name: String,
        duration_ms: u64,
    },
    DrvFailed {
        drv_hash: DrvHash,
        drv_name: String,
        error_category: ErrorCategory,
        error_message: Option<String>,
        log_tail: Option<String>,
        attempt: i32,
        will_retry: bool,
        /// Top-level attr names this drv contributes to (transitively).
        /// Lets the runner say "used by: packages.x86_64-linux.hello"
        /// rather than just the drv_name. Empty for clients that
        /// didn't supply `IngestDrvRequest.attr`.
        #[serde(default)]
        used_by_attrs: Vec<String>,
    },
    Progress {
        counts: JobCounts,
        /// Snapshot of currently-building drvs (claimed but not yet
        /// completed) for THIS job. Useful for "X in flight" displays.
        #[serde(default)]
        in_flight: Vec<DrvInFlight>,
        /// Cumulative count of drvs marked propagated_failure for
        /// this job. Differentiates "X originating + Y propagated"
        /// in failure summaries.
        #[serde(default)]
        propagated_failed: u32,
        /// Cumulative count of transient (retryable) failures
        /// observed on this job's drvs. Surfaces flakiness.
        #[serde(default)]
        transient_retries: u32,
        /// Whether the submission has been sealed yet. Lets the
        /// runner switch its progress display from "ingested X /
        /// eval running" to "X / total" once eval+submit is done.
        #[serde(default)]
        sealed: bool,
    },
    JobDone {
        status: JobStatus,
        failures: Vec<DrvFailure>,
    },
    /// A slow SSE consumer fell behind and missed events. Clients
    /// should re-sync by polling `/jobs/{id}` and resubscribing.
    Lagged { missed: u64 },
}

// ─── Admin snapshot ───────────────────────────────────────────────────

/// One-line summary of a job. Used by `/admin/snapshot.recent_failures`,
/// `GET /jobs?status=...`, and `nix-ci list`.
///
/// `originating_failures` lists the drv_names that actually failed
/// (BuildFailure category) — capped at 3 entries with `+N originating`
/// folded into a count if exceeded. `propagated_failures` is the count
/// of downstream drvs that failed only because their dep failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSummary {
    pub id: JobId,
    #[serde(default)]
    pub external_ref: Option<String>,
    pub status: JobStatus,
    pub done_at: Option<DateTime<Utc>>,
    /// Up to 3 originating failure drv_names; `+N more` is implied
    /// by `originating_failures_total`.
    #[serde(default)]
    pub originating_failures: Vec<String>,
    #[serde(default)]
    pub originating_failures_total: u32,
    #[serde(default)]
    pub propagated_failures: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminSnapshot {
    pub submissions: u32,
    pub steps_total: u32,
    pub steps_pending: u32,
    pub steps_building: u32,
    pub steps_done: u32,
    pub steps_failed: u32,
    pub active_claims: u32,
    /// Up to 5 most recent failed jobs (DB-backed query). Zero-cost
    /// observability: ops sees recent failures without a separate call.
    #[serde(default)]
    pub recent_failures: Vec<JobSummary>,
}

/// Response shape for `GET /jobs?status=...&since=...&cursor=...&limit=N`.
/// Cursor pagination on `done_at DESC`: pass `next_cursor` from the
/// previous response as `cursor` to fetch the next page.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsListResponse {
    pub jobs: Vec<JobSummary>,
    /// If present, more rows exist after the last `done_at` returned.
    /// Pass back as `?cursor=<value>`.
    #[serde(default)]
    pub next_cursor: Option<DateTime<Utc>>,
}

// ─── API: build log archive ───────────────────────────────────────────

/// Worker-side cap on captured stderr per attempt before gzip. ~10-20×
/// compression on typical build output puts the wire payload in the
/// 200-400 KiB range. The 64 KiB tail used for inline display is
/// independent (`MAX_LOG_TAIL_BYTES`).
pub const MAX_BUILD_LOG_RAW_BYTES: usize = 4 * 1024 * 1024;

/// One stored attempt's metadata (no bytes). Returned by
/// `GET /jobs/{id}/drvs/{drv_hash}/logs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildLogAttempt {
    pub claim_id: ClaimId,
    pub attempt: i32,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    /// Raw size before gzip (post-truncation if `truncated`).
    pub original_size: u32,
    /// On-disk gzipped size.
    pub stored_size: u32,
    /// True if the worker had to truncate the head of the log to fit
    /// `MAX_BUILD_LOG_RAW_BYTES`.
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildLogsResponse {
    pub job_id: JobId,
    pub drv_hash: DrvHash,
    /// Newest attempt first.
    pub attempts: Vec<BuildLogAttempt>,
}

// ─── API: in-flight claim listing (worker observability) ──────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveClaimSummary {
    pub claim_id: ClaimId,
    pub job_id: JobId,
    pub drv_hash: DrvHash,
    pub attempt: i32,
    /// Free-form worker identifier supplied at claim time
    /// (e.g. `host42-pid12345-a3b91c`). Empty when the worker is older
    /// or doesn't set the `worker` query param.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// Wall-clock when the claim was issued.
    pub claimed_at: DateTime<Utc>,
    /// Milliseconds since the claim was issued, snapshot at request
    /// time. Sorted newest→oldest in the listing response.
    pub elapsed_ms: u64,
    /// Wall-clock deadline after which the reaper will recycle the
    /// claim back into the runnable queue.
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimsListResponse {
    pub claims: Vec<ActiveClaimSummary>,
}
