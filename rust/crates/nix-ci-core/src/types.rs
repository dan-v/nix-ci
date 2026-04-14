//! Wire types shared by server and client. All JSON, all stable-ish.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Cap on log_tail bytes we accept from workers.
pub const MAX_LOG_TAIL_BYTES: usize = 64 * 1024;

// ─── Identity ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

// ─── Cache-status hint from nix-eval-jobs ─────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CacheStatus {
    /// Already available in configured substituter (skip entirely).
    Cached,
    /// Already in the local store (don't rebuild).
    Local,
    /// Needs to be built.
    NotBuilt,
}

// ─── API: create job ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateJobRequest {
    #[serde(default)]
    pub external_ref: Option<String>,
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
    #[serde(default)]
    pub cache_status: Option<CacheStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestDrvResponse {
    pub drv_hash: DrvHash,
    /// True if this call introduced a new step to the registry.
    /// False on duplicate (already ingested by this or another job).
    pub new_drv: bool,
    pub dedup_skipped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestBatchRequest {
    pub drvs: Vec<IngestDrvRequest>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub ok: bool,
}

// ─── API: claim & complete ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimQuery {
    /// Seconds to long-poll. Capped server-side to 60.
    #[serde(default = "ClaimQuery::default_wait")]
    pub wait: u64,
    pub system: String,
    /// Worker-supported features as a comma-separated string. The
    /// server only claims drvs whose `required_features` ⊆ this set.
    #[serde(default)]
    pub features: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimResponse {
    pub claim_id: ClaimId,
    pub drv_hash: DrvHash,
    pub drv_path: String,
    pub attempt: i32,
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
    /// Opportunistic pipelining: skip the next claim round-trip.
    pub next_build: Option<ClaimResponse>,
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
    pub eval_error: Option<String>,
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
    },
    Progress {
        counts: JobCounts,
    },
    JobDone {
        status: JobStatus,
        failures: Vec<DrvFailure>,
    },
    /// A slow SSE consumer fell behind and missed events. Clients
    /// should re-sync by polling `/jobs/{id}` and resubscribing.
    Lagged {
        missed: u64,
    },
}

// ─── Admin snapshot ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminSnapshot {
    pub submissions: u32,
    pub steps_total: u32,
    pub steps_pending: u32,
    pub steps_building: u32,
    pub steps_done: u32,
    pub steps_failed: u32,
    pub active_claims: u32,
}
