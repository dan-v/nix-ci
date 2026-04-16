//! Build log archive endpoints.
//!
//! * `POST /jobs/{job_id}/claims/{claim_id}/log` — worker uploads
//!   gzipped log bytes (octet-stream body) plus metadata as query
//!   params. Best-effort archive: the build outcome was already
//!   recorded by `/complete`, so a failed upload here is logged but
//!   doesn't change job state. Idempotent on `claim_id` (upsert).
//!
//! * `GET /jobs/{job_id}/claims/{claim_id}/log` — return the stored
//!   log decompressed as `text/plain; charset=utf-8`.
//!
//! * `GET /jobs/{job_id}/drvs/{drv_hash}/logs` — list every stored
//!   attempt's metadata for a drv on a job. Response is JSON
//!   ([`BuildLogsResponse`]).

use std::sync::atomic::Ordering;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::AppState;
use crate::durable::logs::{fetch_decompressed, LogPutRequest};
use crate::error::{Error, Result};
use crate::types::{BuildLogsResponse, ClaimId, DrvHash, JobId, MAX_BUILD_LOG_RAW_BYTES};

/// Upper bound on the gzipped upload body. We assume worst-case ~2:1
/// compression on adversarial input (random or already-compressed
/// data); the cap is `2 * MAX_BUILD_LOG_RAW_BYTES` so a legitimate
/// well-compressed log fits comfortably and a hostile one is rejected
/// before reading the whole body.
const MAX_UPLOAD_BYTES: usize = 2 * MAX_BUILD_LOG_RAW_BYTES;

#[derive(Debug, Deserialize)]
pub struct UploadLogQuery {
    pub drv_hash: String,
    pub attempt: i32,
    /// Original (pre-gzip) size, so we can report it back without
    /// decompressing.
    pub original_size: u32,
    #[serde(default)]
    pub truncated: bool,
    pub success: bool,
    #[serde(default)]
    pub exit_code: Option<i32>,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}

pub async fn upload_log(
    State(state): State<AppState>,
    Path((job_id, claim_id)): Path<(JobId, ClaimId)>,
    Query(q): Query<UploadLogQuery>,
    body: Bytes,
) -> Result<StatusCode> {
    if body.len() > MAX_UPLOAD_BYTES {
        return Err(Error::BadRequest(format!(
            "log upload {} exceeds cap {}",
            body.len(),
            MAX_UPLOAD_BYTES
        )));
    }
    if q.original_size as usize > MAX_BUILD_LOG_RAW_BYTES && !q.truncated {
        return Err(Error::BadRequest(format!(
            "original_size {} > cap {} but truncated=false",
            q.original_size, MAX_BUILD_LOG_RAW_BYTES
        )));
    }

    let req = LogPutRequest {
        job_id,
        claim_id,
        drv_hash: DrvHash::new(q.drv_hash),
        attempt: q.attempt,
        success: q.success,
        exit_code: q.exit_code,
        started_at: q.started_at,
        ended_at: q.ended_at,
        original_size: q.original_size,
        truncated: q.truncated,
    };
    state.log_store.put(req, body.to_vec()).await?;
    if q.truncated {
        state.metrics.inner.build_logs_truncated_total.inc();
    }
    // Defensive use of the atomic in case future code paths grow.
    let _ = Ordering::Relaxed;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn fetch_log(
    State(state): State<AppState>,
    Path((job_id, claim_id)): Path<(JobId, ClaimId)>,
) -> Result<Response> {
    let Some(text) = fetch_decompressed(state.log_store.as_ref(), job_id, claim_id).await? else {
        return Err(Error::NotFound(format!(
            "log claim={claim_id} job={job_id}"
        )));
    };
    let mut resp = text.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/plain; charset=utf-8".parse().unwrap(),
    );
    Ok(resp)
}

pub async fn list_drv_logs(
    State(state): State<AppState>,
    Path((job_id, drv_hash)): Path<(JobId, String)>,
) -> Result<Json<BuildLogsResponse>> {
    let drv_hash = DrvHash::new(drv_hash);
    let rows = state.log_store.list_attempts(job_id, &drv_hash).await?;
    Ok(Json(BuildLogsResponse {
        job_id,
        drv_hash,
        attempts: rows.into_iter().map(|r| r.into_attempt()).collect(),
    }))
}
