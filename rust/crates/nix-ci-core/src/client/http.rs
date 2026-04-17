//! Thin reqwest wrapper for the coordinator HTTP API. The worker and
//! submitter both use this.

use std::time::Duration;

use opentelemetry::propagation::Injector;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::error::{Error, Result};
use crate::types::{
    BuildLogsResponse, ClaimId, ClaimResponse, ClaimsListResponse, CompleteRequest,
    CompleteResponse, CreateJobRequest, CreateJobResponse, DrvHash, ExtendClaimResponse,
    IngestBatchRequest, IngestBatchResponse, IngestDrvRequest, IngestDrvResponse, JobId,
    JobStatusResponse, JobsListResponse, SealJobResponse,
};

#[derive(Clone)]
pub struct CoordinatorClient {
    base: String,
    http: Client,
    /// Optional bearer token. When set, prepended to every request's
    /// `Authorization: Bearer <token>` header. Matches
    /// `ServerConfig::auth_bearer` on the coordinator side.
    auth_bearer: Option<String>,
}

/// Metadata for a build-log upload. Borrowed from caller-owned data so
/// the worker doesn't have to clone the drv_hash on the hot path.
pub struct BuildLogUploadMeta<'a> {
    pub drv_hash: &'a DrvHash,
    pub attempt: i32,
    pub original_size: u32,
    pub truncated: bool,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub ended_at: chrono::DateTime<chrono::Utc>,
}

impl CoordinatorClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_auth(base_url, None)
    }

    /// Construct a client that sends a bearer token with every request.
    /// Match against `ServerConfig::auth_bearer` on the coordinator.
    pub fn with_auth(base_url: impl Into<String>, auth_bearer: Option<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(75))
            .user_agent(concat!("nix-ci/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client build");
        Self {
            base: normalize(base_url.into()),
            http,
            auth_bearer,
        }
    }

    /// Wrap `Client::get` to inject the W3C `traceparent` header off
    /// the current tracing span. When OTLP is disabled (no exporter
    /// configured) the global propagator is a no-op and this adds no
    /// headers — pure cost: one stack-allocated `HeaderMap`.
    fn get(&self, url: &str) -> reqwest::RequestBuilder {
        self.auth(inject_trace(self.http.get(url)))
    }

    fn post(&self, url: &str) -> reqwest::RequestBuilder {
        self.auth(inject_trace(self.http.post(url)))
    }

    fn delete(&self, url: &str) -> reqwest::RequestBuilder {
        self.auth(inject_trace(self.http.delete(url)))
    }

    /// Attach `Authorization: Bearer <token>` if configured.
    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth_bearer {
            Some(tok) => rb.bearer_auth(tok),
            None => rb,
        }
    }

    pub async fn create_job(&self, req: &CreateJobRequest) -> Result<CreateJobResponse> {
        let url = format!("{}/jobs", self.base);
        let resp = self.post(&url).json(req).send().await?;
        decode(resp).await
    }

    /// Ingest a single drv. Convenience wrapper around `ingest_batch`;
    /// the server only implements the batch endpoint.
    pub async fn ingest_drv(
        &self,
        job_id: JobId,
        req: &IngestDrvRequest,
    ) -> Result<IngestDrvResponse> {
        let drv_hash = crate::types::drv_hash_from_path(&req.drv_path)
            .ok_or_else(|| Error::BadRequest(format!("bad drv_path: {}", req.drv_path)))?;
        let batch = IngestBatchRequest {
            drvs: vec![req.clone()],
            eval_errors: Vec::new(),
        };
        let resp = self.ingest_batch(job_id, &batch).await?;
        Ok(IngestDrvResponse {
            drv_hash,
            new_drv: resp.new_drvs > 0,
            dedup_skipped: resp.dedup_skipped > 0,
        })
    }

    pub async fn ingest_batch(
        &self,
        job_id: JobId,
        req: &IngestBatchRequest,
    ) -> Result<IngestBatchResponse> {
        let url = format!("{}/jobs/{}/drvs/batch", self.base, job_id);
        let resp = self.post(&url).json(req).send().await?;
        decode(resp).await
    }

    pub async fn seal(&self, job_id: JobId) -> Result<SealJobResponse> {
        let url = format!("{}/jobs/{}/seal", self.base, job_id);
        let resp = self.post(&url).send().await?;
        decode(resp).await
    }

    /// Mark a job failed with a caller-supplied reason. Used by the
    /// runner when it cannot honor the submission at all (e.g. the
    /// closure walk couldn't read a .drv file) — we'd rather the
    /// coordinator land a real terminal `Failed` row with a cause than
    /// leave the submission to the heartbeat reaper (which shows up as
    /// an opaque timeout).
    pub async fn fail(&self, job_id: JobId, message: &str) -> Result<SealJobResponse> {
        let url = format!("{}/jobs/{}/fail", self.base, job_id);
        let resp = self
            .post(&url)
            .json(&crate::types::FailJobRequest {
                message: message.to_string(),
            })
            .send()
            .await?;
        decode(resp).await
    }

    /// Cancel a job. Idempotent: cancelling an already-terminal job
    /// succeeds silently. Used by tests and by external orchestrators
    /// (e.g. a CI system dropping a cancelled PR's CI job).
    pub async fn cancel(&self, job_id: JobId) -> Result<SealJobResponse> {
        let url = format!("{}/jobs/{}/cancel", self.base, job_id);
        let resp = self.delete(&url).send().await?;
        decode(resp).await
    }

    pub async fn heartbeat(&self, job_id: JobId) -> Result<()> {
        let url = format!("{}/jobs/{}/heartbeat", self.base, job_id);
        let resp = self.post(&url).send().await?;
        match resp.status() {
            s if s.is_success() => Ok(()),
            StatusCode::GONE => Err(Error::Gone("job is terminal".into())),
            StatusCode::UNAUTHORIZED => {
                Err(Error::Unauthorized(resp.text().await.unwrap_or_default()))
            }
            s => {
                let body = resp.text().await.unwrap_or_default();
                Err(Error::Internal(format!("heartbeat {s}: {body}")))
            }
        }
    }

    /// Per-job claim. Convenience wrapper over [`Self::claim_as_worker`]
    /// without a `worker_id`. Used by tests + the legacy code paths.
    ///
    /// `system` accepts either a single system (`x86_64-linux`) or a
    /// comma-separated preference list (`x86_64-linux,aarch64-linux`).
    /// The coordinator walks the list in order and offers the first
    /// runnable drv from any of them.
    pub async fn claim(
        &self,
        job_id: JobId,
        system: &str,
        features: &[String],
        wait_secs: u64,
    ) -> Result<Option<ClaimResponse>> {
        self.claim_as_worker(job_id, system, features, wait_secs, None)
            .await
    }

    /// Per-job claim with explicit worker identity. The id is recorded
    /// on the `ActiveClaim` so `GET /claims` can attribute the work.
    /// See [`Self::claim`] for the `system` format.
    pub async fn claim_as_worker(
        &self,
        job_id: JobId,
        system: &str,
        features: &[String],
        wait_secs: u64,
        worker_id: Option<&str>,
    ) -> Result<Option<ClaimResponse>> {
        let url = format!("{}/jobs/{}/claim", self.base, job_id);
        self.send_claim(&url, system, features, wait_secs, worker_id)
            .await
    }

    /// Fleet claim: ask the coordinator for ANY runnable drv across
    /// every live submission. The response carries the owning
    /// `job_id` so subsequent `complete`/`heartbeat` calls can target
    /// the right job. Returns `Ok(None)` on the 204 deadline path.
    pub async fn claim_any(
        &self,
        system: &str,
        features: &[String],
        wait_secs: u64,
    ) -> Result<Option<ClaimResponse>> {
        self.claim_any_as_worker(system, features, wait_secs, None)
            .await
    }

    pub async fn claim_any_as_worker(
        &self,
        system: &str,
        features: &[String],
        wait_secs: u64,
        worker_id: Option<&str>,
    ) -> Result<Option<ClaimResponse>> {
        let url = format!("{}/claim", self.base);
        self.send_claim(&url, system, features, wait_secs, worker_id)
            .await
    }

    /// Shared GET implementation for both per-job and fleet claim
    /// endpoints. Wire format is identical; only the URL differs.
    async fn send_claim(
        &self,
        url: &str,
        system: &str,
        features: &[String],
        wait_secs: u64,
        worker_id: Option<&str>,
    ) -> Result<Option<ClaimResponse>> {
        let feat_csv = features.join(",");
        let mut req = self.get(url).query(&[
            ("wait", wait_secs.to_string().as_str()),
            ("system", system),
            ("features", feat_csv.as_str()),
        ]);
        if let Some(w) = worker_id {
            req = req.query(&[("worker", w)]);
        }
        // Client timeout is the long-poll wait plus a generous 15s for
        // TLS + roundtrip. Without the extra, a client whose clock is
        // a few seconds ahead of the server can trip reqwest's own
        // timeout before the server-side 204 deadline fires.
        let resp = req
            .timeout(Duration::from_secs(wait_secs + 15))
            .send()
            .await?;
        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        let response: ClaimResponse = decode(resp).await?;
        Ok(Some(response))
    }

    /// Operator: list every active claim across every job, sorted
    /// longest-running first by the server.
    pub async fn list_claims(&self) -> Result<ClaimsListResponse> {
        let url = format!("{}/claims", self.base);
        let resp = self.get(&url).send().await?;
        decode(resp).await
    }

    /// Upload a per-attempt build log. Best-effort archive — caller
    /// should not block its main flow on failure here.
    /// `gz` is the gzip-compressed log bytes; the worker is expected
    /// to do the compression to keep wire bytes small.
    #[allow(clippy::too_many_arguments)]
    pub async fn upload_log(
        &self,
        job_id: JobId,
        claim_id: ClaimId,
        meta: BuildLogUploadMeta<'_>,
        gz: Vec<u8>,
    ) -> Result<()> {
        let url = format!("{}/jobs/{}/claims/{}/log", self.base, job_id, claim_id);
        let mut q: Vec<(&str, String)> = vec![
            ("drv_hash", meta.drv_hash.as_str().to_string()),
            ("attempt", meta.attempt.to_string()),
            ("original_size", meta.original_size.to_string()),
            ("truncated", meta.truncated.to_string()),
            ("success", meta.success.to_string()),
            ("started_at", meta.started_at.to_rfc3339()),
            ("ended_at", meta.ended_at.to_rfc3339()),
        ];
        if let Some(ec) = meta.exit_code {
            q.push(("exit_code", ec.to_string()));
        }
        let resp = self
            .http
            .post(&url)
            .query(&q)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(gz)
            .send()
            .await?;
        if resp.status().is_success() {
            return Ok(());
        }
        let s = resp.status();
        let text = resp.text().await.unwrap_or_default();
        Err(Error::Internal(format!("upload_log {s}: {text}")))
    }

    /// Fetch one attempt's log decompressed as text.
    pub async fn fetch_log(&self, job_id: JobId, claim_id: ClaimId) -> Result<String> {
        let url = format!("{}/jobs/{}/claims/{}/log", self.base, job_id, claim_id);
        let resp = self.get(&url).send().await?;
        let s = resp.status();
        if s.is_success() {
            Ok(resp.text().await?)
        } else if s == StatusCode::NOT_FOUND {
            Err(Error::NotFound(format!("log claim={claim_id}")))
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(Error::Internal(format!("fetch_log {s}: {text}")))
        }
    }

    /// List every stored attempt for a (job, drv).
    pub async fn list_drv_logs(
        &self,
        job_id: JobId,
        drv_hash: &DrvHash,
    ) -> Result<BuildLogsResponse> {
        let url = format!(
            "{}/jobs/{}/drvs/{}/logs",
            self.base,
            job_id,
            drv_hash.as_str()
        );
        let resp = self.get(&url).send().await?;
        decode(resp).await
    }

    /// Extend a claim's lease. Returns `Ok(Some(..))` on success,
    /// `Ok(None)` when the coordinator reports the claim is already
    /// gone (terminal, evicted, or reaped) — the worker should stop
    /// refreshing. Other errors propagate unchanged so transient
    /// failures bubble up to the caller's retry logic.
    pub async fn extend_claim(
        &self,
        job_id: JobId,
        claim_id: ClaimId,
    ) -> Result<Option<ExtendClaimResponse>> {
        let url = format!("{}/jobs/{}/claims/{}/extend", self.base, job_id, claim_id);
        let resp = self.post(&url).send().await?;
        match resp.status() {
            s if s.is_success() => Ok(Some(decode(resp).await?)),
            StatusCode::GONE => Ok(None),
            StatusCode::UNAUTHORIZED => {
                Err(Error::Unauthorized(resp.text().await.unwrap_or_default()))
            }
            s => {
                let body = resp.text().await.unwrap_or_default();
                Err(Error::Internal(format!("extend_claim {s}: {body}")))
            }
        }
    }

    pub async fn complete(
        &self,
        job_id: JobId,
        claim_id: ClaimId,
        req: &CompleteRequest,
    ) -> Result<CompleteResponse> {
        let url = format!("{}/jobs/{}/claims/{}/complete", self.base, job_id, claim_id);
        let resp = self.post(&url).json(req).send().await?;
        decode(resp).await
    }

    pub async fn status(&self, job_id: JobId) -> Result<JobStatusResponse> {
        let url = format!("{}/jobs/{}", self.base, job_id);
        let resp = self.get(&url).send().await?;
        decode(resp).await
    }

    pub fn events_url(&self, job_id: JobId) -> String {
        format!("{}/jobs/{}/events", self.base, job_id)
    }

    /// Resolve "id-or-external-ref" → terminal status snapshot. Tries
    /// the UUID path first; if the input doesn't parse as a UUID,
    /// falls back to the by-external-ref endpoint. Used by
    /// `nix-ci show <arg>`.
    pub async fn show_job(&self, id_or_ref: &str) -> Result<JobStatusResponse> {
        if let Ok(uuid) = uuid::Uuid::parse_str(id_or_ref) {
            return self.status(JobId(uuid)).await;
        }
        let url = format!("{}/jobs/by-external-ref/{}", self.base, id_or_ref);
        let resp = self.get(&url).send().await?;
        decode(resp).await
    }

    /// List jobs filtered by status, ordered newest-first. Cursor-
    /// paginate by passing back `JobsListResponse.next_cursor` as
    /// `cursor`. Used by `nix-ci list --failed`.
    pub async fn list_jobs(
        &self,
        status: &str,
        since: Option<chrono::DateTime<chrono::Utc>>,
        cursor: Option<chrono::DateTime<chrono::Utc>>,
        limit: u32,
    ) -> Result<JobsListResponse> {
        let url = format!("{}/jobs", self.base);
        let mut req = self
            .http
            .get(&url)
            .query(&[("status", status), ("limit", &limit.to_string())]);
        if let Some(s) = since {
            req = req.query(&[("since", s.to_rfc3339().as_str())]);
        }
        if let Some(c) = cursor {
            req = req.query(&[("cursor", c.to_rfc3339().as_str())]);
        }
        let resp = req.send().await?;
        decode(resp).await
    }

    pub async fn admin_snapshot(&self) -> Result<crate::types::AdminSnapshot> {
        let url = format!("{}/admin/snapshot", self.base);
        let resp = self.get(&url).send().await?;
        decode(resp).await
    }
}

/// Inject the W3C `traceparent` (and `tracestate`) header for the
/// current `tracing::Span`'s OTel context onto an outgoing request
/// so the coordinator can stitch the resulting server-side span into
/// the same distributed trace.
///
/// When OTel is disabled (no OTLP endpoint configured) the global
/// propagator is a `NoopTextMapPropagator` and this is effectively
/// free — no headers added, no allocations beyond the empty
/// `HeaderMap`.
fn inject_trace(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let cx = tracing::Span::current().context();
    let mut headers = reqwest::header::HeaderMap::new();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut HeaderMapInjector(&mut headers));
    });
    if headers.is_empty() {
        builder
    } else {
        builder.headers(headers)
    }
}

/// Adapter mapping OTel's `Injector` trait onto `reqwest::HeaderMap`.
/// We accept invalid header values silently — the propagator only
/// emits ASCII-safe strings, so this branch is unreachable in practice.
struct HeaderMapInjector<'a>(&'a mut reqwest::header::HeaderMap);

impl Injector for HeaderMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(k), Ok(v)) = (
            reqwest::header::HeaderName::from_bytes(key.as_bytes()),
            reqwest::header::HeaderValue::from_str(&value),
        ) {
            self.0.insert(k, v);
        }
    }
}

fn normalize(s: String) -> String {
    if let Some(stripped) = s.strip_suffix('/') {
        stripped.to_string()
    } else {
        s
    }
}

async fn decode<T: DeserializeOwned>(resp: reqwest::Response) -> Result<T> {
    let status = resp.status();
    if status.is_success() {
        let text = resp.text().await?;
        serde_json::from_str::<T>(&text)
            .map_err(|e| Error::Internal(format!("decode {status}: {e}: body={}", preview(&text))))
    } else {
        let text = resp.text().await.unwrap_or_default();
        let body = preview(&text);
        Err(match status {
            StatusCode::NOT_FOUND => Error::NotFound(body),
            StatusCode::GONE => Error::Gone(body),
            StatusCode::BAD_REQUEST => Error::BadRequest(body),
            StatusCode::PAYLOAD_TOO_LARGE => Error::PayloadTooLarge(body),
            StatusCode::UNAUTHORIZED => Error::Unauthorized(body),
            s if s.is_client_error() => Error::BadRequest(format!("{status}: {body}")),
            _ => Error::Internal(format!("{status}: {body}")),
        })
    }
}

fn preview(s: &str) -> String {
    if s.len() <= 512 {
        s.to_string()
    } else {
        format!("{}…", &s[..512])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injector_set_adds_header() {
        let mut h = reqwest::header::HeaderMap::new();
        let mut inj = HeaderMapInjector(&mut h);
        inj.set(
            "traceparent",
            "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".to_string(),
        );
        assert_eq!(
            h.get("traceparent").and_then(|v| v.to_str().ok()),
            Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
        );
    }

    #[test]
    fn injector_set_silently_drops_invalid_header_name() {
        // Spaces aren't allowed in header names; the propagator never
        // emits them, but if it ever did we don't want to panic.
        let mut h = reqwest::header::HeaderMap::new();
        let mut inj = HeaderMapInjector(&mut h);
        inj.set("bad name", "value".to_string());
        assert!(h.is_empty(), "invalid header name must be silently dropped");
    }

    #[test]
    fn inject_trace_is_no_op_without_propagator_context() {
        // With no OTel context active (no parent span, no propagator
        // registered), inject_trace must produce a builder with no
        // traceparent header. This is the production path when OTLP
        // is disabled.
        let client = reqwest::Client::new();
        let req = client.get("http://example.invalid/x");
        let injected = inject_trace(req);
        // We can't directly inspect headers on a RequestBuilder, but
        // we can build it into a Request and check the header set.
        let built = injected.build().expect("build");
        assert!(
            built.headers().get("traceparent").is_none(),
            "inject_trace with no active span must not add traceparent"
        );
    }
}
