//! Thin reqwest wrapper for the coordinator HTTP API. The worker and
//! submitter both use this.

use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::types::{
    ClaimId, ClaimResponse, CompleteRequest, CompleteResponse, CreateJobRequest, CreateJobResponse,
    HeartbeatResponse, IngestBatchRequest, IngestBatchResponse, IngestDrvRequest,
    IngestDrvResponse, JobId, JobStatusResponse, SealJobResponse,
};

#[derive(Clone)]
pub struct CoordinatorClient {
    base: String,
    http: Client,
}

impl CoordinatorClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(75))
            .user_agent(concat!("nix-ci/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client build");
        Self {
            base: normalize(base_url.into()),
            http,
        }
    }

    pub async fn create_job(&self, req: &CreateJobRequest) -> Result<CreateJobResponse> {
        let url = format!("{}/jobs", self.base);
        let resp = self.http.post(&url).json(req).send().await?;
        decode(resp).await
    }

    pub async fn ingest_drv(
        &self,
        job_id: JobId,
        req: &IngestDrvRequest,
    ) -> Result<IngestDrvResponse> {
        let url = format!("{}/jobs/{}/drvs", self.base, job_id);
        let resp = self.http.post(&url).json(req).send().await?;
        decode(resp).await
    }

    pub async fn ingest_batch(
        &self,
        job_id: JobId,
        req: &IngestBatchRequest,
    ) -> Result<IngestBatchResponse> {
        let url = format!("{}/jobs/{}/drvs/batch", self.base, job_id);
        let resp = self.http.post(&url).json(req).send().await?;
        decode(resp).await
    }

    pub async fn seal(&self, job_id: JobId) -> Result<SealJobResponse> {
        let url = format!("{}/jobs/{}/seal", self.base, job_id);
        let resp = self.http.post(&url).send().await?;
        decode(resp).await
    }

    pub async fn heartbeat(&self, job_id: JobId) -> Result<HeartbeatResponse> {
        let url = format!("{}/jobs/{}/heartbeat", self.base, job_id);
        let resp = self.http.post(&url).send().await?;
        if resp.status() == StatusCode::GONE {
            return Err(Error::Gone("job is terminal".into()));
        }
        decode(resp).await
    }

    pub async fn claim(
        &self,
        job_id: JobId,
        system: &str,
        features: &[String],
        wait_secs: u64,
    ) -> Result<Option<ClaimResponse>> {
        let url = format!("{}/jobs/{}/claim", self.base, job_id);
        let feat_csv = features.join(",");
        let resp = self
            .http
            .get(&url)
            .query(&[
                ("wait", wait_secs.to_string().as_str()),
                ("system", system),
                ("features", feat_csv.as_str()),
            ])
            .timeout(Duration::from_secs(wait_secs + 15))
            .send()
            .await?;
        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        let response: ClaimResponse = decode(resp).await?;
        Ok(Some(response))
    }

    pub async fn complete(
        &self,
        job_id: JobId,
        claim_id: ClaimId,
        req: &CompleteRequest,
    ) -> Result<CompleteResponse> {
        let url = format!("{}/jobs/{}/claims/{}/complete", self.base, job_id, claim_id);
        let resp = self.http.post(&url).json(req).send().await?;
        decode(resp).await
    }

    pub async fn status(&self, job_id: JobId) -> Result<JobStatusResponse> {
        let url = format!("{}/jobs/{}", self.base, job_id);
        let resp = self.http.get(&url).send().await?;
        decode(resp).await
    }

    pub fn events_url(&self, job_id: JobId) -> String {
        format!("{}/jobs/{}/events", self.base, job_id)
    }

    pub async fn admin_snapshot(&self) -> Result<crate::types::AdminSnapshot> {
        let url = format!("{}/admin/snapshot", self.base);
        let resp = self.http.get(&url).send().await?;
        decode(resp).await
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
            StatusCode::CONFLICT => Error::Conflict(body),
            StatusCode::BAD_REQUEST => Error::BadRequest(body),
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
