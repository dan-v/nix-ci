//! Reproducer for H10: per-job `claim_deadline_secs: Some(0)` is
//! accepted by `POST /jobs`. Every claim on that job expires
//! immediately (deadline = now + 0s). The next reaper tick takes the
//! claim back, re-arms the step, another worker claims it — burning
//! worker cycles forever with `ignored:true` completions.
//!
//! `ServerConfig::validate()` enforces `claim_deadline_secs > 0` for
//! the server default but doesn't validate the per-job override from
//! `CreateJobRequest`. A single misconfigured client can cause a
//! dispatch thrash that wastes the entire fleet.
//!
//! This test just proves the acceptance; the thrash-behavior is
//! derivable from the design.

mod common;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::CreateJobRequest;
use sqlx::PgPool;

#[sqlx::test]
async fn create_job_rejects_zero_claim_deadline(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let req = CreateJobRequest {
        external_ref: Some("zero-deadline-probe".into()),
        claim_deadline_secs: Some(0),
        ..Default::default()
    };
    let result = client.create_job(&req).await;
    // Current behavior: server happily accepts the job, which will
    // thrash. The assertion here documents the fix: server must
    // reject with 400 BadRequest.
    assert!(
        result.is_err(),
        "create_job must reject claim_deadline_secs=0; got Ok({:?})",
        result.ok().map(|r| r.id)
    );
}
