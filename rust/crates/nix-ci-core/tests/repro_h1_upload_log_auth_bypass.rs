//! Reproducer for H1: `CoordinatorClient::upload_log` uses
//! `self.http.post(...)` directly instead of `self.post(...)`, so it
//! bypasses both `.auth()` (which would attach
//! `Authorization: Bearer <token>`) and `inject_trace()`.
//!
//! When the coordinator has `auth_bearer` configured, every build-log
//! archive upload gets rejected with 401 — silently, since the
//! worker-side caller (`upload_log_best_effort`) logs and continues.
//! Operators can never retrieve logs via
//! `GET /jobs/{id}/claims/{cid}/log`.

mod common;

use nix_ci_core::client::{BuildLogUploadMeta, CoordinatorClient};
use nix_ci_core::config::ServerConfig;
use nix_ci_core::types::{ClaimId, CreateJobRequest, DrvHash};
use sqlx::PgPool;

#[sqlx::test]
async fn upload_log_sends_bearer_token_under_auth(pool: PgPool) {
    let token = "secret-worker-token";
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some(token.into());
    })
    .await;
    let client = CoordinatorClient::with_auth(&handle.base_url, Some(token.to_string()));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .expect("authenticated create_job must succeed");

    let drv_hash = DrvHash::new("abc-test.drv".to_string());
    let now = chrono::Utc::now();
    let meta = BuildLogUploadMeta {
        drv_hash: &drv_hash,
        attempt: 1,
        original_size: 3,
        truncated: false,
        success: false,
        exit_code: Some(1),
        started_at: now,
        ended_at: now,
    };
    // A minimal valid gzip payload is not required — the server's
    // bearer-auth layer runs BEFORE the body is parsed, so 401
    // happens regardless of body contents.
    let gz = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        .to_vec();
    let result = client
        .upload_log(job.id, ClaimId::new(), meta, gz)
        .await;

    // The CORRECT behavior is either:
    //  - Ok(()): auth attached, upload accepted; OR
    //  - An error originating from the server-side handler (e.g. 404
    //    for unknown claim), NOT from the bearer-auth middleware.
    //
    // The BUG is that upload_log produces a 401 because the client
    // bypasses bearer-auth attachment. This assertion fails today
    // under the current client code and will pass once upload_log
    // is switched to the `self.post()` helper.
    match result {
        Ok(()) => {}
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                !msg.contains("401") && !msg.contains("Unauthorized") && !msg.contains("unauthorized"),
                "upload_log must attach the bearer token; got 401-like error: {msg}"
            );
        }
    }
}
