//! Reproducer for H3: `runner::sse::print_events_with` constructs a
//! brand-new `reqwest::Client::new()` that has no bearer token. The
//! `/jobs/{id}/events` route is NOT public — the bearer-auth layer
//! rejects the request with 401, and `print_events_with` surfaces an
//! `Error::Internal("SSE /events returned 401 Unauthorized ...")`.
//!
//! Net effect: under `auth_bearer`, `nix-ci run` can never receive
//! any events. Terminal detection via SSE fails and the run never
//! observes `JobDone` via that channel.

mod common;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::config::ServerConfig;
use nix_ci_core::runner::sse;
use nix_ci_core::types::CreateJobRequest;
use sqlx::PgPool;
use std::sync::Arc;

#[sqlx::test]
async fn sse_consumer_uses_bearer_token_under_auth(pool: PgPool) {
    let token = "secret-token";
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some(token.into());
    })
    .await;
    let client = Arc::new(CoordinatorClient::with_auth(
        &handle.base_url,
        Some(token.to_string()),
    ));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .expect("authenticated create_job must succeed");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    // Spawn the SSE consumer; it should connect successfully and
    // receive a terminal event when we cancel below.
    let sse_task = {
        let client = client.clone();
        let job_id = job.id;
        tokio::spawn(async move {
            sse::print_events(client, job_id, shutdown_tx, shutdown_rx).await
        })
    };

    // Give the SSE consumer a moment to connect. If the client
    // bypassed bearer-auth (the original bug), the stream errors
    // with a 401 before we ever get here.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Cancel the job so the SSE sees JobDone(Cancelled) and returns.
    client.cancel(job.id).await.expect("cancel must succeed");

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), sse_task)
        .await
        .expect("sse must terminate promptly after cancel")
        .expect("sse task must not panic");

    match result {
        Ok(_) => {}
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                !msg.contains("401")
                    && !msg.contains("Unauthorized")
                    && !msg.contains("unauthorized"),
                "SSE consumer must attach bearer token; got 401-like error: {msg}"
            );
        }
    }
}
