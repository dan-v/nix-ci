//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: bearer-auth enforcement and public-probe bypass.

mod common;


use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CreateJobRequest,
    JobStatus,
};
use sqlx::PgPool;


/// With `auth_bearer` configured, a request without a matching token
/// must be rejected with 401 on mutating endpoints.
#[sqlx::test]
async fn unauthenticated_request_rejected_with_401(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some("secret-token".into());
    })
    .await;
    // No token: POST /jobs must 401.
    let resp = reqwest::Client::new()
        .post(format!("{}/jobs", handle.base_url))
        .json(&CreateJobRequest::default())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
}

/// Monitoring endpoints (`/healthz`, `/readyz`, `/metrics`) must stay
/// reachable without a token even when auth is enabled — otherwise the
/// first config mistake locks Prometheus / kubelet out of the process.
#[sqlx::test]
async fn public_probes_bypass_auth(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some("secret-token".into());
    })
    .await;
    let http = reqwest::Client::new();
    for path in &["/healthz", "/health", "/readyz", "/metrics"] {
        let resp = http
            .get(format!("{}{}", handle.base_url, path))
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "{path} must be reachable without a token; got {}",
            resp.status()
        );
    }
}

/// A client with the matching token must succeed end-to-end. Also
/// verifies CoordinatorClient::with_auth plumbs the header correctly.
#[sqlx::test]
async fn authenticated_client_reaches_mutating_endpoint(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let token = "abc123";
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some(token.into());
    })
    .await;
    let client = CoordinatorClient::with_auth(&handle.base_url, Some(token.to_string()));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .expect("authenticated create_job must succeed");
    assert_eq!(
        client
            .status(job.id)
            .await
            .expect("authenticated status must succeed")
            .status,
        JobStatus::Pending
    );
}

/// Wrong token must 401; proves the comparison isn't "any non-empty
/// token wins".
#[sqlx::test]
async fn wrong_token_is_rejected(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some("the-right-token".into());
    })
    .await;
    let client =
        CoordinatorClient::with_auth(&handle.base_url, Some("the-wrong-token".to_string()));
    match client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap_err()
    {
        nix_ci_core::Error::Unauthorized(_) => {}
        other => panic!("expected Unauthorized, got {other:?}"),
    }
}
