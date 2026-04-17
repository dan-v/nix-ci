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

/// Worker bearer token is rejected with 403 on admin-scoped endpoints
/// when `admin_bearer` is configured. The admin bearer itself must
/// succeed. Guards against privilege escalation if a worker token leaks.
#[sqlx::test]
async fn admin_bearer_separates_scope(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some("worker-tok".into());
        cfg.admin_bearer = Some("admin-tok".into());
    })
    .await;
    let http = reqwest::Client::new();

    // Create a job with the worker token (worker scope).
    let create = http
        .post(format!("{}/jobs", handle.base_url))
        .bearer_auth("worker-tok")
        .json(&CreateJobRequest::default())
        .send()
        .await
        .unwrap();
    assert!(create.status().is_success(), "worker token must create jobs");
    let job: serde_json::Value = create.json().await.unwrap();
    let job_id = job["id"].as_str().unwrap().to_string();

    // Worker token on an admin endpoint: 403.
    let forbidden = http
        .delete(format!("{}/jobs/{}/cancel", handle.base_url, job_id))
        .bearer_auth("worker-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(
        forbidden.status(),
        reqwest::StatusCode::FORBIDDEN,
        "worker token on admin route must be 403, not 401"
    );

    // Admin token on the same endpoint: success.
    let allowed = http
        .delete(format!("{}/jobs/{}/cancel", handle.base_url, job_id))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap();
    assert!(
        allowed.status().is_success(),
        "admin token must cancel job; got {}",
        allowed.status()
    );
}

/// Per-handler request timeout: long-poll routes must still work
/// (they have their own `wait` semantics) even when the server-wide
/// timeout is tighter than the poll window. Guards against the
/// timeout middleware accidentally being applied where it shouldn't.
#[sqlx::test]
async fn request_timeout_does_not_truncate_long_poll(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        // Tighter than the wait we request below — if the middleware
        // was layered over long-poll routes, this test would 503.
        cfg.request_timeout_secs = 1;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // Long-poll with wait=2s; no drvs ingested → should return None
    // after ~2s, not 503.
    let start = std::time::Instant::now();
    let resp = client
        .claim(job.id, "x86_64-linux", &[], 2)
        .await
        .expect("long-poll claim must not 503 under per-handler timeout");
    let elapsed = start.elapsed();
    assert!(resp.is_none(), "no ingested drvs → no claim");
    assert!(
        elapsed >= std::time::Duration::from_millis(900),
        "long-poll must run its wait window; elapsed {elapsed:?}"
    );
}

/// When `admin_bearer` is unset, the worker token continues to work
/// on admin endpoints — backwards-compat for single-token deployments.
#[sqlx::test]
async fn admin_routes_accept_worker_token_when_no_admin_bearer(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some("single-tok".into());
        cfg.admin_bearer = None;
    })
    .await;
    let client = CoordinatorClient::with_auth(&handle.base_url, Some("single-tok".into()));
    let job = client.create_job(&CreateJobRequest::default()).await.unwrap();
    // cancel is an admin-scoped route; with no admin_bearer it must
    // still accept the worker token.
    client.cancel(job.id).await.expect("cancel must accept worker token");
}
