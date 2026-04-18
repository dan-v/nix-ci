//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: bearer-auth enforcement and public-probe bypass.

mod common;

use nix_ci_core::client::{BuildLogUploadMeta, CoordinatorClient};
use nix_ci_core::types::{ClaimId, CreateJobRequest, DrvHash, JobStatus};
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

/// Every auth-required surface the client talks to must attach the
/// bearer token. Covers `CoordinatorClient` methods (create/status/
/// list_jobs/upload_log) and the SSE consumer path (`events_request`).
///
/// A broad guard: any future method that bypasses the shared `.auth()`
/// helper (e.g. calls `self.http.get` directly) will 401 here. That
/// class of bug silently disables log upload, job listing, or live
/// event streaming against any auth-enabled coordinator.
#[sqlx::test]
async fn all_client_surfaces_attach_bearer_token(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let token = "the-right-token";
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some(token.into());
    })
    .await;
    let client = CoordinatorClient::with_auth(&handle.base_url, Some(token.to_string()));

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .expect("create_job must succeed");
    assert_eq!(
        client
            .status(job.id)
            .await
            .expect("status must succeed")
            .status,
        JobStatus::Pending
    );
    client
        .list_jobs("failed", None, None, 50)
        .await
        .expect("list_jobs must attach bearer token");

    // upload_log: a 401 here means the client skipped `.auth()`.
    // Any other error is fine — the bearer-auth layer runs before
    // the body is parsed, so a legitimately-attached token gets past
    // it regardless of payload validity.
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
    let gz = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        .to_vec();
    if let Err(e) = client.upload_log(job.id, ClaimId::new(), meta, gz).await {
        let msg = format!("{e}");
        assert!(
            !msg.contains("401") && !msg.to_lowercase().contains("unauthorized"),
            "upload_log must attach bearer; got 401-like error: {msg}"
        );
    }

    // SSE consumer path. `events_request` is the authed builder the
    // SSE runner uses; any bypass here would make `nix-ci run` hang
    // on a 401'd stream.
    let resp = client
        .events_request(job.id)
        .send()
        .await
        .expect("events_request.send() must reach the server");
    assert_ne!(
        resp.status().as_u16(),
        401,
        "SSE events_request must attach bearer; got 401"
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
    assert!(
        create.status().is_success(),
        "worker token must create jobs"
    );
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
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // cancel is an admin-scoped route; with no admin_bearer it must
    // still accept the worker token.
    client
        .cancel(job.id)
        .await
        .expect("cancel must accept worker token");
}
