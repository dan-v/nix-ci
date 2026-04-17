//! Reproducer for H2: `CoordinatorClient::list_jobs` uses
//! `self.http.get(...)` directly instead of `self.get(...)`. It
//! bypasses `.auth()` (bearer token attachment) and `inject_trace()`.
//!
//! `nix-ci list` fails with 401 against any coordinator that has
//! `auth_bearer` configured.

mod common;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::config::ServerConfig;
use sqlx::PgPool;

#[sqlx::test]
async fn list_jobs_sends_bearer_token_under_auth(pool: PgPool) {
    let token = "the-right-token";
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.auth_bearer = Some(token.into());
    })
    .await;
    let client = CoordinatorClient::with_auth(&handle.base_url, Some(token.to_string()));

    // list_jobs bypasses the auth helper. The server's bearer-auth
    // middleware rejects it with 401 despite the client having the
    // correct token configured.
    let result = client.list_jobs("failed", None, None, 50).await;
    match result {
        Ok(_) => {}
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                !msg.contains("401") && !msg.contains("Unauthorized") && !msg.contains("unauthorized"),
                "list_jobs must attach bearer token; got 401-like error: {msg}"
            );
        }
    }
}
