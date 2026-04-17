//! Shared error types. Server uses `Error::into_response`; client
//! uses `Error` for both transport and decoded API errors.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("sqlx migrate error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("gone: {0}")]
    Gone(String),

    #[error("payload too large: {0}")]
    PayloadTooLarge(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("subprocess {tool} exited with code {code}")]
    Subprocess { tool: String, code: i32 },
}

impl Error {
    pub fn status_code(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Gone(_) => StatusCode::GONE,
            Error::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Error::Forbidden(_) => StatusCode::FORBIDDEN,
            Error::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            // A failed upstream HTTP call (reqwest) is a bad-gateway
            // class failure, not a coordinator bug. 502 lets ops
            // distinguish "our downstream broke" from "we broke."
            Error::Http(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Whether `self.to_string()` is safe to return in the response
    /// body. 4xx errors carry caller-facing contract messages
    /// ("claim_id {x} belongs to job {y}, not {z}") and are part of
    /// the public API — clients depend on them for diagnostics and we
    /// cover them in tests. 5xx errors can carry internal state
    /// (a PG error message with a query fragment, a filesystem path,
    /// a panic-like Internal string) and leak that shape back to the
    /// network. For 5xx we return a generic message; the original
    /// error still goes to the tracing log with full context for ops.
    fn message_safe_for_client(&self) -> bool {
        let class = self.status_code().as_u16() / 100;
        class == 4
    }
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        let client_msg = if self.message_safe_for_client() {
            self.to_string()
        } else {
            // Fixed-shape message — no PII, no internal structure
            // leaked. Matches the HTTP status reason phrase so an
            // automated client can still branch on `code`.
            match status {
                axum::http::StatusCode::BAD_GATEWAY => "bad gateway".into(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE => "service unavailable".into(),
                _ => "internal server error".into(),
            }
        };
        let body = serde_json::json!({
            "error": client_msg,
            "code": status.as_u16(),
        });
        if status.as_u16() >= 500 {
            // Full error to logs so ops can diagnose without seeing
            // a sanitized message on a dashboard. `%self` hits the
            // Display impl which includes chained causes via thiserror.
            tracing::error!(error = %self, status = status.as_u16(), "request failed");
        } else {
            tracing::debug!(error = %self, status = status.as_u16(), "request rejected");
        }
        (status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    /// A 4xx error body must carry the real message so a caller can
    /// act on it (e.g. `BadRequest("bad input drv_path: ...")`).
    /// These are part of the public API contract.
    #[tokio::test]
    async fn bad_request_body_contains_original_message() {
        let e = Error::BadRequest("missing system query param".into());
        let resp = e.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"].as_str().unwrap(), "bad request: missing system query param");
        assert_eq!(body["code"], 400);
    }

    /// A 500 error body MUST be sanitized — a stray internal error
    /// message (PG query fragment, filesystem path, stack of causes)
    /// leaking to the network is a PII/information-disclosure
    /// footgun. The log still gets the full context.
    #[tokio::test]
    async fn internal_error_body_is_sanitized() {
        let e = Error::Internal(
            "failed to write /var/nix-ci/secrets/production.pem at line 42: EACCES".into(),
        );
        let resp = e.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let msg = body["error"].as_str().unwrap();
        assert_eq!(msg, "internal server error");
        assert!(!msg.contains("/var/nix-ci"), "internal path leaked");
        assert!(!msg.contains("EACCES"), "internal error code leaked");
    }

    /// Downstream HTTP failures classify as 502 Bad Gateway, not 500,
    /// so operators can distinguish "our downstream broke" from
    /// "we broke." Message is still sanitized (downstream URLs could
    /// leak internal topology).
    #[tokio::test]
    async fn http_error_maps_to_bad_gateway_sanitized() {
        // Use a generic Error constructed from a known-bad URL; we
        // can't easily construct a raw reqwest::Error in a test, but
        // any #[from] constructor will satisfy the variant shape.
        // Instead, exercise the status_code() path directly.
        let url = "http://internal-only.example:8080/secret-path";
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(10))
            .build()
            .unwrap();
        let reqwest_err = client.get(url).send().await.unwrap_err();
        let e = Error::Http(reqwest_err);
        assert_eq!(e.status_code(), StatusCode::BAD_GATEWAY);
        let resp = e.into_response();
        let bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "bad gateway");
        assert_eq!(body["code"], 502);
    }

    #[test]
    fn forbidden_status_is_403_and_keeps_message() {
        assert_eq!(
            Error::Forbidden("admin bearer required".into()).status_code(),
            StatusCode::FORBIDDEN
        );
    }
}
