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
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        let body = serde_json::json!({
            "error": self.to_string(),
            "code": status.as_u16(),
        });
        if status == axum::http::StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self, "request failed");
        } else {
            tracing::debug!(error = %self, "request rejected");
        }
        (status, axum::Json(body)).into_response()
    }
}
