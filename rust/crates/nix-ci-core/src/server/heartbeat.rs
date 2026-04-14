//! `POST /jobs/{id}/heartbeat`. Refreshes the job's keep-alive clock.
//! Returns 410 Gone if the job is terminal.

use axum::extract::{Path, State};
use axum::http::StatusCode;

use super::AppState;
use crate::durable::writeback;
use crate::error::{Error, Result};
use crate::types::JobId;

pub async fn heartbeat(State(state): State<AppState>, Path(id): Path<JobId>) -> Result<StatusCode> {
    if writeback::heartbeat_job(&state.pool, id).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(Error::Gone(format!("job {id} is terminal")))
    }
}
