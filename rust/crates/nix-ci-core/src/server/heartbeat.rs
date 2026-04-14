//! `POST /jobs/{id}/heartbeat`. Refreshes the job's keep-alive clock.
//! Returns 410 Gone if the job is terminal.

use axum::extract::{Path, State};
use axum::Json;

use super::AppState;
use crate::durable::writeback;
use crate::error::{Error, Result};
use crate::types::{HeartbeatResponse, JobId};

pub async fn heartbeat(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
) -> Result<Json<HeartbeatResponse>> {
    let ok = writeback::heartbeat_job(&state.pool, id).await?;
    if !ok {
        return Err(Error::Gone(format!("job {id} is terminal")));
    }
    Ok(Json(HeartbeatResponse { ok: true }))
}
