//! `POST /jobs/{id}/drvs` — ingest a single drv.
//!
//! Kept for completeness (and for tests that exercise single-drv
//! semantics). Production clients use the batch endpoint.

use axum::extract::{Path, State};
use axum::Json;

use super::ingest_common::{arm_if_leaf, attach_step_to_submission, reject_if_terminal, wire_dep};
use super::AppState;
use crate::dispatch::Step;
use crate::durable::writeback::{self, UpsertDrv};
use crate::error::{Error, Result};
use crate::types::{drv_hash_from_path, IngestDrvRequest, IngestDrvResponse, JobId};

pub async fn submit_drv(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
    Json(req): Json<IngestDrvRequest>,
) -> Result<Json<IngestDrvResponse>> {
    if req.drv_path.is_empty() || req.drv_name.is_empty() || req.system.is_empty() {
        return Err(Error::BadRequest(
            "drv_path/drv_name/system are required".into(),
        ));
    }
    let drv_hash = drv_hash_from_path(&req.drv_path)
        .ok_or_else(|| Error::BadRequest(format!("bad drv_path: {}", req.drv_path)))?;

    reject_if_terminal(&state, id).await?;

    let is_known_failed = writeback::is_failed_output(&state.pool, &req.drv_path)
        .await
        .unwrap_or(false);

    let sub = state
        .dispatcher
        .submissions
        .get_or_insert(id, state.cfg.submission_event_capacity);

    let outcome = state.dispatcher.steps.get_or_create(&drv_hash, || {
        Step::new(
            drv_hash.clone(),
            req.drv_path.clone(),
            req.drv_name.clone(),
            req.system.clone(),
            req.required_features.clone(),
            2,
        )
    });
    let is_new = outcome.is_new();
    let step = outcome.into_step();

    if is_known_failed {
        step.previous_failure
            .store(true, std::sync::atomic::Ordering::Release);
        step.finished
            .store(true, std::sync::atomic::Ordering::Release);
    }

    attach_step_to_submission(&sub, &step);

    for input_path in &req.input_drvs {
        wire_dep(&state, &sub, &step, input_path, &req.system)?;
    }

    if req.is_root {
        sub.add_root(step.clone());
    }

    writeback::upsert_drv_and_deps(
        &state.pool,
        UpsertDrv {
            drv_hash: &drv_hash,
            drv_path: &req.drv_path,
            drv_name: &req.drv_name,
            system: &req.system,
            required_features: &req.required_features,
            max_attempts: 2,
        },
        &req.input_drvs,
        id,
        req.is_root,
    )
    .await?;

    if is_new {
        arm_if_leaf(&step);
        state.metrics.inner.drvs_ingested.inc();
    } else {
        state.metrics.inner.drvs_deduped.inc();
    }

    state.dispatcher.wake();

    Ok(Json(IngestDrvResponse {
        drv_hash,
        new_drv: is_new,
        dedup_skipped: !is_new,
    }))
}
