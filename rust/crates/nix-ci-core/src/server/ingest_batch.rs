//! `POST /jobs/{id}/drvs/batch` — bulk ingest for full-DAG submissions.
//!
//! Four phases — the split is load-bearing for the 8 invariants:
//! 1. `Steps::create` each drv in the batch (dedup on drv_hash).
//! 2. Wire membership + edges while `created` is still false on new
//!    Steps — safe because `make_rdeps_runnable` respects the barrier.
//! 3. Durable UNNEST bulk write-through.
//! 4. Arm `created=true` → `runnable` on fresh leaves.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use super::ingest_common::{
    arm_if_leaf, attach_step_to_submission, failed_output_hits, reject_if_terminal, wire_dep,
};
use super::AppState;
use crate::dispatch::Step;
use crate::durable::writeback::{self, BatchDrv};
use crate::error::Result;
use crate::types::{drv_hash_from_path, IngestBatchRequest, IngestBatchResponse, IngestDrvRequest};

pub async fn submit_batch(
    State(state): State<AppState>,
    Path(id): Path<crate::types::JobId>,
    Json(req): Json<IngestBatchRequest>,
) -> Result<Json<IngestBatchResponse>> {
    if req.drvs.is_empty() {
        return Ok(Json(IngestBatchResponse {
            new_drvs: 0,
            dedup_skipped: 0,
            errored: 0,
        }));
    }
    reject_if_terminal(&state, id).await?;
    let sub = state
        .dispatcher
        .submissions
        .get_or_insert(id, state.cfg.submission_event_capacity);

    // Bulk-lookup failed-output cache hits so we can pre-finish any
    // drv whose output path recently failed. Matches the per-drv
    // check in `ingest::submit_drv`; kept in one place so the two
    // ingest paths don't drift.
    let drv_path_refs: Vec<&str> = req.drvs.iter().map(|d| d.drv_path.as_str()).collect();
    let known_failed = failed_output_hits(&state, &drv_path_refs).await;

    // Phase 1: look up or create every Step. Hold NO per-Step locks
    // beyond the brief write inside Steps::get_or_create.
    #[allow(clippy::type_complexity)]
    let mut primary: Vec<(IngestDrvRequest, Arc<Step>, bool)> = Vec::with_capacity(req.drvs.len());
    let mut errored: u32 = 0;
    for d in req.drvs {
        if d.drv_path.is_empty() || d.drv_name.is_empty() || d.system.is_empty() {
            errored += 1;
            continue;
        }
        let Some(drv_hash) = drv_hash_from_path(&d.drv_path) else {
            errored += 1;
            continue;
        };
        let outcome = state.dispatcher.steps.get_or_create(&drv_hash, || {
            Step::new(
                drv_hash.clone(),
                d.drv_path.clone(),
                d.drv_name.clone(),
                d.system.clone(),
                d.required_features.clone(),
                2,
            )
        });
        let is_new = outcome.is_new();
        let step = outcome.into_step();
        if is_new && known_failed.contains(&d.drv_path) {
            // Pre-mark as a known failure so the dispatcher short-
            // circuits the build loop without contacting a worker.
            step.previous_failure
                .store(true, std::sync::atomic::Ordering::Release);
            step.finished
                .store(true, std::sync::atomic::Ordering::Release);
        }
        primary.push((d, step, is_new));
    }

    // Phase 2: membership + edges.
    let mut new_drvs: u32 = 0;
    let mut dedup_skipped: u32 = 0;
    for (req_drv, step, is_new) in &primary {
        attach_step_to_submission(&sub, step);
        for dep_path in &req_drv.input_drvs {
            if let Err(e) = wire_dep(&state, &sub, step, dep_path, &req_drv.system) {
                tracing::warn!(error = %e, "batch: wire_dep rejected");
                errored += 1;
            }
        }
        if req_drv.is_root {
            sub.add_root(step.clone());
        }
        if *is_new {
            new_drvs += 1;
        } else {
            dedup_skipped += 1;
        }
    }

    // Phase 3: durable write-through via UNNEST.
    let batch: Vec<BatchDrv> = primary
        .iter()
        .map(|(req_drv, step, _)| BatchDrv {
            drv_hash: step.drv_hash().as_str().to_string(),
            drv_path: req_drv.drv_path.clone(),
            drv_name: req_drv.drv_name.clone(),
            system: req_drv.system.clone(),
            required_features: req_drv.required_features.clone(),
            max_attempts: 2,
            input_drvs: req_drv.input_drvs.clone(),
            is_root: req_drv.is_root,
        })
        .collect();
    writeback::upsert_drvs_batch(&state.pool, id, &batch).await?;

    // Phase 4: arm fresh leaves. `arm_if_leaf` enforces invariant 1:
    // `created=true` before the runnable CAS, deps still empty at the
    // moment of the CAS (the lock inside make_rdeps_runnable holds
    // the relationship with concurrent edge changes elsewhere).
    for (_, step, is_new) in &primary {
        if *is_new {
            arm_if_leaf(step);
        }
    }

    state.dispatcher.wake();
    state.metrics.inner.drvs_ingested.inc_by(new_drvs as u64);
    state
        .metrics
        .inner
        .drvs_deduped
        .inc_by(dedup_skipped as u64);

    Ok(Json(IngestBatchResponse {
        new_drvs,
        dedup_skipped,
        errored,
    }))
}
