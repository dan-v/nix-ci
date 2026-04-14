//! `POST /jobs/{id}/drvs/batch` — bulk ingest for full-DAG submissions.
//!
//! Ingest is pure in-memory: the dispatcher owns the graph, and no
//! derivations/deps rows are written to Postgres. The only DB touch is
//! a bulk lookup against the `failed_outputs` TTL cache so we can pre-
//! mark drvs whose output we just saw fail elsewhere.
//!
//! Four phases — the split is load-bearing for the 8 invariants:
//! 1. `Steps::create` each drv in the batch (dedup on drv_hash).
//! 2. Wire membership + edges while `created` is still false on new
//!    Steps — safe because `make_rdeps_runnable` respects the barrier.
//! 3. Arm `created=true` → `runnable` on fresh leaves.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use super::ingest_common::{arm_if_leaf, attach_step_to_submission, reject_if_terminal, wire_dep};
use super::AppState;
use crate::dispatch::Step;
use crate::durable::writeback;
use crate::error::Result;
use crate::types::{drv_hash_from_path, IngestBatchRequest, IngestBatchResponse};

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

    let drv_path_refs: Vec<&str> = req.drvs.iter().map(|d| d.drv_path.as_str()).collect();
    let known_failed = writeback::failed_output_hits(&state.pool, &drv_path_refs).await;

    // Phase 1: look up or create every Step.
    let mut primary: Vec<(_, Arc<Step>, bool)> = Vec::with_capacity(req.drvs.len());
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
                state.cfg.max_attempts,
            )
        });
        let is_new = outcome.is_new();
        let step = outcome.into_step();
        if is_new && known_failed.contains(&d.drv_path) {
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

    // Phase 3: arm fresh leaves.
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
