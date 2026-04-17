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
use crate::observability::metrics::TerminalLabels;
use crate::types::{
    drv_hash_from_path, IngestBatchRequest, IngestBatchResponse, JobEvent, JobId, JobStatus,
    JobStatusResponse,
};

#[tracing::instrument(skip_all, fields(job_id = %id, batch_size = req.drvs.len()))]
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

    // Hard drv-cap. Runaway evals (10M-drv flake bug, accidental infinite
    // expansion) would OOM the coordinator long before they complete; a
    // cheap `members.len()` upper-bound check catches them at the first
    // batch that crosses the line. We auto-fail the job with a clean
    // sentinel so the caller sees a meaningful error rather than a
    // connection reset mid-ingest. Using the optimistic upper bound
    // (existing + incoming) avoids walking the incoming batch for dedup
    // first; any rejection is conservative.
    if let Some(cap) = state.cfg.max_drvs_per_job {
        let existing = sub.members.read().len() as u64;
        let incoming = req.drvs.len() as u64;
        if existing + incoming > u64::from(cap) {
            let reason = format!(
                "eval_too_large: job {id} would exceed max_drvs_per_job={cap} \
                 (existing={existing}, incoming={incoming})"
            );
            tracing::warn!(job_id = %id, cap, existing, incoming, "ingest rejected: over cap");
            auto_fail_oversized(&state, id, &reason).await?;
            return Err(crate::Error::PayloadTooLarge(reason));
        }
    }

    // The failed_outputs cache stores the OUTPUT path (drv_path with
    // `.drv` stripped — matches what `nix build` produces). Query in
    // the same form so inserts and lookups stay consistent.
    let output_path_refs: Vec<&str> = req
        .drvs
        .iter()
        .map(|d| d.drv_path.trim_end_matches(".drv"))
        .collect();
    let known_failed = writeback::failed_output_hits(&state.pool, &output_path_refs).await;

    // Phase 1: look up or create every Step.
    let mut primary: Vec<(_, Arc<Step>, bool)> = Vec::with_capacity(req.drvs.len());
    let mut errored: u32 = 0;
    for d in req.drvs {
        if d.drv_path.is_empty() || d.drv_name.is_empty() || d.system.is_empty() {
            errored += 1;
            continue;
        }
        if d.drv_path.len() > state.cfg.max_drv_path_bytes
            || d.drv_name.len() > state.cfg.max_drv_name_bytes
        {
            errored += 1;
            continue;
        }
        let Some(drv_hash) = drv_hash_from_path(&d.drv_path) else {
            errored += 1;
            continue;
        };
        let (step, is_new) = state.dispatcher.steps.get_or_create(&drv_hash, || {
            Step::new(
                drv_hash.clone(),
                d.drv_path.clone(),
                d.drv_name.clone(),
                d.system.clone(),
                d.required_features.clone(),
                state.cfg.max_attempts,
            )
        });
        let stripped: &str = d.drv_path.trim_end_matches(".drv");
        if is_new && known_failed.contains(stripped) {
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
            // Attribution: store the attr name (if the client supplied
            // one) keyed on the toplevel's drv_hash. The failure path
            // walks rdeps → toplevels to look up which attrs are
            // affected when a drv fails.
            if let Some(attr) = req_drv.attr.as_deref() {
                sub.root_attrs
                    .write()
                    .insert(step.drv_hash().clone(), attr.to_string());
            }
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

    // Soft warn (no hard cap): emit ONE log line per submission that
    // crosses the configured threshold, so an operator can spot a
    // runaway job before it becomes a memory problem. CAS on the
    // per-submission flag means we don't re-warn on every batch.
    let live_members = sub.members.read().len() as u32;
    if live_members >= state.cfg.submission_warn_threshold {
        use std::sync::atomic::Ordering;
        if sub
            .warned_oversized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            tracing::warn!(
                job_id = %sub.id,
                members = live_members,
                threshold = state.cfg.submission_warn_threshold,
                "submission crossed soft size threshold (no hard cap; this is a heads-up)"
            );
            state.metrics.inner.submission_warn_total.inc();
        }
    }

    Ok(Json(IngestBatchResponse {
        new_drvs,
        dedup_skipped,
        errored,
    }))
}

/// Force a job to terminal=Failed with a sentinel `eval_error` when an
/// ingest batch would push it over the configured drv cap. Runs entirely
/// outside the submission's normal path (no worker ever claimed a drv
/// on this job) so we build the snapshot from scratch.
async fn auto_fail_oversized(state: &AppState, id: JobId, reason: &str) -> Result<()> {
    let snapshot = JobStatusResponse {
        id,
        status: JobStatus::Failed,
        sealed: false,
        counts: crate::types::JobCounts::default(),
        failures: Vec::new(),
        eval_error: Some(reason.to_string()),
    };
    let snapshot_json = serde_json::to_value(&snapshot)
        .map_err(|e| crate::Error::Internal(format!("serialize oversized snapshot: {e}")))?;
    // Terminal write is idempotent (done_at IS NULL guard). If the job
    // was already forced terminal by a concurrent oversized batch, we
    // still return PayloadTooLarge to the caller — same outcome.
    let _ = writeback::transition_job_terminal(
        &state.pool,
        id,
        JobStatus::Failed.as_str(),
        &snapshot_json,
    )
    .await?;

    if let Some(sub) = state.dispatcher.submissions.remove(id) {
        state.dispatcher.evict_claims_for(id);
        if sub.mark_terminal() {
            sub.publish(JobEvent::JobDone {
                status: JobStatus::Failed,
                failures: Vec::new(),
            });
        }
    }
    state
        .metrics
        .inner
        .jobs_terminal
        .get_or_create(&TerminalLabels {
            status: JobStatus::Failed.as_str().into(),
        })
        .inc();
    state.dispatcher.wake();
    Ok(())
}
