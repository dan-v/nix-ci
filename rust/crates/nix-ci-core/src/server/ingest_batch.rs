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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use super::AppState;
use crate::dispatch::rdep::{attach_dep, enqueue_for_all_submissions};
use crate::dispatch::{Step, Submission};
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
    // H3: track batch size distribution so slow-ingest incidents can be
    // attributed to a runaway submitter emitting unusually large batches.
    state
        .metrics
        .inner
        .ingest_batch_drvs
        .observe(req.drvs.len() as f64);
    if req.drvs.is_empty() && req.eval_errors.is_empty() {
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

    // Eval errors are independent of drv batching: record them before
    // any drv-related work so a batch carrying only eval errors (no
    // drvs — possible when nix-eval-jobs's remaining input is all
    // broken attrs) still delivers them.
    if !req.eval_errors.is_empty() {
        sub.append_eval_errors(req.eval_errors.iter().cloned());
    }

    // Secondary seal-check against the in-memory flag. `reject_if_terminal`
    // reads the DB; a seal call that already flipped the in-memory flag
    // but hasn't written to Postgres yet would slip past it. Catching
    // this narrows (but does not eliminate) the ingest-vs-seal race
    // around cycle detection — a racing ingest can still land between
    // seal's check and writeback. That residual race is acceptable:
    // cycle detection is a safety net, not a hard consistency boundary.
    if sub.is_sealed() {
        return Err(crate::Error::Gone(format!(
            "job {id} is sealed; no further ingest accepted"
        )));
    }

    // Hard drv-cap — race-safe reservation via an atomic counter.
    // Prior versions read `members.read().len()` and compared against
    // the cap, which let two concurrent batches both read the same
    // baseline and both pass the check, overshooting the cap silently.
    // `try_reserve_drvs` uses fetch_add + range-check + rollback so
    // concurrent batches whose combined size would exceed the cap
    // both get a false return and auto-fail the job.
    let incoming_u32: u32 = req.drvs.len().try_into().unwrap_or(u32::MAX);
    if let Some(cap) = state.cfg.max_drvs_per_job {
        if !sub.try_reserve_drvs(incoming_u32, cap) {
            let reserved = sub.reserved_drvs.load(Ordering::Acquire);
            let reason = format!(
                "eval_too_large: job {id} would exceed max_drvs_per_job={cap} \
                 (reserved={reserved}, incoming={incoming_u32})"
            );
            tracing::warn!(
                job_id = %id, cap, reserved, incoming = incoming_u32,
                "ingest rejected: over cap"
            );
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
        // Bound free-form attribution string so a broken submitter
        // can't pollute JSONB snapshots with megabytes of attr name.
        if let Some(a) = d.attr.as_deref() {
            if a.len() > state.cfg.max_identifier_bytes {
                errored += 1;
                continue;
            }
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
                .store(true, Ordering::Release);
            step.finished
                .store(true, Ordering::Release);
            state.metrics.inner.failed_outputs_hits_total.inc();
        }
        primary.push((d, step, is_new));
    }

    // Post-Phase-1 refinement of the drv-cap reservation: the initial
    // `try_reserve_drvs` reserved against the raw incoming count
    // (conservative upper bound). Now that Phase 1 has computed dedup
    // hits, release the deduped slots back to the counter so streams
    // with heavy cross-batch overlap don't spuriously trip the cap.
    let dedup_in_batch: u32 = primary
        .iter()
        .filter(|(_, _, is_new)| !*is_new)
        .count()
        .try_into()
        .unwrap_or(u32::MAX);
    if dedup_in_batch > 0 && state.cfg.max_drvs_per_job.is_some() {
        sub.reserved_drvs
            .fetch_sub(dedup_in_batch, Ordering::AcqRel);
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
    if live_members >= state.cfg.submission_warn_threshold
        && sub
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

    Ok(Json(IngestBatchResponse {
        new_drvs,
        dedup_skipped,
        errored,
    }))
}

// ─── Helpers (previously in ingest_common.rs) ─────────────────────────
//
// These were split out when there were two ingest handlers (single +
// batch). After the single-drv endpoint was removed, the batch handler
// is the only caller, so the helpers live here directly. Keeping them
// `fn` (not `pub(super)`) makes the module boundary visible.

/// Attach a step to a submission. Adds the membership strong ref,
/// pushes a weak submission ref onto the step, and — if the step is
/// already runnable for some other submission — queues it on ours so
/// our workers race for the shared claim.
fn attach_step_to_submission(sub: &Arc<Submission>, step: &Arc<Step>) {
    sub.add_member(step);
    let was_new_attach = step.state.write().attach_submission(sub);
    if was_new_attach
        && step.runnable.load(Ordering::Acquire)
        && !step.finished.load(Ordering::Acquire)
    {
        sub.enqueue_ready(step);
    }
}

/// Wire an edge from `parent` to `dep`. Loads or placeholder-creates
/// the dep Step, attaches it to the submission, and enqueues it if
/// it's already runnable. No-op if the dep is finished.
fn wire_dep(
    state: &AppState,
    sub: &Arc<Submission>,
    parent: &Arc<Step>,
    dep_path: &str,
    inherit_system: &str,
) -> Result<()> {
    let dep_hash = drv_hash_from_path(dep_path)
        .ok_or_else(|| crate::Error::BadRequest(format!("bad input drv_path: {dep_path}")))?;
    // Cheap self-loop guard. The post-seal cycle scan catches longer
    // cycles (A→B→A etc.); this anchor prevents the simplest bad input
    // from ever entering the graph and reaching `attach_dep` with
    // parent == dep (which would wedge a step forever).
    if parent.drv_hash() == &dep_hash {
        return Err(crate::Error::BadRequest(format!(
            "self-loop: drv {dep_path} depends on itself"
        )));
    }
    let (dep, _) = state.dispatcher.steps.get_or_create(&dep_hash, || {
        Step::new(
            dep_hash.clone(),
            dep_path.to_string(),
            placeholder_name_from(dep_path),
            inherit_system.to_string(),
            Vec::new(),
            state.cfg.max_attempts,
        )
    });
    if dep.finished.load(Ordering::Acquire) {
        return Ok(());
    }
    attach_dep(parent, &dep);
    attach_step_to_submission(sub, &dep);
    Ok(())
}

/// Arm `runnable` on a fresh Step whose deps are all attached.
/// Invariant 1: `created=true` before the CAS, and `deps.is_empty()`
/// at CAS time.
fn arm_if_leaf(step: &Arc<Step>) {
    if step.finished.load(Ordering::Acquire) {
        return;
    }
    step.created.store(true, Ordering::Release);
    let deps_empty = step.state.read().deps.is_empty();
    if deps_empty
        && step
            .runnable
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    {
        enqueue_for_all_submissions(step);
    }
}

/// Derive a placeholder drv_name from a drv_path like
/// `/nix/store/abc-hello-1.0.drv` → `hello-1.0`. Used when we see a
/// dep edge to a drv we haven't received full metadata for yet.
fn placeholder_name_from(drv_path: &str) -> String {
    let base = drv_path.rsplit('/').next().unwrap_or(drv_path);
    let stripped = base.trim_end_matches(".drv");
    stripped
        .split_once('-')
        .map(|(_, rest)| rest)
        .unwrap_or(stripped)
        .to_string()
}

/// Reject ingest if the job is terminal OR sealed. Once a submission
/// is sealed the caller has told us "no more drvs coming" — a late
/// ingest call after seal is almost certainly a bug (or a lost
/// retry / out-of-order client), and accepting it could re-open a
/// submission that already fired `JobDone`. We reject with 410 Gone
/// so the client sees a clear terminal signal.
async fn reject_if_terminal(state: &AppState, id: JobId) -> Result<()> {
    let row: Option<(String, bool)> =
        sqlx::query_as("SELECT status, sealed FROM jobs WHERE id = $1")
            .bind(id.0)
            .fetch_optional(&state.pool)
            .await?;
    let (status, sealed) = row.ok_or_else(|| crate::Error::NotFound(format!("job {id}")))?;
    if matches!(status.as_str(), "done" | "failed" | "cancelled") {
        return Err(crate::Error::Gone(format!("job {id} is terminal")));
    }
    if sealed {
        return Err(crate::Error::Gone(format!(
            "job {id} is sealed; no further ingest accepted"
        )));
    }
    Ok(())
}

/// Force a job to terminal=Failed with a sentinel `eval_error` when an
/// ingest batch would push it over the configured drv cap. Runs entirely
/// outside the submission's normal path (no worker ever claimed a drv
/// on this job) so we build the snapshot from scratch.
async fn auto_fail_oversized(state: &AppState, id: JobId, reason: &str) -> Result<()> {
    let eval_errors = state
        .dispatcher
        .submissions
        .get(id)
        .map(|sub| sub.eval_errors.read().clone())
        .unwrap_or_default();
    let snapshot = JobStatusResponse {
        id,
        status: JobStatus::Failed,
        sealed: false,
        counts: crate::types::JobCounts::default(),
        failures: Vec::new(),
        eval_error: Some(reason.to_string()),
        eval_errors,
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

#[cfg(test)]
mod helper_tests {
    use super::*;

    #[test]
    fn placeholder_name_handles_common_shapes() {
        assert_eq!(
            placeholder_name_from("/nix/store/abc-hello-2.12.1.drv"),
            "hello-2.12.1"
        );
        assert_eq!(
            placeholder_name_from("/nix/store/hash-stdenv-linux.drv"),
            "stdenv-linux"
        );
        assert_eq!(placeholder_name_from("/nix/store/noprefix.drv"), "noprefix");
        assert_eq!(placeholder_name_from("bare.drv"), "bare");
        assert_eq!(placeholder_name_from("/nix/store/hash-foo"), "foo");
    }

    #[test]
    fn drv_hash_from_path_rejects_empty_and_trivial() {
        assert!(crate::types::drv_hash_from_path("").is_none());
        assert!(crate::types::drv_hash_from_path("nodrv").is_none());
        assert!(crate::types::drv_hash_from_path("/nix/store/nohyphen.drv").is_none());
        assert!(crate::types::drv_hash_from_path("/nix/store/hash-foo.drv").is_some());
    }
}
