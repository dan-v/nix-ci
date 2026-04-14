//! Shared helpers for the batch-ingest handler. Pure in-memory: no DB
//! writes here beyond the `reject_if_terminal` status check.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::AppState;
use crate::dispatch::rdep::{attach_dep, enqueue_for_all_submissions};
use crate::dispatch::{Step, Submission};
use crate::types::{drv_hash_from_path, JobId};

/// Attach a step to a submission. Adds the membership strong ref, pushes
/// a weak submission ref onto the step, and — if the step is already
/// runnable for some other submission — queues it on ours so our workers
/// race for the shared claim.
pub(super) fn attach_step_to_submission(sub: &Arc<Submission>, step: &Arc<Step>) {
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
/// the dep Step, attaches it to the submission, and enqueues it if it's
/// already runnable. No-op if the dep is finished.
pub(super) fn wire_dep(
    state: &AppState,
    sub: &Arc<Submission>,
    parent: &Arc<Step>,
    dep_path: &str,
    inherit_system: &str,
) -> crate::error::Result<()> {
    let dep_hash = drv_hash_from_path(dep_path)
        .ok_or_else(|| crate::Error::BadRequest(format!("bad input drv_path: {dep_path}")))?;
    let dep = state
        .dispatcher
        .steps
        .get_or_create(&dep_hash, || {
            Step::new(
                dep_hash.clone(),
                dep_path.to_string(),
                placeholder_name_from(dep_path),
                inherit_system.to_string(),
                Vec::new(),
                state.cfg.max_attempts,
            )
        })
        .into_step();
    if dep.finished.load(Ordering::Acquire) {
        return Ok(());
    }
    attach_dep(parent, &dep);
    attach_step_to_submission(sub, &dep);
    Ok(())
}

/// Arm `runnable` on a fresh Step whose deps are all attached. Invariant
/// 1: `created=true` before the CAS, and `deps.is_empty()` at CAS time.
pub(super) fn arm_if_leaf(step: &Arc<Step>) {
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
/// `/nix/store/abc-hello-1.0.drv` → `hello-1.0`. Used when we see a dep
/// edge to a drv we haven't received full metadata for yet.
pub(crate) fn placeholder_name_from(drv_path: &str) -> String {
    let base = drv_path.rsplit('/').next().unwrap_or(drv_path);
    let stripped = base.trim_end_matches(".drv");
    stripped
        .split_once('-')
        .map(|(_, rest)| rest)
        .unwrap_or(stripped)
        .to_string()
}

/// Reject ingest if the job is terminal in Postgres. A sealed-but-not-
/// terminal job is still accepting ingest (the sealed flag only prevents
/// *new roots*, not new drvs wired into existing roots).
pub(super) async fn reject_if_terminal(state: &AppState, id: JobId) -> crate::error::Result<()> {
    let row: Option<(String,)> = sqlx::query_as("SELECT status FROM jobs WHERE id = $1")
        .bind(id.0)
        .fetch_optional(&state.pool)
        .await?;
    let status = row
        .ok_or_else(|| crate::Error::NotFound(format!("job {id}")))?
        .0;
    if matches!(status.as_str(), "done" | "failed" | "cancelled") {
        return Err(crate::Error::Gone(format!("job {id} is terminal")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
