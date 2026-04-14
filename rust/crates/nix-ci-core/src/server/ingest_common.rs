//! Helpers shared by single-drv and batch ingest handlers.
//!
//! The ingest endpoints are structurally identical: look up or create
//! a Step, wire it into the submission's membership set, attach dep
//! edges (creating placeholders for unknown deps), optionally mark as
//! root, then — after all edges are stable — arm runnable for fresh
//! leaves. Keeping this logic in one place prevents drift between
//! the two handlers (the cross-job dedup bug was caused by drift).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::AppState;
use crate::dispatch::rdep::{attach_dep, enqueue_for_all_submissions};
use crate::dispatch::{Step, Submission};
use crate::types::{drv_hash_from_path, JobId};

/// Attach a step to a submission: adds membership, pushes the
/// submission Weak ref onto `step.state.submissions` if not already
/// there, and — if the step is currently runnable for another
/// submission — enqueues it in our ready queue so our workers race
/// for the claim.
pub(super) fn attach_step_to_submission(sub: &Arc<Submission>, step: &Arc<Step>) {
    sub.add_member(step);
    let was_new_attach = step.state.write().attach_submission(sub);
    if was_new_attach
        && step.runnable.load(Ordering::Acquire)
        && !step.finished.load(Ordering::Acquire)
    {
        // Join an already-armed step — make sure our ready queue has
        // it so our workers can race for the claim. Whichever worker
        // wins the CAS builds it once for everyone.
        sub.enqueue_ready(step);
    }
}

/// Wire an edge from `parent` to `dep` (loading or placeholder-creating
/// the dep Step from the registry), attach the dep to the submission,
/// and, if the dep is already runnable, enqueue for this submission.
/// No-op if the dep is already finished.
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
                2,
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

/// Arm `runnable` on a fresh Step whose deps are all attached. Callers
/// must ensure `is_new=true` or otherwise confirm the step wasn't
/// already armed elsewhere. Invariant 1 still holds: `created=true`
/// before the CAS, and `deps.is_empty()` before the CAS succeeds.
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

/// Turn a drv path like `/nix/store/abc-foo-1.0.drv` into `foo-1.0` for
/// use in placeholder rows where we haven't yet received the real
/// metadata. Shared between ingest handlers and `writeback`.
pub(crate) fn placeholder_name_from(drv_path: &str) -> String {
    let base = drv_path.rsplit('/').next().unwrap_or(drv_path);
    let stripped = base.trim_end_matches(".drv");
    stripped
        .split_once('-')
        .map(|(_, rest)| rest)
        .unwrap_or(stripped)
        .to_string()
}

/// Reject if a job is terminal in Postgres (independent of in-memory
/// submission state — it may have been transitioned by another
/// coordinator instance). Used by all ingest endpoints.
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
        // No hyphen: whole stripped
        assert_eq!(placeholder_name_from("/nix/store/noprefix.drv"), "noprefix");
        // No slash: whole stripped
        assert_eq!(placeholder_name_from("bare.drv"), "bare");
        // No .drv suffix: unchanged
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
