//! `make_rdeps_runnable` — the O(direct rdeps) propagation primitive
//! at the heart of the dispatcher. Invariant 4: this is a no-op
//! unless `self.finished`; it only flips an rdep runnable if that
//! rdep has `created=true` AND its deps set is empty after removing
//! self.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use super::step::{Step, StepHandle};
use super::submission::Submission;

/// Walk self's rdep list; decrement each rdep's pending-dep set by
/// removing self; for each rdep whose deps become empty (and whose
/// `created` flag is set), arm its `runnable` flag and enqueue it on
/// every submission that references it.
///
/// Must be called AFTER `self.finished.store(true)`.
pub fn make_rdeps_runnable(step: &Arc<Step>) {
    if !step.finished.load(Ordering::Acquire) {
        return;
    }
    // Take a write lock on our state to mutate rdeps (cull dead weaks).
    let mut state = step.state.write();
    let self_hash = step.drv_hash().clone();

    // We'll collect freshly-runnable steps and dispatch them outside
    // the lock to keep invariant 5 (no await under lock, and minimize
    // lock span).
    let mut to_arm: Vec<Arc<Step>> = Vec::new();

    state.rdeps.retain(|rdep_weak| {
        let Some(rdep) = rdep_weak.upgrade() else {
            return false;
        };
        let became_runnable = {
            let mut rdep_state = rdep.state.write();
            rdep_state.deps.retain(|s| s.drv_hash() != &self_hash);
            rdep_state.deps.is_empty() && rdep.created.load(Ordering::Acquire)
        };
        if became_runnable && !rdep.finished.load(Ordering::Acquire) {
            // CAS: only one path arms.
            if rdep
                .runnable
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                to_arm.push(rdep);
            }
        }
        true
    });

    // Release the lock before touching submissions (no await yet, but
    // discipline: we don't want to hold step.state while taking
    // submission.ready).
    drop(state);

    for rdep in to_arm {
        enqueue_for_all_submissions(&rdep);
    }
}

/// For a step that has just become runnable, push it onto each of its
/// submissions' ready queues.
pub fn enqueue_for_all_submissions(step: &Arc<Step>) {
    let subs: Vec<Arc<Submission>> = {
        let state = step.state.read();
        state.submissions.iter().filter_map(Weak::upgrade).collect()
    };
    for sub in subs {
        sub.enqueue_ready(step);
    }
}

/// Attach a step→dep edge in a lock-ordered way. Caller holds no
/// locks. The step's state is mutated, then the dep's state is
/// mutated to add a reverse weak reference.
pub fn attach_dep(step: &Arc<Step>, dep: &Arc<Step>) {
    // Invariant 6: Step.state lock order is consistent — always mutate
    // the "child" step first, then the "dep" for its rdep list.
    // Actually, there is no total order on arbitrary pairs here, but
    // the DAG is acyclic so a single mutation pair can't deadlock with
    // itself. Two tasks attaching different edges that happen to share
    // an endpoint *could* race — the narrowing inner-scope pattern
    // keeps each critical section tiny.
    {
        let mut state = step.state.write();
        state.deps.insert(StepHandle(dep.clone()));
    }
    {
        let mut dep_state = dep.state.write();
        dep_state.rdeps.push(Arc::downgrade(step));
    }
}
