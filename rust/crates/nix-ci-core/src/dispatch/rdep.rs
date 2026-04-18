//! `make_rdeps_runnable` — the O(direct rdeps) propagation primitive
//! at the heart of the dispatcher. Invariant 4: this is a no-op
//! unless `self.finished`; it only flips an rdep runnable if that
//! rdep has `created=true` AND its deps set is empty after removing
//! self.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Instant;

use super::step::{Step, StepHandle};
use super::submission::Submission;

/// Observe the wall-clock cost of a single `make_rdeps_runnable` call
/// into the `rdep_propagation_duration_seconds` histogram. Wrapped so
/// dispatcher paths without a Metrics handle in scope (pure-graph
/// tests) can still call the unobserved variant.
pub fn make_rdeps_runnable_observed(
    step: &Arc<Step>,
    metrics: &crate::observability::metrics::Metrics,
) {
    let t0 = Instant::now();
    make_rdeps_runnable(step);
    metrics
        .inner
        .rdep_propagation_duration_seconds
        .observe(t0.elapsed().as_secs_f64());
}

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

/// Re-arm a step whose claim just vanished (reaper deadline or admin
/// evict) so another worker can pick it up. Guards against the race
/// where a concurrent failure-propagation path flips `finished` to true
/// between our `finished` probe and the `runnable=true` store — we
/// re-check and undo so we never enqueue a queue entry that's already
/// terminal.
///
/// No-op if the step is already `finished`. `pop_runnable` also skips
/// finished steps, so the undo is belt-and-suspenders cleanup rather
/// than a correctness boundary — preserving the property that the
/// ready deque contains only live candidates keeps the hot path honest.
pub fn rearm_step_if_live(step: &Arc<Step>) {
    use std::sync::atomic::Ordering;
    if step.finished.load(Ordering::Acquire) {
        return;
    }
    step.runnable.store(true, Ordering::Release);
    if step.finished.load(Ordering::Acquire) {
        step.runnable.store(false, Ordering::Release);
    } else {
        enqueue_for_all_submissions(step);
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
