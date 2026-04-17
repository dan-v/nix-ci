//! Regression test for H8: the check-then-attach in
//! `server::ingest_batch::wire_dep` was a TOCTOU race.
//!
//! If `dep` transitioned to `finished=true` BETWEEN the
//! `if dep.finished.load()` check and the subsequent `attach_dep`,
//! then `make_rdeps_runnable(dep)` had already iterated an rdep list
//! that didn't contain `parent` — and nothing ever re-propagated.
//! `parent` sat with a non-empty `deps` set forever;
//! `arm_if_leaf(parent)` short-circuited on `deps.is_empty() == false`
//! and the whole job stalled until the heartbeat reaper cancelled it.
//!
//! The fix (in `wire_dep`): after `attach_dep`, re-check
//! `dep.finished`. If it's now true, re-run the appropriate
//! propagation. This test exercises the post-race scenario — parent
//! attached to an already-finished dep — and verifies that re-running
//! `make_rdeps_runnable(dep)` arms `parent` correctly.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use nix_ci_core::dispatch::rdep::{attach_dep, make_rdeps_runnable};
use nix_ci_core::dispatch::Step;
use nix_ci_core::types::DrvHash;

fn mk(name: &str) -> Arc<Step> {
    let hash = DrvHash::new(format!("hash-{name}.drv"));
    Step::new(
        hash.clone(),
        format!("/nix/store/{hash}"),
        name.to_string(),
        "x86_64-linux".into(),
        Vec::new(),
        2,
    )
}

#[test]
fn wire_dep_race_close_arms_parent_on_already_finished_dep() {
    // Dep finishes FIRST (simulating the race: dep's /complete handler
    // ran between wire_dep's `if dep.finished.load()` check and its
    // subsequent `attach_dep` call).
    let dep = mk("dep");
    dep.finished.store(true, Ordering::Release);
    // Mimic handle_success — make_rdeps_runnable iterates the empty
    // rdep list at this point (parent not attached yet).
    make_rdeps_runnable(&dep);

    // Parent is ingested in a later batch. The ingest handler runs
    // attach_dep + attach_step_to_submission under the assumption
    // `dep.finished == false` (it was at check time). After the fix
    // in wire_dep, the handler re-checks `dep.finished` and, if
    // true, re-runs the appropriate propagation.
    let parent = mk("parent");
    attach_dep(&parent, &dep);
    // Mimic Phase 3 arm_if_leaf(parent): set created=true.
    parent.created.store(true, Ordering::Release);

    // The race-close: wire_dep's post-attach re-check.
    if dep.finished.load(Ordering::Acquire) {
        make_rdeps_runnable(&dep);
    }

    // After the race-close, parent should have its `deps` drained
    // (dep removed because it's already done) and its `runnable`
    // CAS'd to true.
    let deps_empty = parent.state.read().deps.is_empty();
    assert!(
        deps_empty,
        "parent.state.deps must be empty after race-close — re-run of \
         make_rdeps_runnable(dep) should have removed the finished dep"
    );
    assert!(
        parent.runnable.load(Ordering::Acquire),
        "parent.runnable must be armed after race-close"
    );
}
