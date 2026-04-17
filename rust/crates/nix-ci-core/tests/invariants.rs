//! Unit tests for the eight dispatcher invariants. No Postgres —
//! pure in-memory graph mutations.
//!
//! The invariants (from V2_DESIGN §3):
//! 1. runnable=true ⟹ created=true ∧ deps.empty()
//! 2. runnable=true ⟹ tries==0 OR step already queued
//! 3. Steps::create dedups on drv_hash
//! 4. make_rdeps_runnable no-ops unless finished, respects created barrier
//! 5. No .await under parking_lot::RwLock (enforced by discipline; untestable here)
//! 6. Lock order Submissions → Steps → Step.state (enforced by discipline)
//! 7. Step.drv_path is immutable
//! 8. Steps registry holds Weak<Step>; strong refs only on Submission/deps

use std::sync::atomic::Ordering;
use std::sync::Arc;

use nix_ci_core::dispatch::rdep::{attach_dep, make_rdeps_runnable};
use nix_ci_core::dispatch::{Step, StepsRegistry, Submission};
use nix_ci_core::types::{DrvHash, JobId};

fn mk_step(hash: &str, deps: &[Arc<Step>]) -> Arc<Step> {
    let step = Step::new(
        DrvHash::new(hash.to_string()),
        format!("/nix/store/{hash}"),
        hash.to_string(),
        "x86_64-linux".into(),
        Vec::new(),
        2,
    );
    for d in deps {
        attach_dep(&step, d);
    }
    step
}

fn arm(step: &Arc<Step>) {
    step.created.store(true, Ordering::Release);
    if step.state.read().deps.is_empty() {
        step.runnable.store(true, Ordering::Release);
    }
}

// ─── Invariant 3: Steps::create dedups ────────────────────────────────

#[test]
fn invariant_3_dedup_returns_same_arc() {
    let registry = StepsRegistry::new();
    let hash = DrvHash::new("aaa-foo.drv".to_string());
    let (a_step, a_new) = registry.get_or_create(&hash, || {
        Step::new(
            hash.clone(),
            "/nix/store/aaa-foo.drv".into(),
            "foo".into(),
            "x86_64-linux".into(),
            vec![],
            2,
        )
    });
    assert!(a_new);

    let (b_step, b_new) = registry.get_or_create(&hash, || panic!("factory must not run on dup"));
    assert!(!b_new);

    assert!(Arc::ptr_eq(&a_step, &b_step));
    assert_eq!(registry.len(), 1);
}

// ─── Invariant 1: runnable precondition ───────────────────────────────

#[test]
fn invariant_1_leaf_can_be_armed() {
    let leaf = mk_step("leaf", &[]);
    arm(&leaf);
    assert!(leaf.runnable.load(Ordering::Acquire));
    assert!(leaf.created.load(Ordering::Acquire));
    assert!(leaf.state.read().deps.is_empty());
}

#[test]
fn invariant_1_non_leaf_is_not_runnable_until_deps_drain() {
    let leaf = mk_step("leaf", &[]);
    let parent = mk_step("parent", std::slice::from_ref(&leaf));

    parent.created.store(true, Ordering::Release);
    assert!(!parent.runnable.load(Ordering::Acquire));
    assert_eq!(parent.state.read().deps.len(), 1);

    // Simulate leaf completion
    leaf.finished.store(true, Ordering::Release);
    make_rdeps_runnable(&leaf);

    assert!(parent.runnable.load(Ordering::Acquire));
    assert!(parent.state.read().deps.is_empty());
}

// ─── Invariant 4: make_rdeps_runnable respects barriers ───────────────

#[test]
fn invariant_4_no_op_unless_finished() {
    let leaf = mk_step("leaf", &[]);
    let parent = mk_step("parent", std::slice::from_ref(&leaf));
    parent.created.store(true, Ordering::Release);

    // leaf.finished == false
    make_rdeps_runnable(&leaf);

    assert!(!parent.runnable.load(Ordering::Acquire));
    assert_eq!(parent.state.read().deps.len(), 1);
}

#[test]
fn invariant_4_respects_created_barrier_on_rdep() {
    let leaf = mk_step("leaf", &[]);
    let parent = mk_step("parent", std::slice::from_ref(&leaf));
    // parent.created = false (under construction)

    leaf.finished.store(true, Ordering::Release);
    make_rdeps_runnable(&leaf);

    // dep set still gets drained (parent.deps is now empty)
    assert!(parent.state.read().deps.is_empty());
    // but runnable is NOT armed because created=false
    assert!(!parent.runnable.load(Ordering::Acquire));

    // Now flip created — parent should become runnable via the
    // next ingest or sweep. In real code the ingest handler sets
    // created=true and re-checks deps-empty; we simulate that.
    parent.created.store(true, Ordering::Release);
    // Re-check: empty deps + created means we can arm
    assert!(parent.state.read().deps.is_empty());
    assert!(parent.created.load(Ordering::Acquire));
}

// ─── Cross-submission dedup via Steps registry ────────────────────────

#[test]
fn cross_submission_dedup_shares_step_arc() {
    let registry = StepsRegistry::new();
    let hash = DrvHash::new("shared-drv".to_string());

    let (a, _) = registry.get_or_create(&hash, || {
        Step::new(
            hash.clone(),
            "/nix/store/shared-drv".into(),
            "shared".into(),
            "x86_64-linux".into(),
            vec![],
            2,
        )
    });
    let sub_a = Submission::new(JobId::new(), 8);
    let sub_b = Submission::new(JobId::new(), 8);

    // Both submissions reference the same step
    sub_a.add_member(&a);
    sub_b.add_member(&a);
    {
        let mut st = a.state.write();
        st.submissions.push(Arc::downgrade(&sub_a));
        st.submissions.push(Arc::downgrade(&sub_b));
    }

    // Second submission hits dedup path
    let (b, _) = registry.get_or_create(&hash, || panic!("should not run"));
    assert!(Arc::ptr_eq(&a, &b));

    // Arm the step; both submissions' ready queues pick it up.
    arm(&a);
    sub_a.enqueue_ready(&a);
    sub_b.enqueue_ready(&a);

    let now_ms = chrono::Utc::now().timestamp_millis();
    let claim_a = sub_a.pop_runnable(&["x86_64-linux".into()], &[], now_ms);
    let claim_b = sub_b.pop_runnable(&["x86_64-linux".into()], &[], now_ms);

    // One of the two must win the CAS, the other gets None.
    match (claim_a, claim_b) {
        (Some(s), None) | (None, Some(s)) => assert_eq!(s.drv_hash().as_str(), "shared-drv"),
        _ => panic!("exactly one submission must win the CAS"),
    }
}

// ─── Feature matching ─────────────────────────────────────────────────

#[test]
fn feature_matching_skips_mismatched() {
    let registry = StepsRegistry::new();
    let hash = DrvHash::new("kvm-drv".to_string());
    let (step, _) = registry.get_or_create(&hash, || {
        Step::new(
            hash.clone(),
            "/nix/store/kvm-drv".into(),
            "kvm-test".into(),
            "x86_64-linux".into(),
            vec!["kvm".into()],
            2,
        )
    });
    arm(&step);

    let sub = Submission::new(JobId::new(), 8);
    sub.add_member(&step);
    {
        let mut st = step.state.write();
        st.submissions.push(Arc::downgrade(&sub));
    }
    sub.enqueue_ready(&step);

    let now_ms = chrono::Utc::now().timestamp_millis();
    // Worker without kvm — doesn't claim
    let miss = sub.pop_runnable(&["x86_64-linux".into()], &[], now_ms);
    assert!(miss.is_none());

    // Worker with kvm — claims
    let hit = sub.pop_runnable(&["x86_64-linux".into()], &["kvm".into()], now_ms);
    assert!(hit.is_some());
}

// ─── Retry backoff honors next_attempt_at ─────────────────────────────

#[test]
fn retry_backoff_defers_until_next_attempt_at() {
    let step = mk_step("retry-drv", &[]);
    arm(&step);

    // Simulate a flaky retry — backoff 60s into the future
    let future_ms = chrono::Utc::now().timestamp_millis() + 60_000;
    step.next_attempt_at.store(future_ms, Ordering::Release);

    let sub = Submission::new(JobId::new(), 8);
    sub.add_member(&step);
    {
        let mut st = step.state.write();
        st.submissions.push(Arc::downgrade(&sub));
    }
    sub.enqueue_ready(&step);

    let now_ms = chrono::Utc::now().timestamp_millis();
    let miss = sub.pop_runnable(&["x86_64-linux".into()], &[], now_ms);
    assert!(miss.is_none());

    let much_later = future_ms + 1000;
    let hit = sub.pop_runnable(&["x86_64-linux".into()], &[], much_later);
    assert!(hit.is_some());
}

// ─── Propagation on success: O(direct rdeps) ──────────────────────────

#[test]
fn propagation_chain_of_three() {
    let a = mk_step("a", &[]);
    let b = mk_step("b", std::slice::from_ref(&a));
    let c = mk_step("c", std::slice::from_ref(&b));

    for s in [&a, &b, &c] {
        s.created.store(true, Ordering::Release);
    }
    a.runnable.store(true, Ordering::Release);

    // Finish a
    a.finished.store(true, Ordering::Release);
    make_rdeps_runnable(&a);
    assert!(b.runnable.load(Ordering::Acquire));
    assert!(b.state.read().deps.is_empty());
    assert!(!c.runnable.load(Ordering::Acquire));

    // Finish b
    b.runnable.store(false, Ordering::Release);
    b.finished.store(true, Ordering::Release);
    make_rdeps_runnable(&b);
    assert!(c.runnable.load(Ordering::Acquire));
    assert!(c.state.read().deps.is_empty());
}

// ─── Diamond: node with two deps finishes only when both done ─────────

#[test]
fn diamond_needs_both_deps() {
    let leaf1 = mk_step("leaf1", &[]);
    let leaf2 = mk_step("leaf2", &[]);
    let top = mk_step("top", &[leaf1.clone(), leaf2.clone()]);
    for s in [&leaf1, &leaf2, &top] {
        s.created.store(true, Ordering::Release);
    }
    leaf1.runnable.store(true, Ordering::Release);
    leaf2.runnable.store(true, Ordering::Release);

    leaf1.finished.store(true, Ordering::Release);
    make_rdeps_runnable(&leaf1);
    assert!(!top.runnable.load(Ordering::Acquire));
    assert_eq!(top.state.read().deps.len(), 1);

    leaf2.finished.store(true, Ordering::Release);
    make_rdeps_runnable(&leaf2);
    assert!(top.runnable.load(Ordering::Acquire));
    assert!(top.state.read().deps.is_empty());
}

// ─── Weak-only registry: dropping strong refs GCs from registry ──────

#[test]
fn weak_registry_drops_orphaned_steps() {
    let registry = StepsRegistry::new();
    let hash = DrvHash::new("ephemeral".to_string());
    {
        let (step, _) = registry.get_or_create(&hash, || {
            Step::new(
                hash.clone(),
                "/nix/store/ephemeral".into(),
                "eph".into(),
                "x86_64-linux".into(),
                vec![],
                2,
            )
        });
        assert_eq!(Arc::strong_count(&step), 1);
    }
    // step dropped — registry holds only a Weak
    assert!(registry.get(&hash).is_none());
    let live = registry.live();
    assert!(live.is_empty());
}
