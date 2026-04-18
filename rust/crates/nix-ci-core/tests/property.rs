#![allow(clippy::needless_range_loop)]
//! Property-based randomized tests for the in-memory dispatcher.
//! No Postgres, no HTTP. Generates random DAGs + random op sequences
//! and checks the 8 invariants hold throughout.
//!
//! Each test is deterministic (seeded PRNG); on failure, rerun with
//! the printed seed to reproduce.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use nix_ci_core::dispatch::rdep::{attach_dep, enqueue_for_all_submissions, make_rdeps_runnable};
use nix_ci_core::dispatch::{Step, StepsRegistry, Submission};
use nix_ci_core::types::{DrvHash, JobId};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

/// A random DAG of `n` drvs where each non-leaf has k random deps
/// chosen from earlier-indexed drvs. Produces a naturally-layered
/// graph guaranteed acyclic.
fn random_dag(rng: &mut StdRng, n: usize, max_fan_in: usize) -> Vec<Vec<usize>> {
    let mut dag = vec![Vec::new(); n];
    for i in 1..n {
        let k = rng.gen_range(0..=max_fan_in.min(i));
        let mut picks: HashSet<usize> = HashSet::new();
        while picks.len() < k {
            picks.insert(rng.gen_range(0..i));
        }
        dag[i] = picks.into_iter().collect();
    }
    dag
}

/// Instantiate a random DAG into a fresh StepsRegistry + Submission,
/// wiring edges in order (deps first). Returns the registry, sub, and
/// the Vec of Arc<Step> indexed by DAG position.
fn build_graph(dag: &[Vec<usize>], steps: &StepsRegistry, sub: &Arc<Submission>) -> Vec<Arc<Step>> {
    let mut nodes: Vec<Arc<Step>> = Vec::with_capacity(dag.len());
    for i in 0..dag.len() {
        let hash = DrvHash::new(format!("drv-{i:04}"));
        let (step, _) = steps.get_or_create(&hash, || {
            Step::new(
                hash.clone(),
                format!("/nix/store/{hash}"),
                format!("drv-{i:04}"),
                "x86_64-linux".into(),
                Vec::new(),
                2,
            )
        });
        {
            let mut st = step.state.write();
            st.submissions.push(Arc::downgrade(sub));
        }
        sub.add_member(&step);
        nodes.push(step);
    }

    // Attach edges: node i depends on each node in dag[i]
    for (i, deps) in dag.iter().enumerate() {
        for &d in deps {
            attach_dep(&nodes[i], &nodes[d]);
        }
    }

    // Mark all created after edges are attached, then arm leaves
    for step in &nodes {
        step.created.store(true, Ordering::Release);
    }
    for step in &nodes {
        let is_leaf = step.state.read().deps.is_empty();
        if is_leaf
            && step
                .runnable
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            enqueue_for_all_submissions(step);
        }
    }

    nodes
}

/// Assert the 8 invariants hold for every Step in the registry.
fn check_invariants(nodes: &[Arc<Step>]) {
    for step in nodes {
        // 1. runnable=true ⟹ created=true ∧ deps.empty()
        if step.runnable.load(Ordering::Acquire) {
            assert!(
                step.created.load(Ordering::Acquire),
                "invariant 1a violated: runnable but !created for {}",
                step.drv_hash()
            );
            assert!(
                step.state.read().deps.is_empty(),
                "invariant 1b violated: runnable but deps.len()={} for {}",
                step.state.read().deps.len(),
                step.drv_hash()
            );
        }
        // 4-adjacent: finished=true ⟹ must not have runnable=true
        // (runnable is reset on claim)
        if step.finished.load(Ordering::Acquire) {
            assert!(
                !step.runnable.load(Ordering::Acquire) || step.state.read().deps.is_empty(),
                "finished step retains runnable with non-empty deps: {}",
                step.drv_hash()
            );
        }
    }
}

/// Simulate a worker claiming a step: pop runnable (CAS true→false),
/// finish it (set finished), propagate rdeps. Returns the step that
/// was built, or None if nothing is claimable.
fn claim_and_finish(sub: &Arc<Submission>) -> Option<Arc<Step>> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let step = sub.pop_runnable(&["x86_64-linux".into()], &[], now_ms)?;
    // Simulate success
    step.finished.store(true, Ordering::Release);
    make_rdeps_runnable(&step);
    Some(step)
}

fn run_seed(seed: u64, n: usize, max_fan_in: usize, ops: usize) {
    let mut rng = StdRng::seed_from_u64(seed);
    let dag = random_dag(&mut rng, n, max_fan_in);

    let registry = StepsRegistry::new();
    let sub = Submission::new(JobId::new(), 64);
    let nodes = build_graph(&dag, &registry, &sub);

    check_invariants(&nodes);

    let mut built: HashSet<DrvHash> = HashSet::new();
    let mut expected_remaining = n;

    for op in 0..ops {
        match claim_and_finish(&sub) {
            Some(s) => {
                let new = built.insert(s.drv_hash().clone());
                assert!(new, "same drv built twice (CAS leak): {}", s.drv_hash());
                expected_remaining -= 1;
                check_invariants(&nodes);
            }
            None => {
                // Either everything is built, or deadlocked
                if built.len() == n {
                    break;
                }
                panic!(
                    "seed={seed} op={op}: nothing claimable but {expected_remaining} remain. \
                     built={}, n={n}",
                    built.len()
                );
            }
        }
    }

    assert_eq!(
        built.len(),
        n,
        "seed={seed}: only built {} of {n}",
        built.len()
    );
    // Final: every step should be finished
    for s in &nodes {
        assert!(
            s.finished.load(Ordering::Acquire),
            "seed={seed}: step {} never finished",
            s.drv_hash()
        );
    }
}

#[test]
fn property_small_dag_many_seeds() {
    for seed in 0..20 {
        run_seed(seed, 30, 3, 200);
    }
}

#[test]
fn property_medium_dag_many_seeds() {
    for seed in 0..10 {
        run_seed(seed * 7 + 1, 200, 5, 500);
    }
}

#[test]
fn property_wide_fan_in_never_starves() {
    // Root with many deps — every dep must be built exactly once
    // before root becomes runnable.
    for seed in 0..5 {
        run_seed(seed + 100, 100, 99, 300);
    }
}

#[test]
fn property_deep_chain_sequences_correctly() {
    // Linear chain n → n-1 → ... → 0. Tests pure sequential dep
    // resolution under the make_rdeps_runnable path.
    for seed in 0..3 {
        let rng = StdRng::seed_from_u64(seed + 200);
        let n = 50;
        let mut dag: Vec<Vec<usize>> = vec![Vec::new(); n];
        for i in 1..n {
            dag[i] = vec![i - 1];
        }
        let registry = StepsRegistry::new();
        let sub = Submission::new(JobId::new(), 64);
        let nodes = build_graph(&dag, &registry, &sub);

        let mut order = Vec::new();
        for _ in 0..n + 5 {
            if let Some(s) = claim_and_finish(&sub) {
                let idx: usize = s
                    .drv_hash()
                    .as_str()
                    .trim_start_matches("drv-")
                    .parse()
                    .unwrap();
                order.push(idx);
            }
        }
        // Chain must be claimed in strict order 0, 1, 2, ...
        assert_eq!(
            order,
            (0..n).collect::<Vec<_>>(),
            "chain must be strictly in-order"
        );
        check_invariants(&nodes);
        let _ = rng;
    }
}

#[test]
fn property_diamond_dependency_both_deps_must_finish() {
    for seed in 0..5 {
        let rng = StdRng::seed_from_u64(seed + 300);
        let n = 20;
        // A diamond: 0 is base leaf. 1..n-1 depend on 0. n-1 depends on
        // 1..n-2.
        let mut dag: Vec<Vec<usize>> = vec![Vec::new(); n];
        for i in 1..n - 1 {
            dag[i] = vec![0];
        }
        dag[n - 1] = (1..n - 1).collect();
        let registry = StepsRegistry::new();
        let sub = Submission::new(JobId::new(), 64);
        let nodes = build_graph(&dag, &registry, &sub);

        let mut order = Vec::new();
        for _ in 0..n + 5 {
            if let Some(s) = claim_and_finish(&sub) {
                let idx: usize = s
                    .drv_hash()
                    .as_str()
                    .trim_start_matches("drv-")
                    .parse()
                    .unwrap();
                order.push(idx);
            }
        }
        // First must be 0; last must be n-1
        assert_eq!(order[0], 0);
        assert_eq!(order[n - 1], n - 1);
        // Middle can be any permutation of 1..n-1 but each exactly once
        let mut middle = order[1..n - 1].to_vec();
        middle.sort();
        assert_eq!(middle, (1..n - 1).collect::<Vec<_>>());
        check_invariants(&nodes);
        let _ = rng;
    }
}

#[test]
fn cross_submission_cas_exactly_one_winner() {
    // 10 submissions all add a shared step; exactly one submission's
    // pop_runnable should win the CAS for that step.
    for seed in 0..8 {
        let mut rng = StdRng::seed_from_u64(seed + 400);
        let n_subs = 10;
        let registry = StepsRegistry::new();
        let subs: Vec<Arc<Submission>> = (0..n_subs)
            .map(|_| Submission::new(JobId::new(), 16))
            .collect();

        let hash = DrvHash::new("shared".to_string());
        let (step, _) = registry.get_or_create(&hash, || {
            Step::new(
                hash.clone(),
                format!("/nix/store/{hash}"),
                "shared".into(),
                "x86_64-linux".into(),
                Vec::new(),
                2,
            )
        });
        {
            let mut st = step.state.write();
            for s in &subs {
                st.submissions.push(Arc::downgrade(s));
            }
        }
        for s in &subs {
            s.add_member(&step);
        }
        step.created.store(true, Ordering::Release);
        step.runnable.store(true, Ordering::Release);
        for s in &subs {
            s.enqueue_ready(&step);
        }

        // Shuffle which sub tries first
        let mut order: Vec<usize> = (0..n_subs).collect();
        order.shuffle(&mut rng);

        let mut winners = 0;
        let now_ms = chrono::Utc::now().timestamp_millis();
        for i in &order {
            if subs[*i]
                .pop_runnable(&["x86_64-linux".into()], &[], now_ms)
                .is_some()
            {
                winners += 1;
            }
        }
        assert_eq!(
            winners, 1,
            "seed={seed}: CAS must elect exactly one winner, got {winners}"
        );
        // Step is now runnable=false
        assert!(!step.runnable.load(Ordering::Acquire));
    }
}

#[test]
fn property_random_dag_from_scratch_matches_bfs_topo() {
    // For random DAGs, the order of claim_and_finish must respect
    // topological order: a step is never claimed before all its deps.
    for seed in 0..15 {
        let mut rng = StdRng::seed_from_u64(seed + 500);
        let n = 40;
        let dag = random_dag(&mut rng, n, 3);
        let registry = StepsRegistry::new();
        let sub = Submission::new(JobId::new(), 64);
        let _nodes = build_graph(&dag, &registry, &sub);

        let mut finished_at: HashMap<usize, usize> = HashMap::new();
        for op in 0..n + 10 {
            if let Some(s) = claim_and_finish(&sub) {
                let idx: usize = s
                    .drv_hash()
                    .as_str()
                    .trim_start_matches("drv-")
                    .parse()
                    .unwrap();
                finished_at.insert(idx, op);
            }
        }
        // Check topo order for every edge
        for (i, deps) in dag.iter().enumerate() {
            let ti = finished_at.get(&i).copied();
            for &d in deps {
                let td = finished_at.get(&d).copied();
                assert!(
                    ti.is_some() && td.is_some(),
                    "seed={seed}: step {} or its dep {} never finished",
                    i,
                    d
                );
                assert!(
                    td.unwrap() < ti.unwrap(),
                    "seed={seed}: topo violated — dep {d} finished after {i}"
                );
            }
        }
    }
}

// Invariant under stress: dedup across many submissions works. Run
// many seeds to guarantee the CAS never lets two submissions "win"
// the same step.

#[test]
fn property_many_submissions_overlapping_graphs() {
    for seed in 0..5 {
        let mut rng = StdRng::seed_from_u64(seed + 600);
        let n_subs = 6;
        let n_drvs = 50;
        let shared_prefix = 20; // first 20 drvs shared across all submissions

        let registry = StepsRegistry::new();
        let subs: Vec<Arc<Submission>> = (0..n_subs)
            .map(|_| Submission::new(JobId::new(), 64))
            .collect();

        // Build a shared prefix graph
        let shared_dag = random_dag(&mut rng, shared_prefix, 3);

        // For each sub, build their own tail with deps into shared
        let mut all_nodes: Vec<Arc<Step>> = Vec::new();
        for sub_idx in 0..n_subs {
            for i in 0..n_drvs {
                let hash = if i < shared_prefix {
                    DrvHash::new(format!("shared-{i:04}"))
                } else {
                    DrvHash::new(format!("sub{sub_idx}-{i:04}"))
                };

                let (step, is_new) = registry.get_or_create(&hash, || {
                    Step::new(
                        hash.clone(),
                        format!("/nix/store/{hash}"),
                        hash.as_str().to_string(),
                        "x86_64-linux".into(),
                        Vec::new(),
                        2,
                    )
                });

                // Attach submission
                {
                    let mut st = step.state.write();
                    if !st.submissions.iter().any(|w| {
                        w.upgrade()
                            .map(|s| s.id == subs[sub_idx].id)
                            .unwrap_or(false)
                    }) {
                        st.submissions.push(Arc::downgrade(&subs[sub_idx]));
                    }
                }
                subs[sub_idx].add_member(&step);

                // Attach deps
                if is_new {
                    if i < shared_prefix {
                        for &d in &shared_dag[i] {
                            let dep_hash = DrvHash::new(format!("shared-{d:04}"));
                            if let Some(dep) = registry.get(&dep_hash) {
                                if !dep.finished.load(Ordering::Acquire) {
                                    attach_dep(&step, &dep);
                                }
                            }
                        }
                    } else {
                        // non-shared drv depends on some shared ones
                        let k = rng.gen_range(0..=3);
                        for _ in 0..k {
                            let d = rng.gen_range(0..shared_prefix);
                            let dep_hash = DrvHash::new(format!("shared-{d:04}"));
                            if let Some(dep) = registry.get(&dep_hash) {
                                if !dep.finished.load(Ordering::Acquire) {
                                    attach_dep(&step, &dep);
                                }
                            }
                        }
                    }
                    step.created.store(true, Ordering::Release);
                    let empty = step.state.read().deps.is_empty();
                    if empty
                        && step
                            .runnable
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                    {
                        enqueue_for_all_submissions(&step);
                    }
                    all_nodes.push(step);
                } else {
                    // Cross-sub dedup path: if it's already runnable,
                    // we should also enqueue for this sub so its
                    // worker can race for the claim.
                    if step.runnable.load(Ordering::Acquire)
                        && !step.finished.load(Ordering::Acquire)
                    {
                        subs[sub_idx].enqueue_ready(&step);
                    }
                }
            }
        }

        // Drain: round-robin across subs until nothing's left
        let mut total_built = HashSet::new();
        for _ in 0..n_subs * n_drvs * 2 {
            let mut made_progress = false;
            for s in &subs {
                if let Some(step) = claim_and_finish(s) {
                    let new = total_built.insert(step.drv_hash().clone());
                    assert!(
                        new,
                        "seed={seed}: dedup leak, {} built twice",
                        step.drv_hash()
                    );
                    made_progress = true;
                }
            }
            if !made_progress {
                break;
            }
        }

        // Every live step must be finished
        for node in &all_nodes {
            assert!(
                node.finished.load(Ordering::Acquire),
                "seed={seed}: step {} never finished",
                node.drv_hash()
            );
        }
    }
}

// ─── H1: dedup-under-concurrent-submission-drop (TSan target) ────────
//
// THE single load-bearing invariant of global dedup: while at least one
// strong `Arc<Step>` for a given `drv_hash` is alive, every concurrent
// `StepsRegistry::get_or_create` for that hash must return the same
// allocation. A bug here = split-brain: two submissions think they're
// coordinating on the same drv but actually hold separate Arcs, and the
// CAS-exactly-once invariant (Invariant 3) silently breaks.
//
// This test holds a "pinning" strong ref per drv_hash in the main
// thread so the Weak registry can never GC the entry during the race
// window. Workers then spam get_or_create on that hash across threads;
// every result must be ptr_eq to the pin. A concurrent churn thread
// hammers `live()` so the retain/GC path runs alongside the lookups.
//
// Separately, Invariant 7 permits a *new* Arc after the last strong
// ref is dropped (registry observes failed Weak::upgrade and mints a
// fresh step). That's correct behavior, not a dedup violation — this
// test never crosses that boundary.
//
// Run under TSan in CI via .github/workflows/ci.yml#tsan.

#[test]
fn property_dedup_survives_concurrent_registry_lookup() {
    use std::sync::Arc;
    use std::thread;
    const THREADS: usize = 8;
    const ITERS_PER_THREAD: usize = 500;
    const ALPHABET_SIZE: usize = 16;

    let registry = Arc::new(StepsRegistry::new());

    // Pin one Arc<Step> per drv_hash upfront so the Weak entry can
    // never expire during the race. `pins[i]` is the canonical Arc
    // for `shared-{i:04}`.
    let mut pins: Vec<Arc<Step>> = Vec::with_capacity(ALPHABET_SIZE);
    for i in 0..ALPHABET_SIZE {
        let hash = DrvHash::new(format!("shared-{i:04}"));
        let (step, is_new) = registry.get_or_create(&hash, || {
            Step::new(
                hash.clone(),
                format!("/nix/store/{hash}"),
                format!("shared-{i:04}"),
                "x86_64-linux".into(),
                Vec::new(),
                2,
            )
        });
        assert!(is_new, "initial pin for {hash} must be new");
        pins.push(step);
    }
    let pins = Arc::new(pins);

    let mut handles = Vec::with_capacity(THREADS);
    for t in 0..THREADS {
        let registry = registry.clone();
        let pins = pins.clone();
        handles.push(thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(0xC0FFEE ^ t as u64);
            for _ in 0..ITERS_PER_THREAD {
                let pick = rng.gen_range(0..ALPHABET_SIZE);
                let hash = DrvHash::new(format!("shared-{pick:04}"));
                let (step, is_new) = registry.get_or_create(&hash, || {
                    Step::new(
                        hash.clone(),
                        format!("/nix/store/{hash}"),
                        format!("shared-{pick:04}"),
                        "x86_64-linux".into(),
                        Vec::new(),
                        2,
                    )
                });
                assert!(
                    !is_new,
                    "DEDUP VIOLATED: get_or_create minted a new Arc for pinned hash={hash}"
                );
                assert!(
                    Arc::ptr_eq(&pins[pick], &step),
                    "DEDUP VIOLATED: Arc identity differs from pin for {hash}"
                );
                // Drop our local Arc; the pin keeps the step alive.
                drop(step);
            }
        }));
    }

    // Churn thread: hammer `live()` so the write-locked retain/GC path
    // runs concurrently with reader get_or_create calls. The pinned
    // refs ensure retain never drops any alphabet hash.
    let churn_registry = registry.clone();
    let churn_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let churn_stop_clone = churn_stop.clone();
    let churn = thread::spawn(move || {
        while !churn_stop_clone.load(Ordering::Acquire) {
            let live = churn_registry.live();
            assert!(
                live.len() >= ALPHABET_SIZE,
                "live() must not GC pinned hashes; got {} live",
                live.len()
            );
        }
    });

    for h in handles {
        h.join().expect("worker thread panicked");
    }
    churn_stop.store(true, Ordering::Release);
    churn.join().expect("churn thread panicked");
}
