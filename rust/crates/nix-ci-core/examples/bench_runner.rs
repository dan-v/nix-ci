//! Standalone driver for profiling dispatcher hot paths with samply.
//! Not part of `cargo test` / `cargo bench` — run via
//! `cargo run --release --example bench_runner -- <scenario>`
//!
//! Scenarios:
//! - `build_dag_50k`: 5 × 10_000 × FI=2 DAG construction, N iterations.
//! - `detect_cycle_25k`: 5 × 5_000 DAG cycle-check.
//! - `rdep_propagation_25k`: rdep fan-out on 5 × 5_000.
//! - `propagate_failure_25k`: failure cascade on 5 × 5_000.
//! - `drv_parser_500`: ATerm parse with 500 input drvs.

use std::env;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use nix_ci_core::dispatch::rdep::{attach_dep, enqueue_for_all_submissions, make_rdeps_runnable};
use nix_ci_core::dispatch::{Step, StepsRegistry, Submission};
use nix_ci_core::types::{DrvHash, JobId};

fn drv_hash_for(layer: usize, idx: usize) -> DrvHash {
    DrvHash::new(format!("l{layer:02}i{idx:06}-drv.drv"))
}

fn make_step(layer: usize, idx: usize) -> Arc<Step> {
    Step::new(
        drv_hash_for(layer, idx),
        format!("/nix/store/l{layer:02}i{idx:06}-drv.drv"),
        format!("l{layer}i{idx}"),
        "x86_64-linux".into(),
        Vec::new(),
        2,
    )
}

#[derive(Default)]
struct PhaseTimes {
    get_or_create: std::time::Duration,
    add_member: std::time::Duration,
    attach_submission: std::time::Duration,
    attach_dep: std::time::Duration,
    arm_leaves: std::time::Duration,
    add_root: std::time::Duration,
}

static PHASE_TIMES: std::sync::OnceLock<parking_lot::Mutex<PhaseTimes>> =
    std::sync::OnceLock::new();

fn phase_times() -> &'static parking_lot::Mutex<PhaseTimes> {
    PHASE_TIMES.get_or_init(|| parking_lot::Mutex::new(PhaseTimes::default()))
}

fn build_dag_timed(
    registry: &StepsRegistry,
    sub: &Arc<Submission>,
    layers: usize,
    width: usize,
    fan_in: usize,
) -> Vec<Arc<Step>> {
    let mut t = PhaseTimes::default();
    let mut prev: Vec<Arc<Step>> = Vec::with_capacity(width);
    for layer in 0..layers {
        let mut curr: Vec<Arc<Step>> = Vec::with_capacity(width);
        for i in 0..width {
            let hash = drv_hash_for(layer, i);

            let s0 = std::time::Instant::now();
            let (step, _) = registry.get_or_create(&hash, || make_step(layer, i));
            t.get_or_create += s0.elapsed();

            let s0 = std::time::Instant::now();
            sub.add_member(&step);
            t.add_member += s0.elapsed();

            let s0 = std::time::Instant::now();
            let _ = step.state.write().attach_submission(sub);
            t.attach_submission += s0.elapsed();

            if layer > 0 && !prev.is_empty() {
                for k in 0..fan_in {
                    let dep_ix = (i * 31 + k * 7 + layer) % prev.len();
                    let dep = prev[dep_ix].clone();

                    let s0 = std::time::Instant::now();
                    attach_dep(&step, &dep);
                    t.attach_dep += s0.elapsed();

                    let s0 = std::time::Instant::now();
                    sub.add_member(&dep);
                    t.add_member += s0.elapsed();

                    let s0 = std::time::Instant::now();
                    let _ = dep.state.write().attach_submission(sub);
                    t.attach_submission += s0.elapsed();
                }
            }
            if layer == layers - 1 {
                let s0 = std::time::Instant::now();
                sub.add_root(step.clone());
                t.add_root += s0.elapsed();
            }
            curr.push(step);
        }
        prev = curr;
    }
    let s0 = std::time::Instant::now();
    for step in registry.live() {
        if step.state.read().deps.is_empty() {
            step.created.store(true, Ordering::Release);
            if step
                .runnable
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                enqueue_for_all_submissions(&step);
            }
        } else {
            step.created.store(true, Ordering::Release);
        }
    }
    t.arm_leaves += s0.elapsed();

    let mut global = phase_times().lock();
    global.get_or_create += t.get_or_create;
    global.add_member += t.add_member;
    global.attach_submission += t.attach_submission;
    global.attach_dep += t.attach_dep;
    global.arm_leaves += t.arm_leaves;
    global.add_root += t.add_root;

    prev
}

fn build_dag(
    registry: &StepsRegistry,
    sub: &Arc<Submission>,
    layers: usize,
    width: usize,
    fan_in: usize,
) -> Vec<Arc<Step>> {
    let mut prev: Vec<Arc<Step>> = Vec::with_capacity(width);
    for layer in 0..layers {
        let mut curr: Vec<Arc<Step>> = Vec::with_capacity(width);
        for i in 0..width {
            let hash = drv_hash_for(layer, i);
            let (step, _) = registry.get_or_create(&hash, || make_step(layer, i));
            sub.add_member(&step);
            let _ = step.state.write().attach_submission(sub);
            if layer > 0 && !prev.is_empty() {
                for k in 0..fan_in {
                    let dep_ix = (i * 31 + k * 7 + layer) % prev.len();
                    let dep = prev[dep_ix].clone();
                    attach_dep(&step, &dep);
                    sub.add_member(&dep);
                    let _ = dep.state.write().attach_submission(sub);
                }
            }
            if layer == layers - 1 {
                sub.add_root(step.clone());
            }
            curr.push(step);
        }
        prev = curr;
    }
    for step in registry.live() {
        if step.state.read().deps.is_empty() {
            step.created.store(true, Ordering::Release);
            if step
                .runnable
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                enqueue_for_all_submissions(&step);
            }
        } else {
            step.created.store(true, Ordering::Release);
        }
    }
    prev
}

fn scenario_build_dag(iters: usize, layers: usize, width: usize, fi: usize) {
    for _ in 0..iters {
        let registry = StepsRegistry::new();
        let sub = Submission::new(JobId::new(), 256);
        let _ = build_dag(&registry, &sub, layers, width, fi);
        std::hint::black_box(&sub);
    }
}

fn scenario_build_dag_timed(iters: usize, layers: usize, width: usize, fi: usize) {
    for _ in 0..iters {
        let registry = StepsRegistry::new();
        let sub = Submission::new(JobId::new(), 256);
        let _ = build_dag_timed(&registry, &sub, layers, width, fi);
        std::hint::black_box(&sub);
    }
    let t = phase_times().lock();
    let total = t.get_or_create
        + t.add_member
        + t.attach_submission
        + t.attach_dep
        + t.arm_leaves
        + t.add_root;
    let pct = |d: std::time::Duration| 100.0 * d.as_secs_f64() / total.as_secs_f64().max(1e-9);
    eprintln!("phase breakdown (total={total:?}):");
    eprintln!("  get_or_create     {:>8?}  {:5.1}%", t.get_or_create, pct(t.get_or_create));
    eprintln!("  add_member        {:>8?}  {:5.1}%", t.add_member, pct(t.add_member));
    eprintln!("  attach_submission {:>8?}  {:5.1}%", t.attach_submission, pct(t.attach_submission));
    eprintln!("  attach_dep        {:>8?}  {:5.1}%", t.attach_dep, pct(t.attach_dep));
    eprintln!("  arm_leaves        {:>8?}  {:5.1}%", t.arm_leaves, pct(t.arm_leaves));
    eprintln!("  add_root          {:>8?}  {:5.1}%", t.add_root, pct(t.add_root));
}

fn scenario_detect_cycle(iters: usize, layers: usize, width: usize) {
    let registry = StepsRegistry::new();
    let sub = Submission::new(JobId::new(), 256);
    let _ = build_dag(&registry, &sub, layers, width, 2);
    for _ in 0..iters {
        let out = sub.detect_cycle();
        std::hint::black_box(out);
    }
}

fn scenario_rdep_propagation(iters: usize, layers: usize, width: usize) {
    for _ in 0..iters {
        let registry = StepsRegistry::new();
        let sub = Submission::new(JobId::new(), 256);
        let _ = build_dag(&registry, &sub, layers, width, 2);
        let leaves: Vec<Arc<Step>> = (0..width)
            .map(|i| registry.get(&drv_hash_for(0, i)).expect("leaf"))
            .collect();
        for leaf in &leaves {
            leaf.finished.store(true, Ordering::Release);
            make_rdeps_runnable(leaf);
        }
        std::hint::black_box(&sub);
    }
}

fn scenario_propagate_failure(iters: usize, layers: usize, width: usize) {
    for _ in 0..iters {
        let registry = StepsRegistry::new();
        let sub = Submission::new(JobId::new(), 256);
        let _ = build_dag(&registry, &sub, layers, width, 2);
        let root = registry.get(&drv_hash_for(0, 0)).expect("leaf");
        root.previous_failure.store(true, Ordering::Release);
        root.finished.store(true, Ordering::Release);
        let origin = root.drv_hash().clone();
        let n = nix_ci_core::server::complete::propagate_failure_inmem_for_bench(&root, &origin);
        std::hint::black_box(n);
    }
}

fn main() {
    let scenario = env::args().nth(1).unwrap_or_else(|| "build_dag_50k".into());
    let iters: usize = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    eprintln!("scenario={scenario} iters={iters}");
    let start = std::time::Instant::now();
    match scenario.as_str() {
        "build_dag_50k" => scenario_build_dag(iters, 5, 10_000, 2),
        "build_dag_10k" => scenario_build_dag(iters, 5, 2_000, 2),
        "build_dag_50k_timed" => scenario_build_dag_timed(iters, 5, 10_000, 2),
        "build_dag_10k_timed" => scenario_build_dag_timed(iters, 5, 2_000, 2),
        "detect_cycle_25k" => scenario_detect_cycle(iters, 5, 5_000),
        "rdep_propagation_25k" => scenario_rdep_propagation(iters, 5, 5_000),
        "propagate_failure_25k" => scenario_propagate_failure(iters, 5, 5_000),
        other => {
            eprintln!("unknown scenario: {other}");
            std::process::exit(2);
        }
    }
    eprintln!("done in {:?}", start.elapsed());
}
