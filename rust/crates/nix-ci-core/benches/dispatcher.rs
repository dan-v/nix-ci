//! Criterion benches for the pure in-memory dispatcher paths.
//!
//! No HTTP, no Postgres, no tokio runtime — just the raw graph work
//! that has to stay fast under nixpkgs-scale submissions.
//!
//! Scenarios:
//! - `ingest/build_dag`: mint Steps + wire edges + arm leaves. Scaled
//!   at 10K and 50K drvs, fan-in=2 and fan-in=4, across a 5-layer DAG.
//! - `claim/pop_runnable`: pop from a fully-runnable ready queue.
//! - `claim/fleet_scan`: `sorted_by_created_at` + scan N submissions.
//! - `complete/rdep_propagation`: make_rdeps_runnable waterfall.
//! - `complete/propagate_failure`: failure cascade across a layered DAG.
//! - `complete/compute_used_by_attrs`: per-failure BFS with attr lookup.
//! - `seal/detect_cycle`: DFS cycle-check on a pure DAG.
//! - `parse/drv_parser`: ATerm parse of a mid-size .drv.
//! - `attach/many_submissions_share_step`: stress the per-Step
//!   submissions Vec<Weak> scan.

use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use nix_ci_core::dispatch::rdep::{attach_dep, enqueue_for_all_submissions, make_rdeps_runnable};
use nix_ci_core::dispatch::{Step, StepsRegistry, Submission, Submissions};
use nix_ci_core::types::{DrvHash, JobId};

// ─── Helpers ────────────────────────────────────────────────────────

fn drv_path(layer: usize, idx: usize) -> String {
    // Match the shape of the real load test so hash lengths + prefixes
    // are representative.
    format!("/nix/store/l{layer:02}i{idx:06}-drv.drv")
}

fn drv_hash_for(layer: usize, idx: usize) -> DrvHash {
    DrvHash::new(format!("l{layer:02}i{idx:06}-drv.drv"))
}

fn make_step(layer: usize, idx: usize) -> Arc<Step> {
    Step::new(
        drv_hash_for(layer, idx),
        drv_path(layer, idx),
        format!("l{layer}i{idx}"),
        "x86_64-linux".into(),
        Vec::new(),
        2,
    )
}

/// Build a `layers × width` fan-in-k DAG directly against the dispatcher
/// primitives. Returns the registry, the submission, and the toplevels
/// so callers can inspect the final shape for their own measurement.
/// Deterministic (no RNG) so bench variance stays low.
fn build_dag(
    registry: &StepsRegistry,
    sub: &Arc<Submission>,
    layers: usize,
    width: usize,
    fan_in: usize,
) -> Vec<Arc<Step>> {
    let mut prev_layer: Vec<Arc<Step>> = Vec::with_capacity(width);

    for layer in 0..layers {
        let mut curr_layer: Vec<Arc<Step>> = Vec::with_capacity(width);
        for i in 0..width {
            let hash = drv_hash_for(layer, i);
            let (step, _is_new) = registry.get_or_create(&hash, || make_step(layer, i));
            // Phase 2a: membership
            sub.add_member(&step);
            let _ = step.state.write().attach_submission(sub);
            // Wire FAN_IN edges to the previous layer.
            if layer > 0 && !prev_layer.is_empty() {
                for k in 0..fan_in {
                    let dep_ix = (i * 31 + k * 7 + layer) % prev_layer.len();
                    let dep = prev_layer[dep_ix].clone();
                    attach_dep(&step, &dep);
                    sub.add_member(&dep);
                    let _ = dep.state.write().attach_submission(sub);
                }
            }
            // Top layer members are roots.
            if layer == layers - 1 {
                sub.add_root(step.clone());
            }
            curr_layer.push(step);
        }
        prev_layer = curr_layer;
    }

    // Phase 3: arm leaves. Layer 0 has no deps; flip created=true and
    // set runnable.
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
    prev_layer
}

// ─── Ingest benches ─────────────────────────────────────────────────

fn bench_build_dag(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest/build_dag");
    // Small, medium, large. Smaller sizes keep the bench wall-clock
    // reasonable while still showing the scaling shape.
    for &(layers, width, fan_in) in &[(5usize, 200usize, 2usize), (5, 2_000, 2), (5, 10_000, 2)] {
        let total = layers * width;
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{layers}x{width}xFI{fan_in}")),
            &(layers, width, fan_in),
            |b, &(l, w, f)| {
                b.iter(|| {
                    let registry = StepsRegistry::new();
                    let sub = Submission::new(JobId::new(), 256);
                    let _ = build_dag(&registry, &sub, l, w, f);
                    black_box(&sub);
                });
            },
        );
    }
    group.finish();
}

// ─── Claim benches ──────────────────────────────────────────────────

fn bench_pop_runnable(c: &mut Criterion) {
    let mut group = c.benchmark_group("claim/pop_runnable");
    for &queue_size in &[100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(queue_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(queue_size),
            &queue_size,
            |b, &n| {
                b.iter_batched(
                    || {
                        // Setup: a submission with N runnable steps.
                        let sub = Submission::new(JobId::new(), 256);
                        for i in 0..n {
                            let s = make_step(0, i);
                            s.created.store(true, Ordering::Release);
                            s.runnable.store(true, Ordering::Release);
                            sub.add_member(&s);
                            sub.enqueue_ready(&s);
                        }
                        sub
                    },
                    |sub| {
                        // Drain the queue via N pop_runnable calls.
                        let systems = vec!["x86_64-linux".to_string()];
                        let features: Vec<String> = Vec::new();
                        let mut popped = 0;
                        while let Some(step) = sub.pop_runnable(&systems, &features, 0) {
                            popped += 1;
                            black_box(step);
                        }
                        black_box(popped);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_fleet_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("claim/fleet_scan");
    // Shape: M submissions, each holding one ready step. The fleet
    // claim walks them in priority / FIFO order and returns the first
    // claimable. Measures "raw scan cost" assuming every submission is
    // skippable except the last — worst case for the scan.
    for &n_subs in &[10usize, 100, 1_000] {
        group.throughput(Throughput::Elements(n_subs as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(n_subs),
            &n_subs,
            |b, &n| {
                let subs = Submissions::new();
                let mut jobs: Vec<JobId> = Vec::with_capacity(n);
                for i in 0..n {
                    let id = JobId::new();
                    let sub = subs.get_or_insert(id, 256);
                    jobs.push(id);
                    // Put one ready step in each submission EXCEPT we
                    // arm only the LAST one. The prior subs have empty
                    // ready queues; the scan walks past them.
                    if i + 1 == n {
                        let s = make_step(0, i);
                        s.created.store(true, Ordering::Release);
                        s.runnable.store(true, Ordering::Release);
                        sub.add_member(&s);
                        sub.enqueue_ready(&s);
                    }
                }
                b.iter(|| {
                    // Sort + scan — same op the fleet claim does.
                    let sorted = subs.sorted_by_created_at();
                    let systems = vec!["x86_64-linux".to_string()];
                    let features: Vec<String> = Vec::new();
                    let mut hit = None;
                    for sub in &sorted {
                        if let Some(step) = sub.pop_runnable(&systems, &features, 0) {
                            hit = Some(step);
                            break;
                        }
                    }
                    black_box(hit);
                });
                // Re-arm the last sub's ready for the next iteration
                // since pop consumed it. (Outside b.iter so it doesn't
                // pollute measurement.)
                if let Some(id) = jobs.last() {
                    if let Some(sub) = subs.get(*id) {
                        for s in sub.members.read().values() {
                            s.runnable.store(true, Ordering::Release);
                            sub.enqueue_ready(s);
                        }
                    }
                }
            },
        );
    }
    group.finish();
}

// ─── Complete / propagation benches ──────────────────────────────────

fn bench_rdep_propagation(c: &mut Criterion) {
    let mut group = c.benchmark_group("complete/rdep_propagation");
    // Shape: full layered DAG of N layers × W wide. Complete every
    // leaf; measure how long it takes for the propagation wave to
    // arm every non-leaf.
    for &(layers, width) in &[(5usize, 200usize), (5, 1_000), (5, 5_000)] {
        let total = layers * width;
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{layers}x{width}")),
            &(layers, width),
            |b, &(l, w)| {
                b.iter_batched(
                    || {
                        let registry = StepsRegistry::new();
                        let sub = Submission::new(JobId::new(), 256);
                        let _ = build_dag(&registry, &sub, l, w, 2);
                        // Collect leaves (layer 0).
                        let leaves: Vec<Arc<Step>> = (0..w)
                            .map(|i| registry.get(&drv_hash_for(0, i)).expect("leaf"))
                            .collect();
                        (registry, sub, leaves)
                    },
                    |(_reg, _sub, leaves)| {
                        for leaf in &leaves {
                            leaf.finished.store(true, Ordering::Release);
                            make_rdeps_runnable(leaf);
                        }
                        black_box(leaves.len());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_propagate_failure(c: &mut Criterion) {
    let mut group = c.benchmark_group("complete/propagate_failure");
    for &(layers, width) in &[(5usize, 200usize), (5, 1_000), (5, 5_000)] {
        let total = layers * width;
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{layers}x{width}")),
            &(layers, width),
            |b, &(l, w)| {
                b.iter_batched(
                    || {
                        let registry = StepsRegistry::new();
                        let sub = Submission::new(JobId::new(), 256);
                        let _ = build_dag(&registry, &sub, l, w, 2);
                        // Fail one leaf; propagate to everything above.
                        let root = registry.get(&drv_hash_for(0, 0)).expect("leaf");
                        root.previous_failure.store(true, Ordering::Release);
                        root.finished.store(true, Ordering::Release);
                        (registry, sub, root)
                    },
                    |(_reg, _sub, root)| {
                        let origin = root.drv_hash().clone();
                        let n = nix_ci_core::server::complete::propagate_failure_inmem_for_bench(
                            &root, &origin,
                        );
                        black_box(n);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ─── Cycle detection ────────────────────────────────────────────────

fn bench_detect_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("seal/detect_cycle");
    // Pure DAG, no cycle — the common case at seal time. We measure
    // the O(V+E) walk over a representative graph.
    for &(layers, width) in &[(5usize, 200usize), (5, 1_000), (5, 5_000)] {
        let total = layers * width;
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{layers}x{width}")),
            &(layers, width),
            |b, &(l, w)| {
                let registry = StepsRegistry::new();
                let sub = Submission::new(JobId::new(), 256);
                let _ = build_dag(&registry, &sub, l, w, 2);
                b.iter(|| {
                    let out = sub.detect_cycle();
                    black_box(out);
                });
            },
        );
    }
    group.finish();
}

// ─── Attach-submission scan ─────────────────────────────────────────

fn bench_attach_step_many_submissions(c: &mut Criterion) {
    let mut group = c.benchmark_group("attach/step_shared_across_submissions");
    // Stress the linear scan inside StepState::attach_submission.
    // Shape: ONE step, M distinct submissions each calling
    // attach_submission in turn. On the Nth call the scan is O(N-1).
    for &n_subs in &[10usize, 100, 1_000] {
        group.throughput(Throughput::Elements(n_subs as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(n_subs),
            &n_subs,
            |b, &n| {
                b.iter_batched(
                    || {
                        let step = make_step(0, 0);
                        let subs: Vec<Arc<Submission>> =
                            (0..n).map(|_| Submission::new(JobId::new(), 8)).collect();
                        (step, subs)
                    },
                    |(step, subs)| {
                        for sub in &subs {
                            let _ = step.state.write().attach_submission(sub);
                        }
                        black_box(step);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ─── Drv parser ─────────────────────────────────────────────────────

fn make_drv_text(n_inputs: usize) -> String {
    let mut s = String::from("Derive([");
    s.push_str(r#"("out","/nix/store/aaaa-hello-1.0","",""),("dev","/nix/store/bbbb-hello-1.0-dev","","")"#);
    s.push_str("],[");
    for i in 0..n_inputs {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            r#"("/nix/store/{:040x}-dep-{i}.drv",["out"])"#,
            (i * 31337) as u128
        ));
    }
    s.push_str("],[");
    // input sources (skipped)
    for i in 0..4 {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(r#""/nix/store/src-{i}""#));
    }
    s.push_str(r#"],"x86_64-linux","/nix/store/bbbb-bash/bin/bash",[],[("name","hello-1.0"),("system","x86_64-linux"),("version","1.0")])"#);
    s
}

fn bench_drv_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse/drv_parser");
    for &n_inputs in &[10usize, 100, 500] {
        let text = make_drv_text(n_inputs);
        let bytes = text.as_bytes();
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(n_inputs),
            &bytes.to_vec(),
            |b, input| {
                b.iter(|| {
                    let parsed =
                        nix_ci_core::runner::drv_parser::parse(black_box(input.as_slice())).unwrap();
                    black_box(parsed);
                });
            },
        );
    }
    group.finish();
}

// ─── compute_used_by_attrs ──────────────────────────────────────────

fn bench_compute_used_by_attrs(c: &mut Criterion) {
    let mut group = c.benchmark_group("complete/compute_used_by_attrs");
    for &(layers, width) in &[(5usize, 200usize), (5, 1_000), (5, 5_000)] {
        let total = layers * width;
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{layers}x{width}")),
            &(layers, width),
            |b, &(l, w)| {
                let registry = StepsRegistry::new();
                let sub = Submission::new(JobId::new(), 256);
                let _ = build_dag(&registry, &sub, l, w, 2);
                // Put attribution strings on every root so the lookup
                // path exercises the HashMap get.
                {
                    let mut r = sub.root_attrs.write();
                    for i in 0..w {
                        let h = drv_hash_for(l - 1, i);
                        r.insert(h, format!("packages.x86_64-linux.pkg{i}"));
                    }
                }
                // Pick the leaf at (0, 0): it transitively feeds every
                // root through the fan-in graph. Worst case.
                let leaf = registry.get(&drv_hash_for(0, 0)).expect("leaf");
                b.iter(|| {
                    let attrs =
                        nix_ci_core::server::complete::compute_used_by_attrs_for_bench(&sub, &leaf);
                    black_box(attrs);
                });
            },
        );
    }
    group.finish();
}

// ─── drv_hash_from_path hot-function bench ──────────────────────────

fn bench_drv_hash_from_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest/drv_hash_from_path");
    // Typical nix store path shape.
    let path = "/nix/store/fgh4m2v9l0p3qrstu8abcdefghij0k1l-hello-2.12.1.drv";
    group.throughput(Throughput::Bytes(path.len() as u64));
    group.bench_function("typical_path", |b| {
        b.iter(|| {
            let h = nix_ci_core::types::drv_hash_from_path(black_box(path));
            black_box(h);
        });
    });
    group.finish();
}

// ─── DrvHash cloning (alloc pressure) ───────────────────────────────

fn bench_drvhash_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("alloc/drvhash_clone");
    let h = DrvHash::new("fgh4m2v9l0p3qrstu8abcdefghij0k1l-hello-2.12.1.drv".to_string());
    group.throughput(Throughput::Elements(1));
    group.bench_function("clone_once", |b| {
        b.iter(|| {
            let c = black_box(&h).clone();
            black_box(c);
        });
    });
    group.finish();
}

// ─── Deduped ingest — cross-batch overlap ──────────────────────────

fn bench_ingest_dedup_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest/dedup_heavy");
    // Second submission ingests the SAME DAG. Every step is a dedup
    // hit. The get_or_create + attach_submission paths dominate. This
    // is how concurrent CCI jobs look: overlap is ~90% on nixpkgs.
    for &(layers, width) in &[(5usize, 1_000usize), (5, 5_000)] {
        let total = layers * width;
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{layers}x{width}")),
            &(layers, width),
            |b, &(l, w)| {
                let registry = Arc::new(StepsRegistry::new());
                // Prime the registry with submission A.
                let sub_a = Submission::new(JobId::new(), 256);
                let _ = build_dag(&registry, &sub_a, l, w, 2);
                // Benchmark: submission B ingests the same DAG. Every
                // step is a dedup hit so we isolate the attach /
                // get_or_create / enqueue cost from raw minting.
                b.iter_batched(
                    || Submission::new(JobId::new(), 256),
                    |sub_b| {
                        let _ = build_dag(&registry, &sub_b, l, w, 2);
                        black_box(sub_b);
                    },
                    criterion::BatchSize::SmallInput,
                );
                // Retain registry so it doesn't get deallocated between
                // iterations (would change shape). Unused-suppression
                // via black_box.
                black_box(&sub_a);
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_drv_hash_from_path,
    bench_drvhash_clone,
    bench_drv_parser,
    bench_build_dag,
    bench_ingest_dedup_heavy,
    bench_pop_runnable,
    bench_fleet_scan,
    bench_attach_step_many_submissions,
    bench_rdep_propagation,
    bench_propagate_failure,
    bench_compute_used_by_attrs,
    bench_detect_cycle,
);
criterion_main!(benches);
