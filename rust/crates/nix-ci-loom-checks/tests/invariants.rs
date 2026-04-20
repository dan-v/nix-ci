//! Loom model-level checkers for a small subset of the 8 dispatcher
//! invariants (canonical numbering in
//! `rust/crates/nix-ci-core/src/dispatch/mod.rs`).
//!
//! # Intended role: executable contract documentation, NOT a gate
//!
//! These are *model* tests — each one reimplements the invariant's
//! state machine in miniature using loom's `sync::*` / `atomic::*`
//! drop-in replacements. Loom exhaustively explores every
//! interleaving of thread operations on the *model* and checks the
//! invariant holds.
//!
//! **Drift warning:** a model test can pass even when the real
//! dispatcher diverges from the modeled logic. The primary
//! real-code concurrency gates remain:
//!
//! * **TSan** (`nix-ci-core/tests/property.rs` under
//!   `-Zsanitizer=thread`) — catches real data races.
//! * **`nix-ci-core/tests/chaos.rs`** — randomized real-code stress.
//! * **`nix-ci-core/tests/sim.rs`** (L2, `--features sim-test`) —
//!   deterministic seed-replayable simulation of the real
//!   dispatcher.
//! * **`nix-ci-core/tests/property.rs`** — property tests on the
//!   real dispatcher types.
//!
//! What the model tests contribute: a *readable, executable
//! specification* of the two subtlest atomic dances in the
//! dispatcher (the CAS-exactly-once on `runnable` and the
//! created-before-runnable barrier). If a future contributor wants to
//! understand what invariants 3 and 4 *mean*, these tests spell it
//! out in under 50 lines apiece.
//!
//! We deliberately kept the set small to limit drift risk — fewer
//! models, fewer chances to diverge.
//!
//! Run with:
//!
//!   cd rust && RUSTFLAGS="--cfg loom" \
//!       cargo test -p nix-ci-loom-checks --release \
//!       -- --test-threads=1

use loom::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use loom::sync::{Arc, RwLock};
use loom::thread;

/// ─── Invariant 3 (CAS-exactly-once on runnable) ───────────────────
///
/// `pop_runnable` uses `compare_exchange(true, false, AcqRel, Acquire)`
/// on `Step::runnable`. When N workers race on a single runnable Step
/// (the multi-submission dedup case — the same shared drv is
/// claimable by every submission that references it), exactly one
/// must win the CAS.
#[test]
fn invariant_3_cas_exactly_once_on_runnable() {
    loom::model(|| {
        let runnable = Arc::new(AtomicBool::new(true));
        let wins = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let runnable = runnable.clone();
            let wins = wins.clone();
            handles.push(thread::spawn(move || {
                if runnable
                    .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    wins.fetch_add(1, Ordering::AcqRel);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            wins.load(Ordering::Acquire),
            1,
            "exactly one worker must win the CAS"
        );
    });
}

/// ─── Invariant 4 (Created-before-runnable) ────────────────────────
///
/// A fresh Step must have `created=true` stored with `Release` BEFORE
/// any worker observes `runnable=true`. The rdep walk's guard is
/// `deps.is_empty() && created.load(Acquire)`. If a worker can
/// observe `runnable=true` without also observing `created=true`,
/// invariant 4 is violated — the step would enter the claim path
/// before ingest finished attaching all deps.
#[test]
fn invariant_4_created_before_runnable() {
    loom::model(|| {
        let created = Arc::new(AtomicBool::new(false));
        let runnable = Arc::new(AtomicBool::new(false));

        let ingester = {
            let c = created.clone();
            thread::spawn(move || {
                c.store(true, Ordering::Release);
            })
        };
        let completer = {
            let c = created.clone();
            let r = runnable.clone();
            thread::spawn(move || {
                if c.load(Ordering::Acquire) {
                    r.store(true, Ordering::Release);
                }
            })
        };
        ingester.join().unwrap();
        completer.join().unwrap();

        if runnable.load(Ordering::Acquire) {
            assert!(
                created.load(Ordering::Acquire),
                "runnable armed without created — invariant 4 violated"
            );
        }
    });
}

/// ─── Reservation-race ──────────────────────────────────────────────
///
/// Not one of the 8 invariants but a production-critical property: the
/// per-job drv cap uses `fetch_add → range-check → fetch_sub rollback`.
/// Two concurrent reservations whose combined total would exceed the
/// cap must both roll back; `reserved_drvs` must never exceed the cap.
#[test]
fn reservation_cap_never_exceeded_under_race() {
    loom::model(|| {
        const CAP: u32 = 10;
        let reserved = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let reserved = reserved.clone();
            handles.push(thread::spawn(move || -> bool {
                let prev = reserved.fetch_add(6, Ordering::AcqRel);
                let new_total = prev.saturating_add(6);
                if new_total > CAP {
                    reserved.fetch_sub(6, Ordering::AcqRel);
                    false
                } else {
                    true
                }
            }));
        }
        let _ = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>();
        assert!(
            reserved.load(Ordering::Acquire) <= CAP,
            "reservation exceeded cap"
        );
    });
}

/// ─── Steps dedup (invariant 1) ─────────────────────────────────────
///
/// `StepsRegistry::get_or_create` is the sole factory: under race,
/// exactly one factory call is made, and all callers receive the same
/// Arc. Modeled as `Option<Arc<u32>>` behind RwLock with fast-path
/// read + slow-path write + re-check.
#[test]
fn invariant_1_steps_dedup_exactly_one_factory_call() {
    loom::model(|| {
        let registry: Arc<RwLock<Option<Arc<u32>>>> = Arc::new(RwLock::new(None));
        let factory_calls = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let registry = registry.clone();
            let factory_calls = factory_calls.clone();
            handles.push(thread::spawn(move || -> Arc<u32> {
                if let Some(x) = registry.read().unwrap().clone() {
                    return x;
                }
                let mut guard = registry.write().unwrap();
                if let Some(x) = guard.clone() {
                    return x;
                }
                factory_calls.fetch_add(1, Ordering::AcqRel);
                let val = Arc::new(42u32);
                *guard = Some(val.clone());
                val
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(
            factory_calls.load(Ordering::Acquire),
            1,
            "exactly one factory invocation — invariant 1 violated"
        );
        for r in &results[1..] {
            assert!(Arc::ptr_eq(&results[0], r), "dedup returned distinct Arcs");
        }
    });
}
