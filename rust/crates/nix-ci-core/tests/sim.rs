//! L2 deterministic simulation on the **real** dispatcher.
//!
//! # Why
//!
//! This is the TigerBeetle / FoundationDB playbook. The critical claim:
//! **we have never seen a production bug that wasn't first caught in
//! simulation.** To get there, we need:
//!
//! * The *real* dispatcher code running, not a model.
//! * Fully deterministic execution — no tokio, no real clock, no
//!   PRNG entropy beyond the seed. Same seed → same trajectory → same
//!   bug → reproducible in seconds.
//! * A synthetic event generator that exercises ingest / claim /
//!   complete / fail / cancel in adversarial interleavings.
//! * Invariant assertions after every event — not just at quiesce.
//!
//! # What's in scope
//!
//! The dispatcher primitives (`Dispatcher`, `Submission`, `Step`,
//! `Claims`, `StepsRegistry`, `make_rdeps_runnable`, `pop_runnable`)
//! are all synchronous — they don't need tokio. The PG writeback path
//! is NOT exercised here: persistence is the durable-envelope contract,
//! not the dispatcher's correctness surface. The sim replaces the
//! writeback with a no-op (nothing to persist in memory-only tests).
//!
//! # What's deferred
//!
//! The event stream is single-threaded by design — full reproducibility
//! requires serialized mutation. That means this sim catches:
//!
//! * Graph-consistency bugs (rdep propagation holes, missing
//!   terminal writes, drv-state leaks)
//! * Sequential edge cases (ingest-after-seal, complete-after-cancel,
//!   cross-job dedup, etc.)
//! * Invariant breaches that don't require racy interleavings
//!
//! It does NOT catch true data races (TSan / loom cover those). The
//! two are complementary: loom for exhaustive interleavings at the
//! atomic level; sim for exhaustive event orderings at the dispatcher
//! API level.
//!
//! # Running
//!
//!   # Single seed (fast, debug):
//!   cargo test -p nix-ci-core --features sim-test --test sim \
//!       -- --nocapture
//!
//!   # Many seeds (CI soak):
//!   SIM_SEEDS=10000 cargo test -p nix-ci-core --features sim-test \
//!       --test sim --release -- --nocapture --test-threads=1

#![cfg(feature = "sim-test")]

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use nix_ci_core::dispatch::rdep::{attach_dep, enqueue_for_all_submissions, make_rdeps_runnable};
use nix_ci_core::dispatch::{Dispatcher, Step};
use nix_ci_core::observability::metrics::Metrics;
use nix_ci_core::types::{ClaimId, DrvHash, JobId};
use rand::prelude::*;
use rand::rngs::StdRng;

// ─── Event schema ──────────────────────────────────────────────────

/// A deterministic, replayable event applied to the dispatcher. Every
/// pseudo-random choice in the sim is seeded; `Event` is a transcript
/// type that would let us dump a failing schedule as a fixture later
/// (the seed itself is already enough to replay).
#[derive(Debug, Clone)]
enum Event {
    /// Create a submission with `drv_count` drvs arranged in a small DAG.
    CreateAndIngest { job_id: JobId, drv_count: usize },
    /// Seal the submission so terminal writes can happen on the next
    /// ingest or complete.
    Seal { job_id: JobId },
    /// A worker claims a runnable drv (pop + issue). Returns None if
    /// nothing claimable — not an error.
    Claim { job_id: JobId },
    /// Complete a claim as success. Drives the rdep propagation path.
    CompleteSuccess { job_id: JobId, claim_id: ClaimId },
    /// Complete a claim as a deterministic BuildFailure. Drives the
    /// propagation path.
    CompleteFailure { job_id: JobId, claim_id: ClaimId },
    /// Cancel a submission.
    Cancel { job_id: JobId },
}

// ─── Harness ──────────────────────────────────────────────────────

struct Sim {
    dispatcher: Dispatcher,
    /// Unique drv-hash counter so every ingested drv gets a globally
    /// distinct hash (unless we deliberately dedup). Deterministic.
    next_drv: u64,
    /// Submissions created but not yet terminal.
    live_jobs: Vec<JobId>,
    /// Claims still outstanding, keyed by job_id for quick selection.
    issued_claims: Vec<(JobId, ClaimId, DrvHash)>,
    rng: StdRng,
}

impl Sim {
    fn new(seed: u64) -> Self {
        Self {
            dispatcher: Dispatcher::new(Metrics::new()),
            next_drv: 0,
            live_jobs: Vec::new(),
            issued_claims: Vec::new(),
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Pick the next event from the current state. Every pseudo-random
    /// choice is seeded. Returns None when the sim should stop (all
    /// jobs terminal, no queued work).
    fn pick_event(&mut self) -> Option<Event> {
        // Bound the workload so every seed terminates in finite time.
        // A bug that prevents progress would blow past this — caught
        // by the outer deadline, not by silent hang.
        const MAX_JOBS: usize = 30;
        const MAX_DRVS_PER_JOB: usize = 40;

        if self.live_jobs.is_empty() && self.issued_claims.is_empty() && self.next_drv == 0 {
            // Bootstrap: first event is always a CreateAndIngest.
            let job_id = JobId::new();
            self.live_jobs.push(job_id);
            return Some(Event::CreateAndIngest {
                job_id,
                drv_count: self.rng.gen_range(1..=MAX_DRVS_PER_JOB),
            });
        }

        // Decide between: create another job (if under cap), claim,
        // complete an existing claim, seal, cancel.
        let roll = self.rng.gen_range(0..100);
        let under_job_cap = self.live_jobs.len() < MAX_JOBS;
        match roll {
            // 15%: create a new job if we haven't hit the cap; otherwise
            // fall through.
            0..=14 if under_job_cap => {
                let job_id = JobId::new();
                self.live_jobs.push(job_id);
                Some(Event::CreateAndIngest {
                    job_id,
                    drv_count: self.rng.gen_range(1..=MAX_DRVS_PER_JOB),
                })
            }
            // 20%: seal an unsealed live job.
            15..=34 => {
                let unsealed: Vec<JobId> = self
                    .live_jobs
                    .iter()
                    .filter(|id| {
                        self.dispatcher
                            .submissions
                            .get(**id)
                            .map(|s| !s.is_sealed())
                            .unwrap_or(false)
                    })
                    .copied()
                    .collect();
                if unsealed.is_empty() {
                    self.pick_event()
                } else {
                    Some(Event::Seal {
                        job_id: *unsealed.choose(&mut self.rng).unwrap(),
                    })
                }
            }
            // 3%: cancel a random live job (rare but exercises the path).
            35..=37 => {
                if self.live_jobs.is_empty() {
                    self.pick_event()
                } else {
                    let job_id = *self.live_jobs.choose(&mut self.rng).unwrap();
                    Some(Event::Cancel { job_id })
                }
            }
            // 25%: complete an outstanding claim (success-weighted 4:1).
            38..=62 => {
                if self.issued_claims.is_empty() {
                    self.pick_event()
                } else {
                    let idx = self.rng.gen_range(0..self.issued_claims.len());
                    let (job_id, claim_id, _) = self.issued_claims[idx];
                    if self.rng.gen_bool(0.8) {
                        Some(Event::CompleteSuccess { job_id, claim_id })
                    } else {
                        Some(Event::CompleteFailure { job_id, claim_id })
                    }
                }
            }
            // Remaining 37%: claim.
            _ => {
                if self.live_jobs.is_empty() {
                    self.pick_event()
                } else {
                    let job_id = *self.live_jobs.choose(&mut self.rng).unwrap();
                    Some(Event::Claim { job_id })
                }
            }
        }
    }

    /// Apply one event, asserting invariants after. Panics on
    /// violation — the panic carries the seed in its trail for replay.
    fn apply(&mut self, ev: Event) {
        match ev {
            Event::CreateAndIngest { job_id, drv_count } => {
                let sub = self
                    .dispatcher
                    .submissions
                    .get_or_insert_with_options(job_id, 64, 0, None, None);
                // A tiny DAG: first drv has no deps, each subsequent
                // drv depends on 0-2 random prior drvs in this batch.
                let mut drvs: Vec<Arc<Step>> = Vec::with_capacity(drv_count);
                for _ in 0..drv_count {
                    let hash = DrvHash::new(format!("sim-{:08x}-x.drv", self.next_drv));
                    self.next_drv += 1;
                    let system = "x86_64-linux".to_string();
                    let (step, _is_new) = self.dispatcher.steps.get_or_create(&hash, || {
                        Step::new(
                            hash.clone(),
                            format!("/nix/store/{hash}"),
                            "sim".into(),
                            system.clone(),
                            Vec::new(),
                            2,
                        )
                    });
                    // Attach 0-2 deps chosen from previously-ingested drvs.
                    let n_deps = if drvs.is_empty() {
                        0
                    } else {
                        self.rng.gen_range(0..=drvs.len().min(2))
                    };
                    let mut chosen: HashSet<DrvHash> = HashSet::new();
                    for _ in 0..n_deps {
                        let dep_idx = self.rng.gen_range(0..drvs.len());
                        let dep = &drvs[dep_idx];
                        if chosen.insert(dep.drv_hash().clone()) {
                            attach_dep(&step, dep);
                        }
                    }
                    sub.add_member(&step);
                    // Every drv is a toplevel in the sim — simpler model
                    // exercise; corresponds to "submit-everything" CCI
                    // flows.
                    sub.add_root(step.clone());
                    drvs.push(step);
                }
                // Mark everything created and arm leaves. This mimics
                // the real ingest's Phase 3 (arm_if_leaf).
                for s in &drvs {
                    s.created.store(true, Ordering::Release);
                }
                for s in &drvs {
                    if s.state.read().deps.is_empty()
                        && !s.finished.load(Ordering::Acquire)
                        && s.runnable
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                    {
                        enqueue_for_all_submissions(s);
                    }
                }
            }
            Event::Seal { job_id } => {
                if let Some(sub) = self.dispatcher.submissions.get(job_id) {
                    sub.seal();
                }
            }
            Event::Claim { job_id } => {
                let Some(sub) = self.dispatcher.submissions.get(job_id) else {
                    return; // job already terminal
                };
                let systems = vec!["x86_64-linux".to_string()];
                if let Some(step) = sub.pop_runnable(&systems, &[], 0) {
                    let claim_id = ClaimId::new();
                    sub.active_claims.fetch_add(1, Ordering::AcqRel);
                    step.tries.fetch_add(1, Ordering::AcqRel);
                    self.dispatcher.claims.insert(Arc::new(
                        nix_ci_core::dispatch::claim::ActiveClaim {
                            claim_id,
                            job_id,
                            drv_hash: step.drv_hash().clone(),
                            attempt: step.tries.load(Ordering::Acquire),
                            deadline: parking_lot::Mutex::new(tokio::time::Instant::now()),
                            deadline_window: std::time::Duration::from_secs(60),
                            started_at: tokio::time::Instant::now(),
                            started_at_wall: chrono::Utc::now(),
                            worker_id: None,
                            hard_deadline: None,
                        },
                    ));
                    self.issued_claims
                        .push((job_id, claim_id, step.drv_hash().clone()));
                }
            }
            Event::CompleteSuccess { job_id, claim_id } => {
                let Ok(claim) = self.dispatcher.claims.take_for_job(claim_id, job_id) else {
                    return;
                };
                self.issued_claims.retain(|(_, cid, _)| *cid != claim_id);
                if let Some(step) = self.dispatcher.steps.get(&claim.drv_hash) {
                    if !step.finished.load(Ordering::Acquire) {
                        step.finished.store(true, Ordering::Release);
                        make_rdeps_runnable(&step);
                    }
                }
                if let Some(sub) = self.dispatcher.submissions.get(job_id) {
                    sub.decrement_active_claim();
                    // Terminal check: every toplevel finished? Mark
                    // terminal + remove.
                    if sub.is_sealed() {
                        let tops = sub.toplevels.read().snapshot();
                        let all_done = tops.iter().all(|t| t.finished.load(Ordering::Acquire));
                        if all_done && sub.mark_terminal() {
                            self.dispatcher.submissions.remove(job_id);
                            self.dispatcher.evict_claims_for(job_id);
                            self.live_jobs.retain(|id| *id != job_id);
                            self.issued_claims.retain(|(id, _, _)| *id != job_id);
                        }
                    }
                }
            }
            Event::CompleteFailure { job_id, claim_id } => {
                let Ok(claim) = self.dispatcher.claims.take_for_job(claim_id, job_id) else {
                    return;
                };
                self.issued_claims.retain(|(_, cid, _)| *cid != claim_id);
                if let Some(step) = self.dispatcher.steps.get(&claim.drv_hash) {
                    if !step.finished.load(Ordering::Acquire) {
                        step.previous_failure.store(true, Ordering::Release);
                        step.finished.store(true, Ordering::Release);
                        nix_ci_core::server::complete::propagate_failure_inmem_for_bench(
                            &step,
                            step.drv_hash(),
                        );
                    }
                }
                if let Some(sub) = self.dispatcher.submissions.get(job_id) {
                    sub.decrement_active_claim();
                    if sub.is_sealed() {
                        let tops = sub.toplevels.read().snapshot();
                        let all_done = tops.iter().all(|t| t.finished.load(Ordering::Acquire));
                        if all_done && sub.mark_terminal() {
                            self.dispatcher.submissions.remove(job_id);
                            self.dispatcher.evict_claims_for(job_id);
                            self.live_jobs.retain(|id| *id != job_id);
                            self.issued_claims.retain(|(id, _, _)| *id != job_id);
                        }
                    }
                }
            }
            Event::Cancel { job_id } => {
                // Cancel = submission.remove + evict claims. Mirrors
                // the server's cancel path modulo PG writeback.
                self.dispatcher.submissions.remove(job_id);
                self.dispatcher.evict_claims_for(job_id);
                self.live_jobs.retain(|id| *id != job_id);
                self.issued_claims.retain(|(id, _, _)| *id != job_id);
            }
        }
        self.check_invariants();
    }

    /// Global invariants that must hold after every event.
    fn check_invariants(&self) {
        // I. Claims-map size equals the tracked issued_claims.
        //    Drift here would mean `take` / `evict_claims_for` leaked
        //    entries or the sim's tracking is wrong.
        assert_eq!(
            self.dispatcher.claims.len(),
            self.issued_claims.len(),
            "claims map drift: dispatcher has {} but sim tracks {}",
            self.dispatcher.claims.len(),
            self.issued_claims.len(),
        );

        // II. No step is simultaneously runnable and finished.
        //     If this fires, either pop_runnable returned a finished
        //     step or a CAS on runnable raced with a finished-store.
        for step in self.dispatcher.steps.live() {
            let finished = step.finished.load(Ordering::Acquire);
            let runnable = step.runnable.load(Ordering::Acquire);
            assert!(
                !(finished && runnable),
                "step {} is both finished and runnable",
                step.drv_hash()
            );
        }

        // III. Every claim in the map points to a step that's still in
        //     the registry (drv hash resolvable).
        for claim in self.dispatcher.claims.all() {
            assert!(
                self.dispatcher.steps.get(&claim.drv_hash).is_some(),
                "claim {} references missing step {}",
                claim.claim_id,
                claim.drv_hash
            );
        }

        // IV. For every live submission, active_claims matches the
        //     subset of issued_claims tied to that submission.
        for sub in self.dispatcher.submissions.all() {
            let expected: u32 = self
                .issued_claims
                .iter()
                .filter(|(id, _, _)| *id == sub.id)
                .count() as u32;
            let actual = sub.active_claims.load(Ordering::Acquire);
            assert_eq!(
                actual, expected,
                "submission {} active_claims drift: atomic={} tracked={}",
                sub.id, actual, expected
            );
        }
    }
}

/// Drive a single seeded sim. Returns the event count executed — a
/// useful proxy for "did the sim actually run anything" in
/// regression hunts.
fn run_sim_for_seed(seed: u64, max_events: u64) -> u64 {
    let mut sim = Sim::new(seed);
    let mut n = 0u64;
    while n < max_events {
        let Some(ev) = sim.pick_event() else {
            break;
        };
        sim.apply(ev);
        n += 1;
        if sim.live_jobs.is_empty() && sim.issued_claims.is_empty() {
            // Let the generator spin up new work by feeding one more
            // CreateAndIngest; pick_event handles the bootstrap.
            if n >= max_events / 2 {
                // Past the half-budget mark, accept quiesce as "done."
                break;
            }
        }
    }
    n
}

#[test]
fn sim_single_seed_terminates_with_invariants_held() {
    // Any hard-coded seed that exercises a reasonable chunk of the
    // event space. The assertion is implicit in `check_invariants`
    // running after every event — a violation would panic.
    let n = run_sim_for_seed(0x0baadf00d, 5_000);
    eprintln!("SIM: single-seed ran {n} events without invariant violation");
    assert!(n > 0);
}

#[test]
fn sim_multiple_seeds_all_green() {
    // SIM_SEEDS env var lets CI bump the count. Default 200 = sub-
    // second wall clock; bump to 10K+ for nightly.
    let n_seeds: u64 = std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let max_events: u64 = std::env::var("SIM_MAX_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000);
    let mut total_events: u64 = 0;
    for seed in 0..n_seeds {
        let n = run_sim_for_seed(seed, max_events);
        total_events += n;
    }
    eprintln!("SIM: {n_seeds} seeds, {total_events} total events, invariants held throughout");
    assert!(total_events > 0);
}
