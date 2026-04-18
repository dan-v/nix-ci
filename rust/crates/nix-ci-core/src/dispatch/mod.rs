//! In-memory dispatch engine.
//!
//! # The 8 invariants (correctness contract)
//!
//! References elsewhere in this module say "Invariant N"; the canonical
//! numbering lives here.
//!
//! 1. **Steps dedup**. A given `drv_hash` resolves to at most one live
//!    `Arc<Step>` across the whole process. `StepsRegistry::get_or_create`
//!    is the only way a Step is minted.
//!
//! 2. **`make_rdeps_runnable` monotonicity**. Each call to
//!    `make_rdeps_runnable(step)` removes `step` from every rdep's deps
//!    set. An rdep flips to `runnable=true` only when its deps set
//!    becomes empty AND `created=true`. Never unflips.
//!
//! 3. **CAS-exactly-once on `runnable`**. `pop_runnable` uses a
//!    `compare_exchange(true, false, AcqRel, Acquire)` on `Step::runnable`
//!    so across all submissions sharing the step, exactly one worker
//!    wins the claim.
//!
//! 4. **Created-before-runnable**. A fresh Step must have every dep
//!    edge attached and `created=true` stored with `Release` *before*
//!    it is ever armed `runnable=true`. Rdep-walk depends on this to
//!    avoid arming a still-being-built Step.
//!
//! 5. **No `await` under a lock**. No code path holds a `parking_lot`
//!    guard across an `.await`. All async handlers take snapshots under
//!    the lock, drop it, then await outside.
//!
//! 6. **Consistent `Step::state` lock order**. Callers that need to
//!    mutate two `Step::state`s mutate the "child" (new step) first,
//!    then the "dep". Acyclic DAG + narrow critical sections prevent
//!    deadlock.
//!
//! 7. **Weak-registry ownership**. The `StepsRegistry` holds `Weak<Step>`
//!    only. Strong references live in `Submission::members` and
//!    `Submission::toplevels`. When the last submission referencing a
//!    Step drops, the Step is GC'd; the registry lazily reaps dead
//!    weaks in `live()`.
//!
//! 8. **Strong-Submission-owns-members**. Each `Submission` keeps a
//!    `HashMap<DrvHash, Arc<Step>>` of every step it references (root
//!    or transitive dep), ensuring lifetime spans the submission. Rdep
//!    lists and submission back-edges are `Weak` to avoid cycles.

pub mod claim;
pub mod rdep;
pub mod step;
pub mod steps;
pub mod submission;
pub mod worker_health;

pub use claim::{ActiveClaim, ClaimJobMismatch, Claims};
pub use step::{Step, StepState};
pub use steps::StepsRegistry;
pub use submission::{Submission, Submissions};
pub use worker_health::{RecordOutcome, WorkerHealth, WorkerQuarantinePolicy};

use std::sync::Arc;
use tokio::sync::Notify;

use crate::observability::metrics::Metrics;

/// The in-memory authoritative dispatcher. One instance per coordinator
/// process. Owns the steps registry, submission set, ready sets, and
/// claim tracker. All in-flight scheduling state is memory-only: only
/// the durable envelope (`jobs.result` + `failed_outputs`) reaches
/// Postgres, and only at job-terminal / failure-cache boundaries. On
/// restart the dispatcher starts empty; callers retry cancelled jobs.
#[derive(Clone)]
pub struct Dispatcher {
    pub steps: Arc<StepsRegistry>,
    pub submissions: Arc<Submissions>,
    pub claims: Arc<Claims>,
    pub notify: Arc<Notify>,
    pub metrics: Metrics,
}

impl Dispatcher {
    pub fn new(metrics: Metrics) -> Self {
        Self {
            steps: Arc::new(StepsRegistry::new()),
            submissions: Arc::new(Submissions::new()),
            claims: Arc::new(Claims::new()),
            notify: Arc::new(Notify::new()),
            metrics,
        }
    }

    /// Kick the dispatcher to re-scan ready sets. Wakes ALL currently
    /// waiting claim long-polls, not just one — so a single cancel /
    /// terminal event promptly reaches every worker polling the
    /// affected job, and so a step-ready event isn't silently missed
    /// if a future waiter happened to not be registered yet.
    pub fn wake(&self) {
        self.notify.notify_waiters();
    }

    /// Evict every in-flight claim tied to `job_id` and decrement the
    /// `claims_in_flight` gauge accordingly. Must be called whenever a
    /// submission is removed for ANY reason (cancel, fail, graceful
    /// Done, heartbeat-reaped) — otherwise the orphan claims linger
    /// in the map until their deadline expires (potentially many
    /// minutes later), inflating the gauge and pinning the step's
    /// drv_hash. Returns the count evicted (for tests / metrics).
    pub fn evict_claims_for(&self, job_id: crate::types::JobId) -> u64 {
        let evicted: Vec<_> = self
            .claims
            .all()
            .into_iter()
            .filter(|c| c.job_id == job_id)
            .map(|c| c.claim_id)
            .collect();
        let mut n: u64 = 0;
        for cid in evicted {
            if self.claims.take(cid).is_some() {
                self.metrics.inner.claims_in_flight.dec();
                n += 1;
            }
        }
        n
    }
}
