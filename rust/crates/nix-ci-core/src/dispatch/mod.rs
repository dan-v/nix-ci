//! In-memory dispatch engine. The eight invariants documented across
//! this module (steps dedup, make_rdeps_runnable monotonicity, CAS-
//! exactly-once on runnable, created-before-runnable, no-await-under-
//! lock, consistent lock ordering, weak-registry ownership, strong-
//! Submission-owns-members) are the correctness contract.

pub mod claim;
pub mod rdep;
pub mod step;
pub mod steps;
pub mod submission;

pub use claim::{ActiveClaim, Claims};
pub use step::{Step, StepState};
pub use steps::StepsRegistry;
pub use submission::{Submission, Submissions};

use std::sync::Arc;
use tokio::sync::Notify;

use crate::observability::metrics::Metrics;

/// The in-memory authoritative dispatcher. One instance per coordinator
/// process. Owns the steps registry, submission set, ready sets, and
/// claim tracker. Persists writes through to Postgres at every state
/// transition.
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
}
