//! In-memory dispatch engine. The eight invariants in the V2 design
//! document are the correctness contract for everything in this module.

pub mod claim;
pub mod rdep;
pub mod step;
pub mod steps;
pub mod submission;

pub use claim::{ActiveClaim, Claims};
pub use step::{Step, StepState};
pub use steps::{CreateOutcome, StepsRegistry};
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

    /// Kick the dispatcher to re-scan ready sets (idempotent).
    pub fn wake(&self) {
        self.notify.notify_one();
    }
}
