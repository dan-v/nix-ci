//! `Submission` — one CCI-step's view of the graph. Holds strong refs
//! to the toplevel steps it submitted, plus per-system FIFO ready
//! queues populated at make-runnable time.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Instant;

use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::broadcast;

use crate::types::{DrvHash, JobEvent, JobId};

use super::step::Step;

pub struct Submission {
    pub id: JobId,
    pub created_at: Instant,
    pub sealed: AtomicBool,
    /// Set when the submission status transitions to terminal.
    pub terminal: AtomicBool,
    pub toplevels: RwLock<Vec<Arc<Step>>>,
    pub ready: RwLock<HashMap<String, VecDeque<Weak<Step>>>>,
    /// Strong references to every step this submission owns — root
    /// or transitive dep. Keeps steps alive across handler returns
    /// even when no other strong ref exists; the Steps registry is
    /// weak-only (invariant 8). Dropped when the Submission itself
    /// is removed from the Submissions map.
    pub members: RwLock<HashMap<DrvHash, Arc<Step>>>,
    pub events: broadcast::Sender<JobEvent>,
}

impl Submission {
    pub fn new(id: JobId, event_capacity: usize) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(event_capacity);
        Arc::new(Self {
            id,
            created_at: Instant::now(),
            sealed: AtomicBool::new(false),
            terminal: AtomicBool::new(false),
            toplevels: RwLock::new(Vec::new()),
            ready: RwLock::new(HashMap::new()),
            members: RwLock::new(HashMap::new()),
            events: tx,
        })
    }

    /// Add a step to this submission's membership, holding a strong
    /// reference. Idempotent on duplicate `drv_hash`. Returns true if
    /// newly added.
    pub fn add_member(&self, step: &Arc<Step>) -> bool {
        let mut guard = self.members.write();
        if guard.contains_key(step.drv_hash()) {
            false
        } else {
            guard.insert(step.drv_hash().clone(), step.clone());
            true
        }
    }

    pub fn add_root(&self, step: Arc<Step>) {
        let mut guard = self.toplevels.write();
        if !guard.iter().any(|t| t.drv_hash() == step.drv_hash()) {
            guard.push(step);
        }
    }

    /// Enqueue a runnable step for this submission. Idempotent-ish:
    /// duplicates in the deque are handled at pop time via CAS on
    /// `runnable`, so we don't bother dedup'ing here.
    pub fn enqueue_ready(&self, step: &Arc<Step>) {
        let system = step.system().to_string();
        let mut guard = self.ready.write();
        guard
            .entry(system)
            .or_insert_with(|| VecDeque::with_capacity(16))
            .push_back(Arc::downgrade(step));
    }

    /// Pop a step from this submission's ready queue that matches the
    /// given system + feature set. Atomic CAS on `step.runnable` wins
    /// at most once across all submissions containing this step.
    ///
    /// Returns:
    /// - `Some(step)` on win (step is now claimed; caller must transition)
    /// - `None` if no matching step is ready
    pub fn pop_runnable(
        &self,
        system: &str,
        worker_features: &[String],
        now_ms: i64,
    ) -> Option<Arc<Step>> {
        let mut guard = self.ready.write();
        let queue = guard.get_mut(system)?;
        // Two distinct requeue destinations:
        // - `requeue` (head): feature mismatches — another worker may
        //   have the feature; put the entry back in the same position
        //   so we don't bias against it.
        // - `requeue_tail` (tail): retry-backoff entries whose
        //   next_attempt_at is still in the future; rotating them to
        //   the tail avoids re-popping them on every single claim.
        let mut requeue: Vec<Weak<Step>> = Vec::new();
        let mut requeue_tail: Vec<Weak<Step>> = Vec::new();
        let result = loop {
            let Some(weak) = queue.pop_front() else {
                break None;
            };
            let Some(step) = weak.upgrade() else {
                continue; // GC'd, drop silently
            };
            // Skip if already terminal or not ready
            if step.finished.load(Ordering::Acquire) {
                continue;
            }
            // Features filter. If worker can't do this, re-queue at tail.
            if !features_subset(step.required_features(), worker_features) {
                requeue.push(Arc::downgrade(&step));
                continue;
            }
            // Retry backoff: not yet eligible. Push to the TAIL of the
            // queue instead of the head so we don't spin re-popping
            // the same still-waiting entry on every claim attempt.
            let next_at = step.next_attempt_at.load(Ordering::Acquire);
            if next_at > 0 && next_at > now_ms {
                requeue_tail.push(Arc::downgrade(&step));
                continue;
            }
            // CAS claim
            if step
                .runnable
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break Some(step);
            }
            // CAS lost to another submission; drop this weak, another
            // submission's worker got it.
        };
        // Put back the skipped entries:
        for w in requeue.into_iter().rev() {
            queue.push_front(w);
        }
        for w in requeue_tail {
            queue.push_back(w);
        }
        result
    }

    pub fn publish(&self, event: JobEvent) {
        // Ignore send errors — no subscribers is normal.
        let _ = self.events.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<JobEvent> {
        self.events.subscribe()
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    pub fn seal(&self) {
        self.sealed.store(true, Ordering::Release);
    }

    pub fn mark_terminal(&self) -> bool {
        self.terminal
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal.load(Ordering::Acquire)
    }

    /// Snapshot every Step this submission owns (strong refs).
    pub fn members_snapshot(&self) -> Vec<Arc<Step>> {
        self.members.read().values().cloned().collect()
    }

    /// Compute live counts from the submission's member set. Cheaper
    /// than the db-backed recursive CTE when the submission is live
    /// in memory.
    pub fn live_counts(&self) -> crate::types::JobCounts {
        use crate::types::{DrvState, JobCounts};
        let mut c = JobCounts::default();
        for s in self.members.read().values() {
            c.total += 1;
            match s.observable_state() {
                DrvState::Pending => c.pending += 1,
                DrvState::Building => c.building += 1,
                DrvState::Done => c.done += 1,
                DrvState::Failed => c.failed += 1,
            }
        }
        c
    }

    /// Compute live status from the toplevels. Returns Pending if the
    /// submission hasn't been sealed and has unfinished roots.
    pub fn live_status(&self) -> crate::types::JobStatus {
        use crate::types::JobStatus;
        let tops = self.toplevels.read();
        let all_finished = tops.iter().all(|t| t.finished.load(Ordering::Acquire));
        let any_failed = tops
            .iter()
            .any(|t| t.previous_failure.load(Ordering::Acquire));
        if !all_finished {
            return if self.is_sealed() {
                JobStatus::Building
            } else {
                JobStatus::Pending
            };
        }
        if any_failed {
            JobStatus::Failed
        } else if self.is_sealed() {
            JobStatus::Done
        } else {
            JobStatus::Pending
        }
    }
}

fn features_subset(required: &[String], supported: &[String]) -> bool {
    required.iter().all(|r| supported.iter().any(|s| s == r))
}

pub struct Submissions {
    inner: RwLock<HashMap<JobId, Arc<Submission>>>,
}

impl Default for Submissions {
    fn default() -> Self {
        Self::new()
    }
}

impl Submissions {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    pub fn get(&self, id: JobId) -> Option<Arc<Submission>> {
        self.inner.read().get(&id).cloned()
    }

    pub fn get_or_insert(&self, id: JobId, event_capacity: usize) -> Arc<Submission> {
        if let Some(existing) = self.inner.read().get(&id).cloned() {
            return existing;
        }
        let mut guard = self.inner.write();
        if let Some(existing) = guard.get(&id).cloned() {
            return existing;
        }
        let sub = Submission::new(id, event_capacity);
        guard.insert(id, sub.clone());
        sub
    }

    pub fn remove(&self, id: JobId) -> Option<Arc<Submission>> {
        self.inner.write().remove(&id)
    }

    pub fn all(&self) -> Vec<Arc<Submission>> {
        self.inner.read().values().cloned().collect()
    }
}
