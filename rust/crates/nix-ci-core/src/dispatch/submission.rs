//! `Submission` — one CCI-step's view of the graph. Holds strong refs
//! to the toplevel steps it submitted, plus per-system FIFO ready
//! queues populated at make-runnable time.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Instant;

use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tokio::sync::broadcast;

use crate::types::{DrvFailure, DrvHash, JobEvent, JobId};

use super::step::Step;

pub struct Submission {
    pub id: JobId,
    /// In-memory creation timestamp. Used by the fleet `claim_any`
    /// endpoint to drain jobs in FIFO order. NOT persisted; restarts
    /// reset to "now" which is acceptable since restart wipes
    /// non-terminal submissions anyway.
    pub created_at: Instant,
    pub sealed: AtomicBool,
    /// Set when the submission status transitions to terminal.
    pub terminal: AtomicBool,
    pub toplevels: RwLock<Vec<Arc<Step>>>,
    /// Per-submission attribution: maps a toplevel's drv_hash to the
    /// human-readable attr name from nix-eval-jobs (e.g.
    /// `packages.x86_64-linux.hello`). Populated at ingest when the
    /// caller supplies `IngestDrvRequest.attr` for an `is_root=true`
    /// drv. Used by the failure path to compute `used_by_attrs` —
    /// "FAILED gcc-13.2.0, used by: packages.x86_64-linux.hello".
    pub root_attrs: RwLock<HashMap<DrvHash, String>>,
    pub ready: RwLock<HashMap<String, VecDeque<Weak<Step>>>>,
    /// Strong references to every step this submission owns — root
    /// or transitive dep. Keeps steps alive across handler returns
    /// even when no other strong ref exists; the Steps registry is
    /// weak-only (invariant 8). Dropped when the Submission itself
    /// is removed from the Submissions map.
    pub members: RwLock<HashMap<DrvHash, Arc<Step>>>,
    /// Failure details for drvs in this submission. Populated by the
    /// complete handler on terminal failure (originating + propagated).
    /// Replaces the previous durable `derivations.error_*` columns.
    pub failures: RwLock<Vec<DrvFailure>>,
    pub events: broadcast::Sender<JobEvent>,
    /// Cumulative count of drvs marked propagated_failure in this
    /// submission. Surfaced in `Progress` events for the runner UI.
    pub propagated_failed: AtomicU32,
    /// Cumulative count of transient retries observed on this
    /// submission's drvs. Surfaced in `Progress` events.
    pub transient_retries: AtomicU32,
    /// Set to true the first time the submission's `members.len()`
    /// crosses `submission_warn_threshold`. Lets the ingest path emit
    /// a single `WARN` log line (and metrics counter) per oversized
    /// submission rather than one per ingest batch.
    pub warned_oversized: AtomicBool,
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
            root_attrs: RwLock::new(HashMap::new()),
            ready: RwLock::new(HashMap::new()),
            members: RwLock::new(HashMap::new()),
            failures: RwLock::new(Vec::new()),
            events: tx,
            propagated_failed: AtomicU32::new(0),
            transient_retries: AtomicU32::new(0),
            warned_oversized: AtomicBool::new(false),
        })
    }

    /// Append a failure record for this submission. Idempotent on
    /// `drv_hash` — the first record wins (originating failure has
    /// priority over a later propagation for the same drv).
    pub fn record_failure(&self, failure: DrvFailure) {
        let mut guard = self.failures.write();
        if guard.iter().any(|f| f.drv_hash == failure.drv_hash) {
            return;
        }
        guard.push(failure);
    }

    /// Add a step to this submission's membership (strong ref).
    /// Idempotent on duplicate `drv_hash`.
    pub fn add_member(&self, step: &Arc<Step>) {
        let mut guard = self.members.write();
        guard
            .entry(step.drv_hash().clone())
            .or_insert_with(|| step.clone());
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
            // `next_attempt_at == 0` means no backoff; since now_ms is
            // always wall-clock positive, `0 > now_ms` is false and the
            // comparison alone suffices.
            let next_at = step.next_attempt_at.load(Ordering::Acquire);
            if next_at > now_ms {
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

    /// Compute live counts from the submission's member set.
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

    /// Compute live status from the toplevels. `Done` / `Failed` only
    /// emerge from a sealed submission whose roots are all finished;
    /// an unsealed submission is always Pending / Building regardless
    /// of root state because more roots may still be ingested.
    pub fn live_status(&self) -> crate::types::JobStatus {
        use crate::types::JobStatus;
        let tops = self.toplevels.read();
        let all_finished = tops.iter().all(|t| t.finished.load(Ordering::Acquire));
        let any_failed = tops
            .iter()
            .any(|t| t.previous_failure.load(Ordering::Acquire));
        match (self.is_sealed(), all_finished, any_failed) {
            (true, true, true) => JobStatus::Failed,
            (true, true, false) => JobStatus::Done,
            (true, false, _) => JobStatus::Building,
            (false, _, _) => JobStatus::Pending,
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

    /// Snapshot every live submission. Used by the fleet `/claim`
    /// endpoint to FIFO-sort by `created_at` and drain. Cloning the
    /// `Arc`s is cheap; the read lock is released before the caller
    /// touches submission state.
    pub fn all(&self) -> Vec<Arc<Submission>> {
        self.inner.read().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DrvState;
    use std::sync::atomic::Ordering;

    fn mk_step(name: &str) -> Arc<Step> {
        let hash = crate::types::DrvHash::new(format!("{name}.drv"));
        Step::new(
            hash.clone(),
            format!("/nix/store/{hash}"),
            name.to_string(),
            "x86_64-linux".into(),
            Vec::new(),
            2,
        )
    }

    #[test]
    fn submissions_is_empty_tracks_inserts() {
        let subs = Submissions::new();
        assert!(subs.is_empty());
        assert_eq!(subs.len(), 0);
        let id = JobId::new();
        let _ = subs.get_or_insert(id, 8);
        assert!(!subs.is_empty());
        assert_eq!(subs.len(), 1);
        subs.remove(id);
        assert!(subs.is_empty());
    }

    #[test]
    fn live_counts_totals_match_membership() {
        // live_counts uses `c.total += 1` per member plus per-state
        // increments. A mutation that replaces `+=` with `*=` for any
        // of the four state lanes would silently stick that count at
        // 0 or 1 — cover ALL four lanes (Pending, Building, Done,
        // Failed) so every increment site is asserted with a count > 1.
        let sub = Submission::new(JobId::new(), 8);
        // Two of each state so *= vs += is visibly different (2 vs 1).
        let pending = [mk_step("p1"), mk_step("p2")];
        let building = [mk_step("b1"), mk_step("b2")];
        let done = [mk_step("d1"), mk_step("d2")];
        let failed = [mk_step("f1"), mk_step("f2")];

        for step in pending.iter() {
            sub.add_member(step);
            step.runnable.store(true, Ordering::Release);
        }
        for step in building.iter() {
            sub.add_member(step);
            // Building = tries > 0 ∧ !runnable ∧ !finished.
            step.tries.store(1, Ordering::Release);
        }
        for step in done.iter() {
            sub.add_member(step);
            step.finished.store(true, Ordering::Release);
        }
        for step in failed.iter() {
            sub.add_member(step);
            step.finished.store(true, Ordering::Release);
            step.previous_failure.store(true, Ordering::Release);
        }

        let counts = sub.live_counts();
        assert_eq!(counts.total, 8, "total must be linear in membership");
        assert_eq!(counts.pending, 2);
        assert_eq!(counts.building, 2);
        assert_eq!(counts.done, 2);
        assert_eq!(counts.failed, 2);
        // Sanity on observable_state mapping.
        assert!(matches!(pending[0].observable_state(), DrvState::Pending));
        assert!(matches!(building[0].observable_state(), DrvState::Building));
        assert!(matches!(done[0].observable_state(), DrvState::Done));
        assert!(matches!(failed[0].observable_state(), DrvState::Failed));
    }

    #[test]
    fn pop_runnable_respects_next_attempt_at_boundary() {
        // `next_attempt_at > 0 && next_attempt_at > now_ms` gates the
        // retry-backoff requeue. Off-by-one on either comparison would
        // either skip the backoff entirely (letting an ineligible step
        // be claimed early) or hold forever. Exercise the precise
        // boundary at now_ms == next_attempt_at.
        let sub = Submission::new(JobId::new(), 8);
        let step = mk_step("delayed");
        step.runnable.store(true, Ordering::Release);
        step.next_attempt_at.store(1_000, Ordering::Release);
        sub.add_member(&step);
        sub.enqueue_ready(&step);

        // now_ms < next_attempt_at: not claimable.
        assert!(sub.pop_runnable("x86_64-linux", &[], 999).is_none());
        // now_ms == next_attempt_at: eligible (`>` not `>=`).
        let claimed = sub.pop_runnable("x86_64-linux", &[], 1_000);
        assert!(
            claimed.is_some(),
            "must claim at exact eligibility boundary"
        );

        // And: next_attempt_at = 0 always means unconditionally eligible.
        let fresh = mk_step("fresh");
        fresh.runnable.store(true, Ordering::Release);
        // next_attempt_at defaults to 0.
        let sub2 = Submission::new(JobId::new(), 8);
        sub2.add_member(&fresh);
        sub2.enqueue_ready(&fresh);
        assert!(sub2.pop_runnable("x86_64-linux", &[], 0).is_some());
    }
}
