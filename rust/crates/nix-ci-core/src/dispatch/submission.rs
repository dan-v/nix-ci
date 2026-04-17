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
    /// Caller-supplied priority. Higher = scanned first by the fleet
    /// scheduler. In-memory only; a restart cancels the submission
    /// and the caller retries with whatever priority they want.
    pub priority: i32,
    /// Caller-supplied per-job cap on concurrent in-flight claims.
    /// `None` = no cap. When `active_claims >= max_workers`, the
    /// fleet scheduler skips this submission until one completes.
    pub max_workers: Option<u32>,
    /// Caller-supplied override of the server's default claim deadline
    /// (in seconds). `None` = use the server default from
    /// `ServerConfig::claim_deadline_secs`. Read by `issue_claim`.
    pub claim_deadline_secs: Option<u64>,
    /// Live counter of in-flight claims for this submission. Bumped
    /// by `issue_claim`, decremented by `complete` / `evict_claims_for`.
    pub active_claims: AtomicU32,
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
    /// Running count of drvs reserved for this submission by
    /// in-flight ingest batches. Used by the per-job drv-cap check to
    /// race-safely reserve slots before membership mutation: two
    /// concurrent batches both fetch_add their size first, and at
    /// least one will see a post-reservation count above the cap and
    /// roll back. Without this, concurrent batches could both read
    /// the same `members.len()`, both pass the check, and both insert
    /// — silently overshooting the cap.
    pub reserved_drvs: AtomicU32,
}

impl Submission {
    pub fn new(id: JobId, event_capacity: usize) -> Arc<Self> {
        Self::with_options(id, event_capacity, 0, None, None)
    }

    pub fn with_options(
        id: JobId,
        event_capacity: usize,
        priority: i32,
        max_workers: Option<u32>,
        claim_deadline_secs: Option<u64>,
    ) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(event_capacity);
        Arc::new(Self {
            id,
            created_at: Instant::now(),
            priority,
            max_workers,
            claim_deadline_secs,
            active_claims: AtomicU32::new(0),
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
            reserved_drvs: AtomicU32::new(0),
        })
    }

    /// Race-safely reserve `n` drv slots against `cap`. Returns `true`
    /// if the reservation succeeded (caller may proceed with ingest);
    /// `false` if accepting this batch would exceed the cap, in which
    /// case the counter is rolled back to its pre-call value and the
    /// caller must reject the batch.
    ///
    /// Uses fetch_add then range-check: two concurrent calls whose
    /// combined total exceeds the cap will both fetch_add first, one
    /// will see a post-reservation total > cap and roll back; the
    /// other also rolls back if its own total was over. A total
    /// below cap always succeeds. Saturating semantics prevent
    /// u32 overflow from a pathologically large batch.
    pub fn try_reserve_drvs(&self, n: u32, cap: u32) -> bool {
        let prev = self.reserved_drvs.fetch_add(n, Ordering::AcqRel);
        let new_total = prev.saturating_add(n);
        if new_total > cap {
            self.reserved_drvs.fetch_sub(n, Ordering::AcqRel);
            false
        } else {
            true
        }
    }

    /// True if this submission is at its in-flight claim cap. Fleet
    /// scheduler uses this as a "skip this submission for now" signal.
    pub fn at_worker_cap(&self) -> bool {
        let Some(cap) = self.max_workers else {
            return false;
        };
        self.active_claims.load(Ordering::Acquire) >= cap
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

    /// Walk the dep graph rooted at `toplevels` and return a cycle if
    /// one exists. Iterative DFS with 3-color marking (White / Grey /
    /// Black). A Grey→Grey edge is a back-edge = cycle. Returns the
    /// `drv_hash`es on the cycle (closed path) in reverse DFS order, or
    /// `None` if the graph is acyclic.
    ///
    /// Called at seal time. O(V+E). We hold `members.read()` for the
    /// duration so no ingest can sneak in a new edge mid-scan — but
    /// since seal is already exclusive with `reject_if_terminal` this
    /// is a belt-and-suspenders measure.
    pub fn detect_cycle(&self) -> Option<Vec<DrvHash>> {
        use std::collections::HashMap;
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Color {
            White,
            Grey,
            Black,
        }
        let tops = self.toplevels.read();
        if tops.is_empty() {
            return None;
        }
        // Snapshot members + their dep sets under one lock acquisition
        // so the DFS doesn't re-take Step::state locks per visit. The
        // snapshot is just drv_hash → Vec<drv_hash>.
        let members = self.members.read();
        let mut adj: HashMap<DrvHash, Vec<DrvHash>> = HashMap::with_capacity(members.len());
        for (hash, step) in members.iter() {
            let deps = step
                .state
                .read()
                .deps
                .iter()
                .map(|dep| dep.drv_hash().clone())
                .collect();
            adj.insert(hash.clone(), deps);
        }
        drop(members);
        let mut color: HashMap<DrvHash, Color> =
            adj.keys().map(|k| (k.clone(), Color::White)).collect();
        let mut parent: HashMap<DrvHash, DrvHash> = HashMap::new();

        for top in tops.iter() {
            let start = top.drv_hash().clone();
            if color.get(&start).copied() != Some(Color::White) {
                continue;
            }
            // Iterative DFS. The stack stores (node, next_dep_ix) so we
            // don't borrow across iterations.
            let mut stack: Vec<(DrvHash, usize)> = vec![(start.clone(), 0)];
            color.insert(start, Color::Grey);
            while let Some((node, ix)) = stack.last().cloned() {
                let deps = adj.get(&node).cloned().unwrap_or_default();
                if ix >= deps.len() {
                    color.insert(node, Color::Black);
                    stack.pop();
                    continue;
                }
                if let Some(last) = stack.last_mut() {
                    last.1 = ix + 1;
                }
                let dep = deps[ix].clone();
                match color.get(&dep).copied().unwrap_or(Color::White) {
                    Color::White => {
                        color.insert(dep.clone(), Color::Grey);
                        parent.insert(dep.clone(), node.clone());
                        stack.push((dep, 0));
                    }
                    Color::Grey => {
                        // Back-edge: node → dep and dep is an ancestor.
                        // Reconstruct cycle: dep ← ... ← node ← dep.
                        let mut cycle = vec![dep.clone()];
                        let mut cur = node.clone();
                        while cur != dep {
                            cycle.push(cur.clone());
                            match parent.get(&cur) {
                                Some(p) => cur = p.clone(),
                                None => break, // defensive; shouldn't happen
                            }
                        }
                        cycle.push(dep);
                        return Some(cycle);
                    }
                    Color::Black => { /* fully-explored; no cycle via this edge */ }
                }
            }
        }
        None
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

/// Dual index over live submissions: a HashMap for O(1) lookup by id,
/// and a BTreeMap keyed on `(created_at, id)` for O(N) sorted iteration
/// without a per-call sort. The fleet claim endpoint hot-paths the
/// sorted snapshot; a prior implementation re-sorted `all()` on every
/// wake-up which was O(N log N) per claim attempt across thousands of
/// workers on a single `notify_waiters()` edge.
///
/// Both structures mirror each other. `insert` and `remove` maintain
/// the pair atomically under a single write lock; readers always hit
/// a consistent view.
pub struct Submissions {
    inner: RwLock<SubmissionsInner>,
}

struct SubmissionsInner {
    by_id: HashMap<JobId, Arc<Submission>>,
    /// Secondary index keyed on the scheduling tuple
    /// `(Reverse(priority), created_at, id)`. BTreeMap iterates in
    /// ascending-key order, so `Reverse(priority)` puts higher-priority
    /// jobs FIRST; within a tier, older `created_at` wins; within the
    /// same `Instant`, id is a stable tiebreaker. Maintained atomically
    /// with `by_id` under a single write lock.
    by_schedule: std::collections::BTreeMap<ScheduleKey, Arc<Submission>>,
}

type ScheduleKey = (std::cmp::Reverse<i32>, Instant, JobId);

fn schedule_key(sub: &Submission) -> ScheduleKey {
    (std::cmp::Reverse(sub.priority), sub.created_at, sub.id)
}

impl Default for Submissions {
    fn default() -> Self {
        Self::new()
    }
}

impl Submissions {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(SubmissionsInner {
                by_id: HashMap::new(),
                by_schedule: std::collections::BTreeMap::new(),
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().by_id.is_empty()
    }

    pub fn get(&self, id: JobId) -> Option<Arc<Submission>> {
        self.inner.read().by_id.get(&id).cloned()
    }

    pub fn get_or_insert(&self, id: JobId, event_capacity: usize) -> Arc<Submission> {
        self.get_or_insert_with_options(id, event_capacity, 0, None, None)
    }

    /// Insert with explicit scheduling options. Used by the POST /jobs
    /// handler to attach priority + max_workers + claim_deadline_secs
    /// at the authoritative creation point. On dedup hit, returns the
    /// existing submission — the options are **not** applied
    /// retroactively (an already-live submission keeps its original
    /// params).
    pub fn get_or_insert_with_options(
        &self,
        id: JobId,
        event_capacity: usize,
        priority: i32,
        max_workers: Option<u32>,
        claim_deadline_secs: Option<u64>,
    ) -> Arc<Submission> {
        if let Some(existing) = self.inner.read().by_id.get(&id).cloned() {
            return existing;
        }
        let mut guard = self.inner.write();
        if let Some(existing) = guard.by_id.get(&id).cloned() {
            return existing;
        }
        let sub = Submission::with_options(
            id,
            event_capacity,
            priority,
            max_workers,
            claim_deadline_secs,
        );
        let key = schedule_key(&sub);
        guard.by_id.insert(id, sub.clone());
        guard.by_schedule.insert(key, sub.clone());
        sub
    }

    pub fn remove(&self, id: JobId) -> Option<Arc<Submission>> {
        let mut guard = self.inner.write();
        let removed = guard.by_id.remove(&id)?;
        guard.by_schedule.remove(&schedule_key(&removed));
        Some(removed)
    }

    /// Snapshot every live submission in unspecified order. Callers
    /// that need FIFO order use `sorted_by_created_at` instead.
    pub fn all(&self) -> Vec<Arc<Submission>> {
        self.inner.read().by_id.values().cloned().collect()
    }

    /// Snapshot every live submission in schedule order:
    /// higher-priority first, then oldest first within a priority
    /// tier. Used by the fleet `/claim` endpoint — no per-call sort
    /// even under thousand-worker wake storms.
    pub fn sorted_by_created_at(&self) -> Vec<Arc<Submission>> {
        self.inner.read().by_schedule.values().cloned().collect()
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

    // ─── try_reserve_drvs (cap-race guard) ──────────────────────

    #[test]
    fn try_reserve_under_cap_succeeds() {
        let sub = Submission::new(JobId::new(), 8);
        assert!(sub.try_reserve_drvs(100, 1000));
        assert_eq!(sub.reserved_drvs.load(Ordering::Acquire), 100);
        assert!(sub.try_reserve_drvs(200, 1000));
        assert_eq!(sub.reserved_drvs.load(Ordering::Acquire), 300);
    }

    #[test]
    fn try_reserve_at_cap_succeeds() {
        // cap=1000 and we reserve exactly 1000 in one go — must succeed.
        // Off-by-one here would reject legitimate submitters.
        let sub = Submission::new(JobId::new(), 8);
        assert!(sub.try_reserve_drvs(1000, 1000));
        assert_eq!(sub.reserved_drvs.load(Ordering::Acquire), 1000);
    }

    #[test]
    fn try_reserve_over_cap_rolls_back() {
        let sub = Submission::new(JobId::new(), 8);
        sub.reserved_drvs.store(900, Ordering::Release);
        // 900 + 200 > 1000 → rejected. Counter must return to 900.
        assert!(!sub.try_reserve_drvs(200, 1000));
        assert_eq!(sub.reserved_drvs.load(Ordering::Acquire), 900);
    }

    #[test]
    fn try_reserve_concurrent_both_over_cap_both_fail() {
        // THE race: two concurrent reservations whose combined total
        // would exceed the cap. The prior `members.len()` check let
        // both pass. `try_reserve_drvs` uses fetch_add-then-check so
        // at least one must fail and roll back. Property under
        // stress: `reserved_drvs` never exceeds the cap after all
        // calls complete.
        use std::sync::Arc;
        use std::thread;
        const CAP: u32 = 10_000;
        // 100 threads, each reserving 200. Only ~50 should succeed
        // (10_000 / 200 = 50); the rest must roll back.
        let sub = Submission::new(JobId::new(), 8);
        let succeeded = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let mut handles = Vec::with_capacity(100);
        for _ in 0..100 {
            let sub = sub.clone();
            let ok = succeeded.clone();
            handles.push(thread::spawn(move || {
                if sub.try_reserve_drvs(200, CAP) {
                    ok.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let final_reserved = sub.reserved_drvs.load(Ordering::Acquire);
        assert!(
            final_reserved <= CAP,
            "reserved_drvs ({final_reserved}) must never exceed cap ({CAP})"
        );
        // The exact number of successes depends on interleaving, but
        // at least one must fail (100 × 200 = 20_000 > 10_000 cap).
        let ok = succeeded.load(Ordering::Relaxed);
        assert!(ok < 100, "cap must reject at least one reservation");
        assert_eq!(
            final_reserved,
            ok * 200,
            "every success must correspond to 200 reserved"
        );
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
    fn sorted_by_created_at_returns_fifo_order() {
        // Three submissions inserted at increasing Instant ticks. The
        // sorted snapshot must preserve FIFO order — load-bearing for
        // fleet claim fairness.
        let subs = Submissions::new();
        let id1 = JobId::new();
        let s1 = subs.get_or_insert(id1, 8);
        // Force a detectable gap between Instants so BTreeMap can
        // order them; Instant::now() resolution is machine-dependent.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = JobId::new();
        let s2 = subs.get_or_insert(id2, 8);
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id3 = JobId::new();
        let s3 = subs.get_or_insert(id3, 8);

        let sorted = subs.sorted_by_created_at();
        let ids: Vec<_> = sorted.iter().map(|s| s.id).collect();
        assert_eq!(ids, vec![id1, id2, id3], "FIFO order must be preserved");
        // Arc identity preserved end-to-end.
        assert!(Arc::ptr_eq(&sorted[0], &s1));
        assert!(Arc::ptr_eq(&sorted[1], &s2));
        assert!(Arc::ptr_eq(&sorted[2], &s3));
    }

    #[test]
    fn remove_prunes_both_indexes() {
        // A dangling entry in the secondary BTreeMap after a remove()
        // would return a zombie Arc to callers of sorted_by_created_at.
        // Guard by asserting len() (read from by_id) matches the
        // sorted snapshot length after a remove.
        let subs = Submissions::new();
        let ids: Vec<_> = (0..5)
            .map(|_| {
                let id = JobId::new();
                let _ = subs.get_or_insert(id, 8);
                id
            })
            .collect();
        assert_eq!(subs.len(), 5);
        assert_eq!(subs.sorted_by_created_at().len(), 5);
        subs.remove(ids[2]);
        assert_eq!(subs.len(), 4);
        assert_eq!(
            subs.sorted_by_created_at().len(),
            4,
            "secondary index must drop in lockstep"
        );
        // The removed id must not appear in the sorted snapshot.
        assert!(subs.sorted_by_created_at().iter().all(|s| s.id != ids[2]));
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

    // ─── Cycle detection (C2) ───────────────────────────────────

    fn attach_dep_for_test(parent: &Arc<Step>, dep: &Arc<Step>) {
        parent
            .state
            .write()
            .deps
            .insert(crate::dispatch::step::StepHandle(dep.clone()));
    }

    #[test]
    fn detect_cycle_empty_graph_is_none() {
        let sub = Submission::new(JobId::new(), 8);
        assert!(sub.detect_cycle().is_none());
    }

    #[test]
    fn detect_cycle_pure_dag_is_none() {
        // a → b → c (linear), and a → c (direct). No cycle.
        let sub = Submission::new(JobId::new(), 8);
        let a = mk_step("a");
        let b = mk_step("b");
        let c = mk_step("c");
        for s in [&a, &b, &c] {
            sub.add_member(s);
        }
        sub.add_root(a.clone());
        attach_dep_for_test(&a, &b);
        attach_dep_for_test(&b, &c);
        attach_dep_for_test(&a, &c);
        assert!(sub.detect_cycle().is_none());
    }

    #[test]
    fn detect_cycle_simple_two_node_cycle() {
        // a → b → a
        let sub = Submission::new(JobId::new(), 8);
        let a = mk_step("a");
        let b = mk_step("b");
        sub.add_member(&a);
        sub.add_member(&b);
        sub.add_root(a.clone());
        attach_dep_for_test(&a, &b);
        attach_dep_for_test(&b, &a);
        let cycle = sub.detect_cycle().expect("cycle should be detected");
        // The reported cycle must contain both a and b (exact ordering
        // depends on DFS traversal but both must be present, at least
        // one repeat as the closing edge).
        let hashes: std::collections::HashSet<_> = cycle.iter().collect();
        assert!(hashes.len() >= 2, "cycle must touch ≥ 2 distinct drvs");
    }

    #[test]
    fn detect_cycle_deep_chain_closing() {
        // a → b → c → d → b (cycle inside the chain)
        let sub = Submission::new(JobId::new(), 8);
        let a = mk_step("a");
        let b = mk_step("b");
        let c = mk_step("c");
        let d = mk_step("d");
        for s in [&a, &b, &c, &d] {
            sub.add_member(s);
        }
        sub.add_root(a.clone());
        attach_dep_for_test(&a, &b);
        attach_dep_for_test(&b, &c);
        attach_dep_for_test(&c, &d);
        attach_dep_for_test(&d, &b);
        let cycle = sub.detect_cycle().expect("cycle should be detected");
        assert!(cycle.len() >= 3, "chain-closing cycle must show ≥ 3 nodes");
    }

    #[test]
    fn detect_cycle_disjoint_components() {
        // Component 1 (a → b): clean. Component 2 (c → d → c): cycle.
        // Both toplevels; detection must find the cycle.
        let sub = Submission::new(JobId::new(), 8);
        let a = mk_step("a");
        let b = mk_step("b");
        let c = mk_step("c");
        let d = mk_step("d");
        for s in [&a, &b, &c, &d] {
            sub.add_member(s);
        }
        sub.add_root(a.clone());
        sub.add_root(c.clone());
        attach_dep_for_test(&a, &b);
        attach_dep_for_test(&c, &d);
        attach_dep_for_test(&d, &c);
        assert!(sub.detect_cycle().is_some());
    }

    #[test]
    fn detect_cycle_diamond_is_not_a_cycle() {
        // Two paths from a to d via b and c. Not a cycle.
        let sub = Submission::new(JobId::new(), 8);
        let a = mk_step("a");
        let b = mk_step("b");
        let c = mk_step("c");
        let d = mk_step("d");
        for s in [&a, &b, &c, &d] {
            sub.add_member(s);
        }
        sub.add_root(a.clone());
        attach_dep_for_test(&a, &b);
        attach_dep_for_test(&a, &c);
        attach_dep_for_test(&b, &d);
        attach_dep_for_test(&c, &d);
        assert!(
            sub.detect_cycle().is_none(),
            "diamond DAGs must not be flagged as cycles"
        );
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
