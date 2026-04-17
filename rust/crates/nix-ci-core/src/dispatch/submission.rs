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
