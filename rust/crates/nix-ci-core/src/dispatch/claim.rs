//! Active-claim tracking. A claim is the ephemeral lease a worker
//! holds on a specific drv while it's building. Purely in-memory —
//! nothing about in-flight claims is persisted.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
// `tokio::time::Instant` is a zero-cost wrapper around
// `std::time::Instant` in a running tokio runtime — identical perf
// and semantics in production. Under `tokio::time::pause()` (tests,
// deterministic-sim runners like turmoil) it returns the runtime's
// virtual clock, which is what lets the reaper fire on simulated
// time. Using it for every scheduling-relevant timestamp —
// claim deadlines, started_at — preserves production behavior and
// unlocks paused-time testing. Rule of thumb: in tokio code,
// default to `tokio::time::Instant`; reserve `std::time::Instant`
// for genuinely non-async contexts (none here).
use tokio::time::Instant;

use crate::types::{ClaimId, DrvHash, JobId};

pub struct ActiveClaim {
    pub claim_id: ClaimId,
    pub job_id: JobId,
    pub drv_hash: DrvHash,
    pub attempt: i32,
    /// Deadline at which the reaper re-arms the step unless the worker
    /// extends the lease via `POST .../claims/{id}/extend`. Mutable so
    /// a long-running build can keep the claim alive.
    pub deadline: Mutex<Instant>,
    /// Original lease window. Extensions set `deadline = now() +
    /// deadline_window` so the refresh behavior is stable even if the
    /// server-side config changes mid-flight.
    pub deadline_window: Duration,
    /// When the claim was issued. Used by the `Progress` event to
    /// report "currently building X (3m 12s)" — derive elapsed via
    /// `Instant::now() - started_at`. Wall-clock millis are computed
    /// on demand at event-emission time.
    pub started_at: Instant,
    /// Wall-clock when the claim was issued. Persistent for
    /// `GET /claims` callers (which can't read `Instant`).
    pub started_at_wall: chrono::DateTime<chrono::Utc>,
    /// Optional worker identity (e.g. `host42-pid12345-a3b91c`)
    /// supplied by the worker on the `?worker=` query param. Empty
    /// for older clients.
    pub worker_id: Option<String>,
    /// Absolute wall-ceiling: no extension may push `deadline` past
    /// this instant. Protects against a worker that keeps sending
    /// `/extend` but never finishes — e.g. a build stuck on NFS I/O
    /// that still heartbeats. Derived from `started_at +
    /// max_claim_lifetime_secs` at issue time; `None` disables the
    /// ceiling (v3 default, matching `max_claim_lifetime_secs`).
    pub hard_deadline: Option<Instant>,
}

/// Dual-indexed claim store. `by_id` is the primary lookup by
/// `claim_id`; `by_job` is a secondary `HashMap<JobId, HashSet<ClaimId>>`
/// so per-job eviction and per-job iteration (SSE progress tick) are
/// O(claims for that job) rather than O(total claims). At 10K
/// in-flight claims across 100 jobs, the primary-only approach scanned
/// 10K entries per cancel event; the dual-index version walks ~100.
/// Both maps are maintained atomically under a single write lock.
pub struct Claims {
    inner: RwLock<ClaimsInner>,
}

struct ClaimsInner {
    by_id: HashMap<ClaimId, Arc<ActiveClaim>>,
    by_job: HashMap<JobId, std::collections::HashSet<ClaimId>>,
}

/// Outcome when a caller tries to take a claim but the path-bound
/// job_id doesn't match the claim's recorded job_id. Split from
/// `None` so handlers can distinguish "stale claim" (treat as
/// ignored, normal) from "wrong job" (400 BadRequest — a client
/// contract violation worth surfacing).
#[derive(Debug, PartialEq, Eq)]
pub enum ClaimJobMismatch {
    /// Claim not in the map. Treat as stale — return ignored:true.
    NotFound,
    /// Claim exists but belongs to `actual`. The claim is left in
    /// place so the correct job can still complete it.
    WrongJob { actual: JobId },
}

impl Default for Claims {
    fn default() -> Self {
        Self::new()
    }
}

impl Claims {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(ClaimsInner {
                by_id: HashMap::new(),
                by_job: HashMap::new(),
            }),
        }
    }

    pub fn insert(&self, claim: Arc<ActiveClaim>) {
        let mut guard = self.inner.write();
        guard
            .by_job
            .entry(claim.job_id)
            .or_default()
            .insert(claim.claim_id);
        guard.by_id.insert(claim.claim_id, claim);
    }

    pub fn take(&self, claim_id: ClaimId) -> Option<Arc<ActiveClaim>> {
        let mut guard = self.inner.write();
        let claim = guard.by_id.remove(&claim_id)?;
        // Prune the by_job entry. If the set becomes empty, drop the
        // outer map entry too — otherwise a million short-lived jobs
        // would leave a million empty HashSets as residue.
        if let Some(set) = guard.by_job.get_mut(&claim.job_id) {
            set.remove(&claim_id);
            if set.is_empty() {
                guard.by_job.remove(&claim.job_id);
            }
        }
        Some(claim)
    }

    /// Atomic take-if-job-matches. Returned value is `Ok(claim)` when
    /// the claim existed and `claim.job_id == expected_job_id`;
    /// `Err(ClaimJobMismatch::NotFound)` when the claim wasn't in
    /// the map (already completed or never issued); and
    /// `Err(ClaimJobMismatch::WrongJob { .. })` when the claim was
    /// found but belongs to a different job — crucially, in that case
    /// the claim is LEFT IN PLACE, so a subsequent /complete under
    /// the correct job_id still succeeds. Before this was introduced
    /// a cross-job complete removed the claim before the job_id check
    /// in server/complete.rs ran, eating the claim; this helper moves
    /// the check inside the write-lock to close the gap.
    pub fn take_for_job(
        &self,
        claim_id: ClaimId,
        expected_job_id: JobId,
    ) -> std::result::Result<Arc<ActiveClaim>, ClaimJobMismatch> {
        let mut guard = self.inner.write();
        match guard.by_id.get(&claim_id) {
            None => Err(ClaimJobMismatch::NotFound),
            Some(c) if c.job_id != expected_job_id => {
                Err(ClaimJobMismatch::WrongJob { actual: c.job_id })
            }
            Some(_) => {
                let claim = guard
                    .by_id
                    .remove(&claim_id)
                    .expect("just checked present");
                if let Some(set) = guard.by_job.get_mut(&expected_job_id) {
                    set.remove(&claim_id);
                    if set.is_empty() {
                        guard.by_job.remove(&expected_job_id);
                    }
                }
                Ok(claim)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().by_id.is_empty()
    }

    /// Return every claim whose deadline has passed. The caller must
    /// remove them via `take()` before acting.
    pub fn expired_ids(&self, now: Instant) -> Vec<ClaimId> {
        self.inner
            .read()
            .by_id
            .values()
            .filter(|c| *c.deadline.lock() <= now)
            .map(|c| c.claim_id)
            .collect()
    }

    /// Extend the deadline of an active claim to `now + deadline_window`,
    /// capped at the claim's `hard_deadline` (when set). Returns the new
    /// deadline if the claim was found, `None` otherwise (claim already
    /// expired / completed / evicted). The caller uses `None` as the
    /// signal to stop sending extensions.
    ///
    /// When the hard ceiling pins the new deadline below
    /// `now + deadline_window`, the caller can detect this by
    /// comparing the returned value — the extend still succeeds (so
    /// the worker keeps running until the ceiling hits) but future
    /// extensions will saturate at the same instant. Once `now` is
    /// past the hard deadline, the returned value is in the past and
    /// the reaper will evict on its next tick.
    pub fn extend(&self, claim_id: ClaimId, now: Instant) -> Option<Instant> {
        let guard = self.inner.read();
        let claim = guard.by_id.get(&claim_id)?;
        let proposed = now + claim.deadline_window;
        let new_deadline = match claim.hard_deadline {
            Some(ceiling) if ceiling < proposed => ceiling,
            _ => proposed,
        };
        *claim.deadline.lock() = new_deadline;
        Some(new_deadline)
    }

    /// True when this claim's deadline was capped by its hard ceiling
    /// — i.e. the claim's `deadline` equals its `hard_deadline`.
    /// Callers in the reap path use this to distinguish a normal
    /// deadline-expiry reap (worker stopped heartbeating) from a
    /// hard-ceiling reap (worker kept extending forever). Metrics
    /// split on this axis so operators can alert on stuck-but-alive
    /// workers without noise from normal reap.
    pub fn was_reaped_by_hard_ceiling(claim: &ActiveClaim, now: Instant) -> bool {
        let Some(ceiling) = claim.hard_deadline else {
            return false;
        };
        now >= ceiling
    }

    /// Snapshot every active claim. Used by the reaper for cross-job
    /// scans (`expired_ids`) and by operator endpoints (`/claims`,
    /// `/admin/debug/dispatcher-dump`). Callers that want per-job
    /// claims should use `by_job` instead — it avoids cloning every
    /// unrelated Arc in the map.
    pub fn all(&self) -> Vec<Arc<ActiveClaim>> {
        self.inner.read().by_id.values().cloned().collect()
    }

    /// Snapshot claims for one specific job. O(claims for that job)
    /// via the secondary `by_job` index, vs O(total claims) for
    /// `all().filter(|c| c.job_id == ..)`. Hot on the SSE progress
    /// tick (runs every 10s per subscriber per live job) and on
    /// per-job eviction paths.
    pub fn by_job(&self, job_id: JobId) -> Vec<Arc<ActiveClaim>> {
        let guard = self.inner.read();
        let Some(set) = guard.by_job.get(&job_id) else {
            return Vec::new();
        };
        set.iter()
            .filter_map(|cid| guard.by_id.get(cid).cloned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn mk_claim(deadline: Instant) -> Arc<ActiveClaim> {
        mk_claim_with_ceiling(deadline, None)
    }

    fn mk_claim_with_ceiling(deadline: Instant, ceiling: Option<Instant>) -> Arc<ActiveClaim> {
        Arc::new(ActiveClaim {
            claim_id: ClaimId::new(),
            job_id: JobId::new(),
            drv_hash: DrvHash::new("drv-test.drv"),
            attempt: 1,
            deadline: Mutex::new(deadline),
            deadline_window: Duration::from_secs(60),
            started_at: Instant::now(),
            started_at_wall: chrono::Utc::now(),
            worker_id: None,
            hard_deadline: ceiling,
        })
    }

    #[test]
    fn is_empty_and_len_track_inserts_and_takes() {
        let c = Claims::new();
        assert!(c.is_empty());
        assert_eq!(c.len(), 0);
        let claim = mk_claim(Instant::now() + Duration::from_secs(60));
        let cid = claim.claim_id;
        c.insert(claim);
        assert!(!c.is_empty());
        assert_eq!(c.len(), 1);
        let _ = c.take(cid).expect("present");
        assert!(c.is_empty());
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn expired_ids_returns_only_past_deadlines() {
        let c = Claims::new();
        let now = Instant::now();
        let past = mk_claim(now - Duration::from_secs(5));
        let exactly_now = mk_claim(now);
        let future = mk_claim(now + Duration::from_secs(60));
        let past_id = past.claim_id;
        let now_id = exactly_now.claim_id;
        let future_id = future.claim_id;
        c.insert(past);
        c.insert(exactly_now);
        c.insert(future);

        // `<= now` semantics: both past and exactly-now expire; future
        // does not.
        let expired: std::collections::HashSet<ClaimId> = c.expired_ids(now).into_iter().collect();
        let want: std::collections::HashSet<ClaimId> = [past_id, now_id].into_iter().collect();
        assert_eq!(expired, want);
        assert!(!expired.contains(&future_id));
    }

    #[test]
    fn expired_ids_empty_when_no_claims_past_deadline() {
        let c = Claims::new();
        let now = Instant::now();
        c.insert(mk_claim(now + Duration::from_secs(60)));
        c.insert(mk_claim(now + Duration::from_secs(120)));
        assert!(c.expired_ids(now).is_empty());
    }

    #[test]
    fn extend_moves_deadline_forward_by_window() {
        // A claim about to expire; extend must push the deadline out by
        // the full deadline_window so a long-running worker stays alive.
        let c = Claims::new();
        let now = Instant::now();
        let claim = mk_claim(now + Duration::from_secs(1));
        let cid = claim.claim_id;
        c.insert(claim);
        let new_deadline = c.extend(cid, now).expect("claim must exist");
        // deadline_window is 60s from mk_claim; new deadline is now + 60s
        assert_eq!(new_deadline, now + Duration::from_secs(60));
        // And the deadline stored in the claim matches.
        assert!(c.expired_ids(now + Duration::from_secs(30)).is_empty());
    }

    #[test]
    fn extend_returns_none_for_unknown_claim_id() {
        // Stale-extension signal: the worker must know to stop sending
        // refreshes once the coordinator has lost the claim.
        let c = Claims::new();
        assert!(c.extend(ClaimId::new(), Instant::now()).is_none());
    }

    #[test]
    fn extend_after_take_is_none() {
        // Once a claim has been completed / evicted, extend must report
        // no-claim — callers rely on this to stop the refresh loop.
        let c = Claims::new();
        let claim = mk_claim(Instant::now() + Duration::from_secs(60));
        let cid = claim.claim_id;
        c.insert(claim);
        let _ = c.take(cid).expect("present");
        assert!(c.extend(cid, Instant::now()).is_none());
    }

    #[test]
    fn extend_caps_at_hard_deadline() {
        // The contract under test: no amount of extension can push
        // `deadline` past the hard ceiling. Without this, a worker
        // that keeps sending /extend while stuck can hold a drv
        // forever (the most-dangerous-failure-mode item E of the
        // reliability review).
        let c = Claims::new();
        let now = Instant::now();
        // Claim started at `now`, with window=60s and ceiling=90s
        // from `now`. After one extension at `now+30s` the extend's
        // proposed value is `now+30s+60s = now+90s`, which is
        // exactly the ceiling — just barely OK.
        //
        // After a *second* extension at `now+60s`, proposed is
        // `now+120s`, clearly past the ceiling: must clamp.
        let claim = mk_claim_with_ceiling(
            now + Duration::from_secs(60),
            Some(now + Duration::from_secs(90)),
        );
        let cid = claim.claim_id;
        c.insert(claim);

        // First extension at t=30s → proposed 90s, ceiling 90s. Equal.
        let t30 = now + Duration::from_secs(30);
        let new1 = c.extend(cid, t30).expect("claim present");
        assert_eq!(new1, now + Duration::from_secs(90));

        // Second extension at t=60s → proposed 120s, clamp to 90s.
        let t60 = now + Duration::from_secs(60);
        let new2 = c.extend(cid, t60).expect("claim present");
        assert_eq!(
            new2,
            now + Duration::from_secs(90),
            "extend must clamp at hard_deadline"
        );

        // Reaper behavior: at t=91s the claim is expired because
        // deadline=90s ≤ 91s. was_reaped_by_hard_ceiling returns true.
        let at_ceiling = now + Duration::from_secs(91);
        let expired = c.expired_ids(at_ceiling);
        assert!(
            expired.contains(&cid),
            "claim clamped to ceiling must be reaped once ceiling passes"
        );
        // Inspect the claim's hard-ceiling attribution.
        let taken = c.take(cid).expect("still present before take");
        assert!(
            Claims::was_reaped_by_hard_ceiling(&taken, at_ceiling),
            "ceiling-clamped claim past its hard_deadline must flag as ceiling-reaped"
        );
    }

    #[test]
    fn extend_without_ceiling_is_unbounded_like_before() {
        // Back-compat: `hard_deadline=None` must preserve the v3
        // behavior where /extend keeps pushing indefinitely.
        let c = Claims::new();
        let now = Instant::now();
        let claim = mk_claim_with_ceiling(now + Duration::from_secs(60), None);
        let cid = claim.claim_id;
        c.insert(claim);
        for i in 1..=5 {
            let t = now + Duration::from_secs(10 * i);
            let new = c.extend(cid, t).expect("claim present");
            assert_eq!(new, t + Duration::from_secs(60));
        }
    }

    #[test]
    fn was_reaped_by_hard_ceiling_is_false_before_ceiling() {
        let now = Instant::now();
        let claim = mk_claim_with_ceiling(
            now + Duration::from_secs(60),
            Some(now + Duration::from_secs(120)),
        );
        assert!(!Claims::was_reaped_by_hard_ceiling(&claim, now));
        assert!(!Claims::was_reaped_by_hard_ceiling(
            &claim,
            now + Duration::from_secs(119)
        ));
        assert!(Claims::was_reaped_by_hard_ceiling(
            &claim,
            now + Duration::from_secs(120)
        ));
    }

    // ─── by_job secondary index ─────────────────────────────────

    fn mk_claim_with_job(job_id: JobId) -> Arc<ActiveClaim> {
        Arc::new(ActiveClaim {
            claim_id: ClaimId::new(),
            job_id,
            drv_hash: DrvHash::new("drv-test.drv"),
            attempt: 1,
            deadline: Mutex::new(Instant::now() + Duration::from_secs(60)),
            deadline_window: Duration::from_secs(60),
            started_at: Instant::now(),
            started_at_wall: chrono::Utc::now(),
            worker_id: None,
            hard_deadline: None,
        })
    }

    #[test]
    fn by_job_returns_empty_for_unknown_job() {
        let c = Claims::new();
        c.insert(mk_claim_with_job(JobId::new()));
        assert!(
            c.by_job(JobId::new()).is_empty(),
            "unknown job_id must return empty, not all claims"
        );
    }

    #[test]
    fn by_job_returns_only_claims_for_that_job() {
        // The whole point of the secondary index: a per-job query
        // must not walk unrelated claims.
        let c = Claims::new();
        let job_a = JobId::new();
        let job_b = JobId::new();
        // 3 for A, 5 for B.
        for _ in 0..3 {
            c.insert(mk_claim_with_job(job_a));
        }
        for _ in 0..5 {
            c.insert(mk_claim_with_job(job_b));
        }

        let a = c.by_job(job_a);
        let b = c.by_job(job_b);
        assert_eq!(a.len(), 3);
        assert_eq!(b.len(), 5);
        assert!(
            a.iter().all(|cl| cl.job_id == job_a),
            "by_job(A) must only return job A's claims"
        );
        assert!(
            b.iter().all(|cl| cl.job_id == job_b),
            "by_job(B) must only return job B's claims"
        );
    }

    #[test]
    fn take_maintains_by_job_index() {
        // Removing a claim must also drop it from the secondary index,
        // otherwise by_job() would surface phantom claim_ids whose
        // primary-map entries have been removed.
        let c = Claims::new();
        let job = JobId::new();
        let claim = mk_claim_with_job(job);
        let cid = claim.claim_id;
        c.insert(claim);
        assert_eq!(c.by_job(job).len(), 1);
        let taken = c.take(cid).expect("present");
        assert_eq!(taken.claim_id, cid);
        assert!(
            c.by_job(job).is_empty(),
            "take must remove the claim from by_job too"
        );
        // And the outer map entry should be gone (the set is empty
        // after the only member is removed).
        assert!(
            !c.inner.read().by_job.contains_key(&job),
            "empty set must be pruned from by_job to prevent map bloat"
        );
    }

    #[test]
    fn take_for_job_maintains_by_job_index() {
        let c = Claims::new();
        let job = JobId::new();
        let claim = mk_claim_with_job(job);
        let cid = claim.claim_id;
        c.insert(claim);
        let taken = c.take_for_job(cid, job).expect("match");
        assert_eq!(taken.claim_id, cid);
        assert!(c.by_job(job).is_empty());
        assert!(!c.inner.read().by_job.contains_key(&job));
    }

    #[test]
    fn take_for_job_mismatch_does_not_touch_by_job() {
        // WrongJob is an error — the claim stays in the map AND in the
        // secondary index. Without this guard the claim would vanish
        // silently on a cross-job POST.
        let c = Claims::new();
        let owning_job = JobId::new();
        let other_job = JobId::new();
        let claim = mk_claim_with_job(owning_job);
        let cid = claim.claim_id;
        c.insert(claim);
        let err = match c.take_for_job(cid, other_job) {
            Err(e) => e,
            Ok(_) => panic!("mismatch must not take the claim"),
        };
        match err {
            ClaimJobMismatch::WrongJob { actual } => assert_eq!(actual, owning_job),
            other => panic!("expected WrongJob, got {other:?}"),
        }
        // Claim still in both indexes — the correct owner can still complete.
        assert!(c.take(cid).is_some());
    }

    #[test]
    fn insert_take_stress_leaves_indexes_consistent() {
        // Invariant: len() == sum over by_job of set lengths. Maintained
        // under random insert/take interleaving.
        let c = Claims::new();
        let job_a = JobId::new();
        let job_b = JobId::new();
        let mut ids_a: Vec<ClaimId> = Vec::new();
        let mut ids_b: Vec<ClaimId> = Vec::new();
        for _ in 0..50 {
            let ca = mk_claim_with_job(job_a);
            ids_a.push(ca.claim_id);
            c.insert(ca);
            let cb = mk_claim_with_job(job_b);
            ids_b.push(cb.claim_id);
            c.insert(cb);
        }
        // Remove first 30 of each.
        for cid in ids_a.drain(..30) {
            c.take(cid);
        }
        for cid in ids_b.drain(..30) {
            c.take(cid);
        }
        // 20 each remain.
        assert_eq!(c.len(), 40);
        assert_eq!(c.by_job(job_a).len(), 20);
        assert_eq!(c.by_job(job_b).len(), 20);
        // Invariant: sum of secondary sets == primary len.
        let guard = c.inner.read();
        let secondary_sum: usize = guard.by_job.values().map(|s| s.len()).sum();
        assert_eq!(
            secondary_sum,
            guard.by_id.len(),
            "secondary index must sum to primary index size"
        );
    }
}
