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

pub struct Claims {
    inner: RwLock<HashMap<ClaimId, Arc<ActiveClaim>>>,
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
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, claim: Arc<ActiveClaim>) {
        self.inner.write().insert(claim.claim_id, claim);
    }

    pub fn take(&self, claim_id: ClaimId) -> Option<Arc<ActiveClaim>> {
        self.inner.write().remove(&claim_id)
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
        match guard.get(&claim_id) {
            None => Err(ClaimJobMismatch::NotFound),
            Some(c) if c.job_id != expected_job_id => {
                Err(ClaimJobMismatch::WrongJob { actual: c.job_id })
            }
            Some(_) => Ok(guard.remove(&claim_id).expect("just checked present")),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Return every claim whose deadline has passed. The caller must
    /// remove them via `take()` before acting.
    pub fn expired_ids(&self, now: Instant) -> Vec<ClaimId> {
        self.inner
            .read()
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
        let claim = guard.get(&claim_id)?;
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

    /// Snapshot every active claim. Used by the reaper to find claims
    /// tied to a reaped job so they can be evicted immediately instead
    /// of lingering until their deadline.
    pub fn all(&self) -> Vec<Arc<ActiveClaim>> {
        self.inner.read().values().cloned().collect()
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
}
