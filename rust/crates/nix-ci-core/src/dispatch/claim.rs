//! Active-claim tracking. A claim is the ephemeral lease a worker
//! holds on a specific drv while it's building. Purely in-memory —
//! nothing about in-flight claims is persisted.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

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

    /// Extend the deadline of an active claim to `now + deadline_window`.
    /// Returns the new deadline if the claim was found, `None` otherwise
    /// (claim already expired / completed / evicted). The caller uses
    /// `None` as the signal to stop sending extensions.
    pub fn extend(&self, claim_id: ClaimId, now: Instant) -> Option<Instant> {
        let guard = self.inner.read();
        let claim = guard.get(&claim_id)?;
        let new_deadline = now + claim.deadline_window;
        *claim.deadline.lock() = new_deadline;
        Some(new_deadline)
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
}
