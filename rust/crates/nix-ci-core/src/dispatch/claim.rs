//! Active-claim tracking. A claim is the ephemeral lease a worker
//! holds on a specific drv while it's building. Purely in-memory —
//! nothing about in-flight claims is persisted.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::types::{ClaimId, DrvHash, JobId};

#[derive(Clone)]
pub struct ActiveClaim {
    pub claim_id: ClaimId,
    pub job_id: JobId,
    pub drv_hash: DrvHash,
    pub attempt: i32,
    pub deadline: Instant,
    /// When the claim was issued. Used by the `Progress` event to
    /// report "currently building X (3m 12s)" — derive elapsed via
    /// `Instant::now() - started_at`. Wall-clock millis are computed
    /// on demand at event-emission time.
    pub started_at: Instant,
}

pub struct Claims {
    inner: RwLock<HashMap<ClaimId, Arc<ActiveClaim>>>,
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
            .filter(|c| c.deadline <= now)
            .map(|c| c.claim_id)
            .collect()
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
            deadline,
            started_at: Instant::now(),
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
}
