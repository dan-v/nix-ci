//! Active-claim tracking. A claim is the ephemeral lease a worker
//! holds on a specific drv while it's building. Purely in-memory —
//! nothing about in-flight claims is persisted.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::types::{ClaimId, DrvHash, JobId};

pub struct ActiveClaim {
    pub claim_id: ClaimId,
    pub job_id: JobId,
    pub drv_hash: DrvHash,
    pub attempt: i32,
    pub deadline: Instant,
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
