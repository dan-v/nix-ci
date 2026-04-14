//! The Steps registry: global cross-submission dedup for Steps.
//!
//! Invariant 3 of the 8: `create` is the dedup primitive. If the
//! drv_hash is already present, return the existing `Arc<Step>` with
//! `is_new = false`; the caller attaches its submission edge and
//! moves on.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use crate::types::DrvHash;

use super::step::Step;

pub struct StepsRegistry {
    inner: RwLock<HashMap<DrvHash, Weak<Step>>>,
}

pub enum CreateOutcome {
    New(Arc<Step>),
    Existing(Arc<Step>),
}

impl CreateOutcome {
    pub fn is_new(&self) -> bool {
        matches!(self, Self::New(_))
    }
    pub fn into_step(self) -> Arc<Step> {
        match self {
            Self::New(s) | Self::Existing(s) => s,
        }
    }
}

impl Default for StepsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl StepsRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::with_capacity(1024)),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Look up an existing step. O(1). Returns None if absent or GC'd.
    pub fn get(&self, drv_hash: &DrvHash) -> Option<Arc<Step>> {
        self.inner.read().get(drv_hash).and_then(Weak::upgrade)
    }

    /// Insert or reuse a step keyed by its `drv_hash`. `factory` is
    /// invoked only on miss — the caller constructs the step inside
    /// it so we don't do extra work on the fast path of a duplicate.
    pub fn get_or_create<F>(&self, drv_hash: &DrvHash, factory: F) -> CreateOutcome
    where
        F: FnOnce() -> Arc<Step>,
    {
        // Fast path: read lock + successful upgrade.
        if let Some(existing) = self.inner.read().get(drv_hash).and_then(Weak::upgrade) {
            return CreateOutcome::Existing(existing);
        }
        // Slow path: write lock. Re-check; another task might have won
        // the race.
        let mut guard = self.inner.write();
        if let Some(existing) = guard.get(drv_hash).and_then(Weak::upgrade) {
            return CreateOutcome::Existing(existing);
        }
        let step = factory();
        guard.insert(drv_hash.clone(), Arc::downgrade(&step));
        CreateOutcome::New(step)
    }

    /// Iterate live steps. Write-locks to GC dead weak entries in the
    /// same pass. Used by the snapshot endpoint — NOT on the hot path.
    pub fn live(&self) -> Vec<Arc<Step>> {
        let mut guard = self.inner.write();
        let mut out = Vec::with_capacity(guard.len());
        guard.retain(|_, w| match w.upgrade() {
            Some(s) => {
                out.push(s);
                true
            }
            None => false,
        });
        out
    }
}
