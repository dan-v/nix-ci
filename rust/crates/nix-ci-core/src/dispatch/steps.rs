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
    /// Returns `(step, is_new)`; `is_new` is true iff this call minted
    /// the step (vs. reused an existing Arc).
    pub fn get_or_create<F>(&self, drv_hash: &DrvHash, factory: F) -> (Arc<Step>, bool)
    where
        F: FnOnce() -> Arc<Step>,
    {
        // Fast path: read lock + successful upgrade.
        if let Some(existing) = self.inner.read().get(drv_hash).and_then(Weak::upgrade) {
            return (existing, false);
        }
        // Slow path: write lock. Re-check; another task might have won
        // the race.
        let mut guard = self.inner.write();
        if let Some(existing) = guard.get(drv_hash).and_then(Weak::upgrade) {
            return (existing, false);
        }
        let step = factory();
        guard.insert(drv_hash.clone(), Arc::downgrade(&step));
        (step, true)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_step(name: &str) -> Arc<Step> {
        let hash = DrvHash::new(format!("{name}.drv"));
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
    fn len_and_is_empty_track_inserts_linearly() {
        let reg = StepsRegistry::new();
        assert_eq!(reg.len(), 0);
        assert!(reg.is_empty());
        let (_s1, _) = reg.get_or_create(&DrvHash::new("a.drv"), || mk_step("a"));
        assert_eq!(reg.len(), 1);
        let (_s2, _) = reg.get_or_create(&DrvHash::new("b.drv"), || mk_step("b"));
        assert_eq!(reg.len(), 2);
        let (_s3, _) = reg.get_or_create(&DrvHash::new("c.drv"), || mk_step("c"));
        assert_eq!(reg.len(), 3);
        // Dedup insert does not grow len.
        let (_dup, is_new) = reg.get_or_create(&DrvHash::new("a.drv"), || mk_step("a"));
        assert!(!is_new);
        assert_eq!(reg.len(), 3);
        assert!(!reg.is_empty());
    }

    #[test]
    fn is_empty_after_weak_refs_drop() {
        let reg = StepsRegistry::new();
        {
            let (_step, _) = reg.get_or_create(&DrvHash::new("b.drv"), || mk_step("b"));
            // `_step` dropped at end of scope — registry holds only a
            // Weak, so the Arc is gone.
        }
        // `len()` still counts the weak entry; `live()` GCs it.
        assert_eq!(reg.len(), 1);
        let live = reg.live();
        assert!(live.is_empty());
        assert!(reg.is_empty());
    }
}
