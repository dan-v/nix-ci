//! `Step` — the in-memory node for a single derivation. In-flight
//! state (runnable / finished / tries / previous_failure / etc.) lives
//! entirely here; nothing is persisted.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use crate::types::{DrvHash, DrvState};

use super::submission::Submission;

/// Monotonic step state. Retries bump `tries` but don't clear
/// `finished`; a step only becomes `finished` when terminal.
pub struct Step {
    // Immutable identity
    drv_hash: DrvHash,
    drv_path: String,
    drv_name: String,
    system: String,
    required_features: Vec<String>,
    max_tries: i32,

    // One-way latches (AcqRel semantics)
    pub runnable: AtomicBool,
    pub finished: AtomicBool,
    pub previous_failure: AtomicBool,
    pub created: AtomicBool,

    // Retry bookkeeping
    pub tries: AtomicI32,
    /// Unix millis; 0 means no restriction.
    pub next_attempt_at: AtomicI64,

    // Graph edges / submission edges. See the 8 invariants for the
    // locking contract.
    pub state: RwLock<StepState>,
}

pub struct StepState {
    /// Deps we depend on; strong refs keep them alive while we're
    /// waiting. Removed on completion via `make_rdeps_runnable`.
    pub deps: HashSet<StepHandle>,
    /// Dependents: weak — they hold us via `deps`.
    pub rdeps: Vec<Weak<Step>>,
    /// Which submissions reference this step. Weak — submissions own
    /// us via `toplevels`.
    pub submissions: Vec<Weak<Submission>>,
}

impl StepState {
    /// Idempotently attach a submission Weak ref. Returns true if
    /// newly added.
    pub fn attach_submission(&mut self, sub: &Arc<Submission>) -> bool {
        let already = self
            .submissions
            .iter()
            .any(|w| w.upgrade().map(|s| s.id == sub.id).unwrap_or(false));
        if already {
            false
        } else {
            self.submissions.push(Arc::downgrade(sub));
            true
        }
    }
}

/// Hashable wrapper so `Arc<Step>` can live in a `HashSet` keyed on
/// `drv_hash`.
#[derive(Clone)]
pub struct StepHandle(pub Arc<Step>);

impl std::hash::Hash for StepHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.drv_hash.hash(state);
    }
}
impl PartialEq for StepHandle {
    fn eq(&self, other: &Self) -> bool {
        self.0.drv_hash == other.0.drv_hash
    }
}
impl Eq for StepHandle {}
impl std::ops::Deref for StepHandle {
    type Target = Arc<Step>;
    fn deref(&self) -> &Arc<Step> {
        &self.0
    }
}

impl Step {
    pub fn new(
        drv_hash: DrvHash,
        drv_path: String,
        drv_name: String,
        system: String,
        required_features: Vec<String>,
        max_tries: i32,
    ) -> Arc<Self> {
        Arc::new(Self {
            drv_hash,
            drv_path,
            drv_name,
            system,
            required_features,
            max_tries,
            runnable: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            previous_failure: AtomicBool::new(false),
            created: AtomicBool::new(false),
            tries: AtomicI32::new(0),
            next_attempt_at: AtomicI64::new(0),
            state: RwLock::new(StepState {
                deps: HashSet::new(),
                rdeps: Vec::new(),
                submissions: Vec::new(),
            }),
        })
    }

    pub fn drv_hash(&self) -> &DrvHash {
        &self.drv_hash
    }

    pub fn drv_path(&self) -> &str {
        &self.drv_path
    }

    pub fn drv_name(&self) -> &str {
        &self.drv_name
    }

    pub fn system(&self) -> &str {
        &self.system
    }

    pub fn required_features(&self) -> &[String] {
        &self.required_features
    }

    pub fn max_tries(&self) -> i32 {
        self.max_tries
    }

    /// Observable state derived from the atomic flags. Does NOT lock
    /// `state`. Suitable for gauges and snapshots.
    pub fn observable_state(&self) -> DrvState {
        if self.finished.load(Ordering::Acquire) {
            if self.previous_failure.load(Ordering::Acquire) {
                DrvState::Failed
            } else {
                DrvState::Done
            }
        } else if self.runnable.load(Ordering::Acquire) {
            DrvState::Pending
        } else if self.tries.load(Ordering::Acquire) > 0 {
            DrvState::Building
        } else {
            DrvState::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(name: &str) -> Arc<Step> {
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
    #[allow(clippy::mutable_key_type)] // StepHandle contains Arc<Step> with atomics, but is only keyed on drv_hash
    fn step_handle_hashset_dedupes_by_drv_hash() {
        // StepHandle lives inside StepState::deps as a HashSet. If
        // PartialEq or Hash misbehaves, two attach_dep calls with the
        // same dep would leave stale duplicates in the set, OR two
        // distinct deps would collide and one would be dropped.
        let a = mk("alpha");
        let b = mk("beta");
        let mut set = HashSet::<StepHandle>::new();
        assert!(set.insert(StepHandle(a.clone())));
        assert!(
            !set.insert(StepHandle(a.clone())),
            "duplicate alpha must not insert"
        );
        assert!(
            set.insert(StepHandle(b.clone())),
            "distinct beta must insert"
        );
        assert_eq!(set.len(), 2, "set must retain two distinct handles");
        // Explicit hash-equality check: two distinct drv_hashes must
        // produce distinct hashes (a constant-0 hash would still allow
        // HashSet to dedup by eq, so the lens-len check above isn't
        // enough — probe the hasher directly).
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut ha = DefaultHasher::new();
        StepHandle(a).hash(&mut ha);
        let mut hb = DefaultHasher::new();
        StepHandle(b).hash(&mut hb);
        assert_ne!(
            ha.finish(),
            hb.finish(),
            "distinct drv_hashes must produce distinct hashes"
        );
    }

    #[test]
    fn step_handle_equality_matches_drv_hash_only() {
        // Two Arc<Step> with the same drv_hash must compare equal even
        // though their memory addresses differ. The registry never
        // actually does this (it returns the existing Arc), but the
        // StepHandle contract is "equal iff drv_hash equal".
        let a = mk("same");
        let b = mk("same");
        assert!(StepHandle(a) == StepHandle(b));
    }
}
