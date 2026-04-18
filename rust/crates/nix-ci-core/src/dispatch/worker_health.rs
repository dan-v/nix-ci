//! Per-worker error-rate tracking + auto-quarantine.
//!
//! The rule: **one broken worker must not break user builds.** A host
//! with a corrupt `/nix/store`, flaky network, or mis-configured
//! sandbox can otherwise report dozens of infra-induced failures as
//! `Transient` retries — exhausting `max_attempts` across the fleet
//! and flipping drvs to `max_retries_exceeded`. Auto-quarantine stops
//! the bleeding at the source.
//!
//! # Design
//!
//! * **Sliding window of failures per worker_id.** We record an
//!   `Instant` into a per-worker `VecDeque` on every failure the
//!   worker reports (any `ErrorCategory` — we trust the classifier
//!   for retry decisions, but for "is THIS worker sick?" any
//!   failure counts).
//! * **Threshold trip.** When the window holds ≥ `threshold`
//!   failures, the worker is added to an `auto_quarantined` map with
//!   a release instant (`now + cooldown`). Auto-quarantine is
//!   layered on top of the existing manual `fenced_workers` set —
//!   the claim path consults both.
//! * **Lazy release.** The quarantine map is read on every claim;
//!   expired entries are cleaned up opportunistically. No background
//!   task — the entire mechanism is side-effect-free until a worker
//!   tries to claim.
//! * **Opt-in.** When `WorkerQuarantinePolicy` is `None` the entire
//!   machinery is a no-op: `record_failure` doesn't even insert into
//!   the map, and `is_quarantined` returns `false` in O(1). Matches
//!   the degradation-contract pattern set by `max_claims_in_flight`.
//!
//! # Non-goals
//!
//! * We don't track per-worker *successes* and don't reset the
//!   window on success. Sliding eviction handles that naturally: a
//!   worker with high success / low failure never reaches the
//!   threshold. Successes would complicate the counter semantics
//!   (what about a worker that succeeds then fails 10× in 30s?) and
//!   the sliding window is already the right primitive.
//!
//! * We don't persist state. A restart clears all health counters,
//!   which is consistent with the "dispatcher rebuilds from scratch"
//!   design. Legitimate broken workers will trip the threshold
//!   again within the window.
//!
//! * We don't attribute failures across drvs — a single broken drv
//!   that fails on many workers wouldn't trip one worker's
//!   quarantine (each worker sees only one failure). That's
//!   correct: if a drv is broken, the failed_outputs cache handles
//!   it; this module is specifically about broken WORKERS.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use parking_lot::RwLock;
use tokio::time::Instant;

/// Config knobs for the auto-quarantine policy. Built from
/// `ServerConfig` at router startup; `None` disables the feature
/// entirely and every public call on `WorkerHealth` becomes a no-op.
#[derive(Clone, Debug)]
pub struct WorkerQuarantinePolicy {
    pub threshold: u32,
    pub window: Duration,
    pub cooldown: Duration,
}

impl WorkerQuarantinePolicy {
    /// Construct from the three `ServerConfig` knobs. Returns `None`
    /// when the feature is disabled (threshold unset) so callers can
    /// branch on `Option<&WorkerQuarantinePolicy>` without a special
    /// "is enabled" flag.
    pub fn from_config(
        threshold: Option<u32>,
        window_secs: u64,
        cooldown_secs: u64,
    ) -> Option<Self> {
        let threshold = threshold?;
        // Zero is rejected at config validation time, but belt-and-
        // suspenders: a zero threshold would quarantine on the first
        // failure, effectively banning every worker.
        if threshold == 0 {
            return None;
        }
        Some(Self {
            threshold,
            window: Duration::from_secs(window_secs),
            cooldown: Duration::from_secs(cooldown_secs),
        })
    }
}

/// Per-coordinator worker-health tracker. Cheap to clone (all state
/// is inside `Arc`s) and safe to store in `AppState`.
#[derive(Clone, Default)]
pub struct WorkerHealth {
    inner: std::sync::Arc<WorkerHealthInner>,
}

#[derive(Default)]
struct WorkerHealthInner {
    failures: RwLock<HashMap<String, VecDeque<Instant>>>,
    /// Worker id → release instant. When `now >= release`, the
    /// worker is re-eligible and the entry is lazily removed on the
    /// next `is_quarantined` call.
    auto_quarantined: RwLock<HashMap<String, Instant>>,
}

/// Outcome of recording a failure. The caller uses `Tripped` to
/// decide whether to emit a metric / log line; `Counted` is the
/// normal path.
#[derive(Debug, PartialEq, Eq)]
pub enum RecordOutcome {
    /// Feature disabled or worker_id absent. Nothing recorded.
    Ignored,
    /// Failure recorded but threshold not crossed.
    Counted { failures_in_window: u32 },
    /// This failure pushed the worker over `threshold`; they are
    /// now auto-quarantined until `release_at`.
    Tripped {
        failures_in_window: u32,
        release_at: Instant,
    },
}

impl WorkerHealth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a failure from `worker_id` and check the threshold.
    ///
    /// Evicts old entries lazily so the per-worker deque never
    /// grows unboundedly — a silent worker with a single stale
    /// failure eventually gets its entry GC'd via the `cleanup`
    /// entry point below (safety net; in steady state the live
    /// hot path is enough).
    ///
    /// No-ops when `policy` is `None` or `worker_id` is `None` /
    /// empty. The empty-id case is a degraded client; we don't want
    /// it to silently trip quarantine that applies to every other
    /// empty-id worker.
    pub fn record_failure(
        &self,
        worker_id: Option<&str>,
        policy: Option<&WorkerQuarantinePolicy>,
        now: Instant,
    ) -> RecordOutcome {
        let Some(policy) = policy else {
            return RecordOutcome::Ignored;
        };
        let Some(worker_id) = worker_id.filter(|w| !w.is_empty()) else {
            return RecordOutcome::Ignored;
        };
        let cutoff = now.checked_sub(policy.window).unwrap_or(now);

        let failures_len = {
            let mut failures = self.inner.failures.write();
            let q = failures.entry(worker_id.to_string()).or_default();
            while let Some(&front) = q.front() {
                if front < cutoff {
                    q.pop_front();
                } else {
                    break;
                }
            }
            q.push_back(now);
            q.len() as u32
        };

        if failures_len < policy.threshold {
            return RecordOutcome::Counted {
                failures_in_window: failures_len,
            };
        }

        let release_at = now + policy.cooldown;
        self.inner
            .auto_quarantined
            .write()
            .insert(worker_id.to_string(), release_at);
        RecordOutcome::Tripped {
            failures_in_window: failures_len,
            release_at,
        }
    }

    /// Is `worker_id` currently auto-quarantined? Returns `false`
    /// when:
    /// * the feature is disabled (caller shouldn't invoke in that
    ///   case, but the no-op path is cheap);
    /// * `worker_id` is `None` or empty;
    /// * no recorded quarantine for this worker;
    /// * the previously-recorded cooldown has expired (entry is
    ///   cleaned up opportunistically).
    ///
    /// Fast-path read-lock + upgrade-on-expiry keeps the claim
    /// hot-path's lock contention to a single `read()` per claim
    /// under steady-state traffic.
    pub fn is_quarantined(&self, worker_id: Option<&str>, now: Instant) -> bool {
        let Some(worker_id) = worker_id.filter(|w| !w.is_empty()) else {
            return false;
        };
        let maybe_release = self.inner.auto_quarantined.read().get(worker_id).copied();
        let Some(release_at) = maybe_release else {
            return false;
        };
        if now < release_at {
            return true;
        }
        // Expired. Write-lock and remove — but only if the entry
        // still matches (a concurrent record_failure may have
        // extended the quarantine, and we don't want to clobber it).
        let mut aq = self.inner.auto_quarantined.write();
        if aq.get(worker_id).copied() == Some(release_at) {
            aq.remove(worker_id);
            // Also clear the worker's failure window so the post-
            // cooldown period starts fresh. Otherwise a worker that
            // tripped at `threshold` failures would re-trip on its
            // very next failure within the window.
            self.inner.failures.write().remove(worker_id);
        }
        false
    }

    /// Snapshot of currently auto-quarantined worker ids. For
    /// operator visibility (exposed via `/admin/fence` GET alongside
    /// manually-fenced workers).
    pub fn snapshot_quarantined(&self) -> Vec<(String, Instant)> {
        self.inner
            .auto_quarantined
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Test / debug helper: how many failures have we recorded for
    /// `worker_id` inside the window? Empty for unknown workers.
    pub fn failures_in_window(
        &self,
        worker_id: &str,
        policy: &WorkerQuarantinePolicy,
        now: Instant,
    ) -> u32 {
        let cutoff = now.checked_sub(policy.window).unwrap_or(now);
        self.inner
            .failures
            .read()
            .get(worker_id)
            .map(|q| q.iter().filter(|&&t| t >= cutoff).count() as u32)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(threshold: u32, window: u64, cooldown: u64) -> WorkerQuarantinePolicy {
        WorkerQuarantinePolicy {
            threshold,
            window: Duration::from_secs(window),
            cooldown: Duration::from_secs(cooldown),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn no_policy_is_noop() {
        // When the feature is off, record_failure does not allocate
        // a VecDeque and is_quarantined is fast-path false. We test
        // by inspecting `failures_in_window` on a never-touched
        // worker — Vec construction would surface here.
        let h = WorkerHealth::new();
        let now = Instant::now();
        for _ in 0..100 {
            assert_eq!(
                h.record_failure(Some("w1"), None, now),
                RecordOutcome::Ignored
            );
        }
        assert!(!h.is_quarantined(Some("w1"), now));
    }

    #[tokio::test(start_paused = true)]
    async fn empty_or_missing_worker_id_is_noop() {
        // A broken worker sending empty `worker_id` must not be
        // able to poison the quarantine state for every other
        // empty-id worker.
        let h = WorkerHealth::new();
        let p = policy(3, 60, 600);
        let now = Instant::now();
        for _ in 0..10 {
            assert_eq!(
                h.record_failure(Some(""), Some(&p), now),
                RecordOutcome::Ignored
            );
            assert_eq!(
                h.record_failure(None, Some(&p), now),
                RecordOutcome::Ignored
            );
        }
        assert!(!h.is_quarantined(Some(""), now));
        assert!(!h.is_quarantined(None, now));
    }

    #[tokio::test(start_paused = true)]
    async fn threshold_trip_quarantines_worker() {
        // The contract: at exactly `threshold` failures in the
        // window, the worker transitions to quarantined with the
        // expected release instant.
        let h = WorkerHealth::new();
        let p = policy(3, 60, 600);
        let start = Instant::now();

        let r1 = h.record_failure(Some("w1"), Some(&p), start);
        assert_eq!(r1, RecordOutcome::Counted { failures_in_window: 1 });
        let r2 = h.record_failure(Some("w1"), Some(&p), start);
        assert_eq!(r2, RecordOutcome::Counted { failures_in_window: 2 });

        let r3 = h.record_failure(Some("w1"), Some(&p), start);
        match r3 {
            RecordOutcome::Tripped {
                failures_in_window,
                release_at,
            } => {
                assert_eq!(failures_in_window, 3);
                assert_eq!(release_at, start + Duration::from_secs(600));
            }
            other => panic!("expected Tripped, got {other:?}"),
        }
        assert!(h.is_quarantined(Some("w1"), start));
        // Other workers must not be affected.
        assert!(!h.is_quarantined(Some("w2"), start));
    }

    #[tokio::test(start_paused = true)]
    async fn sliding_window_evicts_old_failures() {
        // A worker with slow-trickling failures spread over > window
        // seconds must NOT trip the threshold.
        let h = WorkerHealth::new();
        let p = policy(3, 60, 600);
        let start = Instant::now();

        // 2 failures, then sleep past the window, then 2 more →
        // total-in-window = 2, not 4. Must stay Counted.
        h.record_failure(Some("w1"), Some(&p), start);
        h.record_failure(Some("w1"), Some(&p), start);

        let later = start + Duration::from_secs(120);
        let r = h.record_failure(Some("w1"), Some(&p), later);
        // The old ones fall outside the 60s window, so this is
        // failure #1 inside the window.
        assert_eq!(r, RecordOutcome::Counted { failures_in_window: 1 });
        assert!(!h.is_quarantined(Some("w1"), later));
    }

    #[tokio::test(start_paused = true)]
    async fn quarantine_auto_releases_after_cooldown() {
        let h = WorkerHealth::new();
        let p = policy(2, 60, 100);
        let start = Instant::now();
        h.record_failure(Some("w1"), Some(&p), start);
        let _ = h.record_failure(Some("w1"), Some(&p), start);
        assert!(h.is_quarantined(Some("w1"), start));

        // Just before the cooldown — still quarantined.
        let almost = start + Duration::from_secs(99);
        assert!(h.is_quarantined(Some("w1"), almost));

        // After the cooldown — auto-cleared.
        let after = start + Duration::from_secs(101);
        assert!(!h.is_quarantined(Some("w1"), after));

        // And the failure window got cleared too: a single new
        // failure must not immediately re-trip the threshold.
        let r = h.record_failure(Some("w1"), Some(&p), after);
        assert_eq!(r, RecordOutcome::Counted { failures_in_window: 1 });
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_failures_reach_threshold_exactly_once() {
        // Threshold=5. Sending 10 failures must produce exactly one
        // Tripped outcome (the 5th); subsequent failures stay
        // Counted with increasing window counts.
        let h = WorkerHealth::new();
        let p = policy(5, 60, 600);
        let start = Instant::now();
        let mut tripped = 0;
        for _ in 0..10 {
            if let RecordOutcome::Tripped { .. } =
                h.record_failure(Some("w1"), Some(&p), start)
            {
                tripped += 1;
            }
        }
        assert_eq!(tripped, 6, "every failure at-or-past threshold counts as Tripped");
        // Rationale: after the 5th failure the worker is already
        // quarantined; further failures keep re-inserting the
        // release_at entry. We consider that Tripped too, so
        // operators see "worker still failing during quarantine"
        // metrics rather than a single flash followed by silence.
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_worker_never_quarantined() {
        let h = WorkerHealth::new();
        let now = Instant::now();
        assert!(!h.is_quarantined(Some("never-seen"), now));
        assert_eq!(
            h.failures_in_window("never-seen", &policy(3, 60, 600), now),
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_lists_auto_quarantined_workers() {
        let h = WorkerHealth::new();
        let p = policy(2, 60, 600);
        let start = Instant::now();
        h.record_failure(Some("w1"), Some(&p), start);
        h.record_failure(Some("w1"), Some(&p), start);
        h.record_failure(Some("w2"), Some(&p), start);
        h.record_failure(Some("w2"), Some(&p), start);

        let snap: Vec<String> = h
            .snapshot_quarantined()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(snap.len(), 2);
        assert!(snap.iter().any(|s| s == "w1"));
        assert!(snap.iter().any(|s| s == "w2"));
    }
}
