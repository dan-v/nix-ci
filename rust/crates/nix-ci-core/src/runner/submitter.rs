//! Streaming submitter with full-DAG ingest.
//!
//! For each line emitted by nix-eval-jobs, walk the .drv closure
//! (shared cache across lines) filtered to the union of `neededBuilds`
//! across all emitted attrs. Batch-POST the result to the coordinator's
//! `/drvs/batch` endpoint. Each drv in the batch carries its
//! drv-level deps, so the dispatcher's full sequencing invariants
//! (make_rdeps_runnable, cross-job dedup on every uncached internal
//! drv) all apply.
//!
//! The closure walk is wrapped in `spawn_blocking` so the synchronous
//! filesystem reads don't starve the async runtime.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;

use crate::client::CoordinatorClient;
use crate::error::{Error, Result};
use crate::runner::drv_parser::ParsedDrv;
use crate::runner::drv_walk;
use crate::runner::eval_jobs::EvalLine;
use crate::types::{EvalError, IngestBatchRequest, JobId};

pub struct SubmitStats {
    pub new_drvs: u64,
    pub dedup_skipped: u64,
    pub cached_skipped: u64,
    /// Per-attribute eval errors reported by nix-eval-jobs. These do
    /// not abort the submission — a single broken attr in a large
    /// evaluation shouldn't poison every other attr. The job's overall
    /// fate is decided by the build outcomes of the attrs that *did*
    /// evaluate.
    pub eval_errors: u64,
    /// Infrastructure / transport errors from the submitter itself
    /// (batch POST failed, walker failed, etc).
    pub errors: u64,
}

#[tracing::instrument(skip_all, fields(job_id = %job_id))]
pub async fn run(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    mut eval_rx: Receiver<EvalLine>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<SubmitStats> {
    let mut stats = SubmitStats {
        new_drvs: 0,
        dedup_skipped: 0,
        cached_skipped: 0,
        eval_errors: 0,
        errors: 0,
    };
    // Shared across spawn_blocking invocations: stdenv / bash / glibc
    // parse once per run. Bounded (see `PARSED_CACHE_CAP`) so a huge
    // submission with many root closures can't monotonically grow
    // memory.
    let parsed_cache = Arc::new(Mutex::new(BoundedCache::new(PARSED_CACHE_CAP)));
    // Dedup what we've already ingested so overlapping closures don't
    // re-POST to the coordinator.
    let mut submitted: HashSet<String> = HashSet::new();
    // Buffered eval errors: piggyback on the next ingest_batch call so
    // the coordinator records them alongside regular drv traffic. On
    // end-of-eval with leftover buffer (no more drvs coming), flush
    // via a final drv-less batch.
    let mut eval_error_buf: Vec<EvalError> = Vec::new();

    loop {
        let line = tokio::select! {
            maybe = eval_rx.recv() => match maybe {
                Some(l) => l,
                None => break,
            },
            // Shutdown flipped: the job is gone (cancel / reaper / SSE
            // saw JobDone). Stop consuming the eval stream — further
            // ingest POSTs would 410 and waste work. The eval kill
            // guard in the orchestrator handles the nix-eval-jobs child.
            _ = shutdown.changed() => break,
        };
        // Per-attribute eval errors are NOT fatal for the submission.
        // nix-eval-jobs emits them inline ({"attr":"broken","error":
        // "…"}) and keeps going; we do the same. The attr is skipped
        // (no drvPath to ingest), other attrs continue. The user sees
        // eval errors in submitter stats and in the tracing output.
        if let Some(err) = &line.error {
            stats.eval_errors += 1;
            let attr = line
                .attr
                .as_deref()
                .or(line.name.as_deref())
                .unwrap_or("<unknown>");
            tracing::warn!(%attr, eval_error = %err, "nix-eval-jobs: attr failed; continuing");
            eval_error_buf.push(EvalError {
                attr: attr.to_string(),
                error: err.clone(),
            });
            continue;
        }
        let Some(root_drv) = line.drv_path.clone() else {
            continue;
        };

        // Cache hint: top-level attr already substitutable → nothing
        // in its closure needs building, skip entirely.
        if attr_is_cached(&line) {
            stats.cached_skipped += 1;
            continue;
        }

        let needed_set = build_needed_set(&line, &root_drv);
        let walked = walk_off_runtime(parsed_cache.clone(), root_drv.clone(), needed_set)
            .await
            .map_err(|e| {
                // Closure walk is CCI infrastructure, not user input.
                // A missing / unparseable .drv means the submission
                // cannot be honored — fail the whole run with a clear
                // cause rather than silently dropping an attr (which
                // would seal the job as Done with incomplete ingest
                // and the user would never know).
                Error::Internal(format!(
                    "drv closure walk failed for root {root_drv}: {e}"
                ))
            })?;

        let mut drvs: Vec<_> = walked
            .into_iter()
            .filter(|w| submitted.insert(w.drv_path.clone()))
            .map(drv_walk::WalkedDrv::into_request)
            .collect();
        // Attach the human-readable attr name to the root drv so the
        // server can later say "FAILED gcc-13.2.0, used by:
        // packages.x86_64-linux.hello" instead of just the drv_name.
        // Only the root from THIS eval line gets it — transitive deps
        // don't have a direct attr identity.
        if let Some(attr_name) = line.attr.as_deref() {
            for d in drvs.iter_mut() {
                if d.is_root && d.drv_path == root_drv {
                    d.attr = Some(attr_name.to_string());
                }
            }
        }
        if drvs.is_empty() {
            continue;
        }

        let count = drvs.len();
        let eval_errors = std::mem::take(&mut eval_error_buf);
        match client
            .ingest_batch(
                job_id,
                &IngestBatchRequest { drvs, eval_errors },
            )
            .await
        {
            Ok(resp) => {
                stats.new_drvs += resp.new_drvs as u64;
                stats.dedup_skipped += resp.dedup_skipped as u64;
                stats.errors += resp.errored as u64;
                tracing::debug!(
                    root = %root_drv,
                    drvs_in_batch = count,
                    new = resp.new_drvs,
                    deduped = resp.dedup_skipped,
                    "batch ingested"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, root = %root_drv, drvs = count, "batch ingest failed");
                stats.errors += count as u64;
            }
        }
    }
    // End-of-stream: if eval emitted errors in its final stretch after
    // the last drv-bearing line (or emitted nothing but errors), those
    // are still sitting in the buffer. Flush with a drv-less ingest
    // batch so the coordinator records them on the submission before
    // seal.
    if !eval_error_buf.is_empty() {
        let errs = std::mem::take(&mut eval_error_buf);
        if let Err(e) = client
            .ingest_batch(
                job_id,
                &IngestBatchRequest {
                    drvs: Vec::new(),
                    eval_errors: errs,
                },
            )
            .await
        {
            tracing::warn!(error = %e, "eval-errors flush failed");
            stats.errors += 1;
        }
    }
    Ok(stats)
}

/// Whether a top-level attr's `cacheStatus` indicates it's already
/// substitutable. Accepts nix-eval-jobs 2.x string form and legacy
/// boolean form.
fn attr_is_cached(line: &EvalLine) -> bool {
    matches!(
        &line.cache_status,
        Some(serde_json::Value::String(s)) if s == "cached"
    ) || line.is_cached == Some(true)
}

/// Compute the filter set for `walk_filtered` from `neededBuilds`. If
/// nix-eval-jobs didn't populate it (older release), fall back to
/// seeding with the root — the walker will traverse the full closure
/// but only emit drvs we mark as "needed."
fn build_needed_set(line: &EvalLine, root: &str) -> HashSet<String> {
    let mut needed: HashSet<String> = line.needed_builds.iter().cloned().collect();
    needed.insert(root.to_string());
    needed
}

/// Run `walk_filtered` on the blocking thread pool: the synchronous
/// `std::fs::read` calls inside it would otherwise starve the async
/// runtime.
async fn walk_off_runtime(
    cache: Arc<Mutex<BoundedCache>>,
    root: String,
    needed: HashSet<String>,
) -> Result<Vec<drv_walk::WalkedDrv>> {
    tokio::task::spawn_blocking(move || {
        let mut guard = cache.lock();
        drv_walk::walk_filtered(std::slice::from_ref(&root), &needed, guard.as_hashmap_mut())
            .map_err(|e| Error::Internal(format!("walk_filtered: {e}")))
            .inspect(|_| guard.evict_to_cap())
    })
    .await
    .map_err(|e| Error::Internal(format!("walk spawn_blocking: {e}")))?
}

/// Cap on parsed-.drv entries across a run. At 50k entries × ~1 KB
/// ParsedDrv each = ~50 MB — bounded and safe. Overflow evicts the
/// oldest entries.
const PARSED_CACHE_CAP: usize = 50_000;

/// Simple FIFO-evicting wrapper so the parsed-drv cache can't grow
/// without bound. `walk_filtered` mutates the inner HashMap directly;
/// we rebuild the eviction order from the insertion log afterward.
struct BoundedCache {
    inner: HashMap<String, ParsedDrv>,
    /// Insertion order — front is oldest. The walker can insert into
    /// `inner` without going through `as_hashmap_mut`, so we reconcile
    /// `order` (and `seen`) lazily on every borrow + every evict.
    order: VecDeque<String>,
    /// Mirrors the contents of `order` for O(1) "is this key tracked?"
    /// — without it, the per-borrow reconcile is O(n²) because each
    /// inner key linearly scans the deque.
    seen: HashSet<String>,
    cap: usize,
}

impl BoundedCache {
    fn new(cap: usize) -> Self {
        Self {
            inner: HashMap::with_capacity(cap.min(1024)),
            order: VecDeque::with_capacity(cap.min(1024)),
            seen: HashSet::with_capacity(cap.min(1024)),
            cap,
        }
    }

    /// Bring `order` + `seen` in sync with whatever the walker pushed
    /// into `inner` since the last reconcile. O(n) in new entries.
    fn reconcile(&mut self) {
        for key in self.inner.keys() {
            if self.seen.insert(key.clone()) {
                self.order.push_back(key.clone());
            }
        }
    }

    fn as_hashmap_mut(&mut self) -> &mut HashMap<String, ParsedDrv> {
        self.reconcile();
        &mut self.inner
    }

    /// Evict oldest entries until under cap. Called after each walk.
    fn evict_to_cap(&mut self) {
        self.reconcile();
        // Drop entries that the caller already removed from `inner`.
        let still_present = &self.inner;
        let seen = &mut self.seen;
        self.order.retain(|k| {
            let keep = still_present.contains_key(k);
            if !keep {
                seen.remove(k);
            }
            keep
        });
        while self.inner.len() > self.cap {
            let Some(key) = self.order.pop_front() else {
                break;
            };
            self.inner.remove(&key);
            self.seen.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── attr_is_cached: accepts both cacheStatus shapes ─────────

    #[test]
    fn attr_is_cached_with_string_cached() {
        let line = EvalLine {
            cache_status: Some(serde_json::Value::String("cached".into())),
            ..default_line()
        };
        assert!(attr_is_cached(&line));
    }

    #[test]
    fn attr_is_cached_with_string_notbuilt_is_false() {
        let line = EvalLine {
            cache_status: Some(serde_json::Value::String("notBuilt".into())),
            ..default_line()
        };
        assert!(!attr_is_cached(&line));
    }

    #[test]
    fn attr_is_cached_legacy_boolean_true() {
        let line = EvalLine {
            is_cached: Some(true),
            ..default_line()
        };
        assert!(attr_is_cached(&line));
    }

    #[test]
    fn attr_is_cached_legacy_boolean_false() {
        let line = EvalLine {
            is_cached: Some(false),
            ..default_line()
        };
        assert!(!attr_is_cached(&line));
    }

    #[test]
    fn attr_is_cached_absent_fields_is_false() {
        let line = default_line();
        assert!(!attr_is_cached(&line));
    }

    // ─── build_needed_set: always includes the root ──────────────

    #[test]
    fn build_needed_set_includes_root_even_when_empty() {
        let mut line = default_line();
        line.needed_builds = vec![];
        let set = build_needed_set(&line, "/nix/store/r-root.drv");
        assert_eq!(set.len(), 1);
        assert!(set.contains("/nix/store/r-root.drv"));
    }

    #[test]
    fn build_needed_set_unions_needed_builds_with_root() {
        let mut line = default_line();
        line.needed_builds = vec!["/nix/store/a-a.drv".into(), "/nix/store/b-b.drv".into()];
        let set = build_needed_set(&line, "/nix/store/r-root.drv");
        assert_eq!(set.len(), 3);
        assert!(set.contains("/nix/store/r-root.drv"));
        assert!(set.contains("/nix/store/a-a.drv"));
        assert!(set.contains("/nix/store/b-b.drv"));
    }

    #[test]
    fn build_needed_set_root_in_needed_no_duplicate() {
        let mut line = default_line();
        line.needed_builds = vec!["/nix/store/r-root.drv".into()];
        let set = build_needed_set(&line, "/nix/store/r-root.drv");
        assert_eq!(set.len(), 1);
    }

    // ─── BoundedCache FIFO eviction ──────────────────────────────

    #[test]
    fn bounded_cache_keeps_only_cap_entries() {
        let mut c = BoundedCache::new(3);
        c.as_hashmap_mut().insert("a".into(), mk_parsed("a"));
        c.evict_to_cap();
        c.as_hashmap_mut().insert("b".into(), mk_parsed("b"));
        c.evict_to_cap();
        c.as_hashmap_mut().insert("c".into(), mk_parsed("c"));
        c.evict_to_cap();
        assert_eq!(c.inner.len(), 3);
        // Overflow: d pushes the oldest ("a") out.
        c.as_hashmap_mut().insert("d".into(), mk_parsed("d"));
        c.evict_to_cap();
        assert_eq!(c.inner.len(), 3);
        assert!(!c.inner.contains_key("a"));
        assert!(c.inner.contains_key("b"));
        assert!(c.inner.contains_key("c"));
        assert!(c.inner.contains_key("d"));
    }

    #[test]
    fn bounded_cache_zero_cap_evicts_everything() {
        let mut c = BoundedCache::new(0);
        c.as_hashmap_mut().insert("a".into(), mk_parsed("a"));
        c.evict_to_cap();
        assert_eq!(c.inner.len(), 0);
    }

    #[test]
    fn bounded_cache_reconcile_is_linear_at_scale() {
        // The cache is borrowed every walk; with cap=50_000 a quadratic
        // dedup is fatal. Insert 5_000 entries (well below cap, no
        // eviction) and reconcile — must complete in well under a
        // second. An O(n²) scan would be ~25M VecDeque comparisons,
        // closer to multiple seconds in debug mode.
        let mut c = BoundedCache::new(50_000);
        let n = 20_000usize;
        for i in 0..n {
            c.inner.insert(format!("k-{i}"), mk_parsed("x"));
        }
        let start = std::time::Instant::now();
        let _ = c.as_hashmap_mut();
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "as_hashmap_mut on {n} entries took {elapsed:?} — should be O(n), not O(n²)"
        );
        assert_eq!(c.order.len(), n, "every key must end up in order");
    }

    // ─── helpers ─────────────────────────────────────────────────

    fn default_line() -> EvalLine {
        EvalLine {
            drv_path: None,
            attr: None,
            name: None,
            error: None,
            cache_status: None,
            is_cached: None,
            needed_builds: Vec::new(),
        }
    }

    fn mk_parsed(name: &str) -> ParsedDrv {
        ParsedDrv {
            name: name.to_string(),
            system: "x86_64-linux".into(),
            input_drvs: Vec::new(),
            outputs: Vec::new(),
        }
    }
}
