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

use crate::client::CoordinatorClient;
use crate::error::{Error, Result};
use crate::runner::drv_parser::ParsedDrv;
use crate::runner::drv_walk;
use crate::runner::eval_jobs::EvalLine;
use crate::types::{IngestBatchRequest, JobId};

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

pub async fn run(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    mut eval_rx: Receiver<EvalLine>,
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

    while let Some(line) = eval_rx.recv().await {
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
        let walked = match walk_off_runtime(parsed_cache.clone(), root_drv.clone(), needed_set)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, drv = %root_drv, "walk_filtered failed; skipping root");
                stats.errors += 1;
                continue;
            }
        };

        let drvs: Vec<_> = walked
            .into_iter()
            .filter(|w| submitted.insert(w.drv_path.clone()))
            .map(drv_walk::WalkedDrv::into_request)
            .collect();
        if drvs.is_empty() {
            continue;
        }

        let count = drvs.len();
        match client
            .ingest_batch(job_id, &IngestBatchRequest { drvs })
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
    order: VecDeque<String>,
    cap: usize,
}

impl BoundedCache {
    fn new(cap: usize) -> Self {
        Self {
            inner: HashMap::with_capacity(cap.min(1024)),
            order: VecDeque::with_capacity(cap.min(1024)),
            cap,
        }
    }

    fn as_hashmap_mut(&mut self) -> &mut HashMap<String, ParsedDrv> {
        // Before exposing, record any fresh insertions from a previous
        // call. The order may be out of sync if the walker inserted
        // entries we haven't seen yet; we reconcile on each borrow.
        for key in self.inner.keys() {
            if !self.order.iter().any(|k| k == key) {
                self.order.push_back(key.clone());
            }
        }
        &mut self.inner
    }

    /// Evict oldest entries until under cap. Called after each walk.
    fn evict_to_cap(&mut self) {
        // Rebuild order from current keys to account for walker insertions.
        for key in self.inner.keys() {
            if !self.order.iter().any(|k| k == key) {
                self.order.push_back(key.clone());
            }
        }
        // Prune order entries that no longer exist in the map.
        self.order.retain(|k| self.inner.contains_key(k));
        while self.inner.len() > self.cap {
            let Some(key) = self.order.pop_front() else {
                break;
            };
            self.inner.remove(&key);
        }
    }
}
