//! Walk a derivation's closure by reading `.drv` files directly from
//! the Nix store and emit a filtered list of [`IngestDrvRequest`]s the
//! submitter can batch-POST.
//!
//! ATerm parsing is done by [`super::drv_parser`]; this module is just
//! the iterative DFS over the drv graph plus filtering.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use super::drv_parser::{self, ParsedDrv};
use crate::types::IngestDrvRequest;

/// One derivation emitted to the coordinator, with its direct drv-level
/// deps filtered to only other members of the submission's closure.
pub struct WalkedDrv {
    pub drv_path: String,
    pub drv_name: String,
    pub system: String,
    /// Direct deps, filtered to only include drvs that are also in the
    /// walk's `needed_set`. Cached deps are omitted — the worker's
    /// `nix build` substitutes them inline, no coordination needed.
    pub input_drvs: Vec<String>,
    pub required_features: Vec<String>,
    /// True if this drv is one of the original root drvs.
    pub is_root: bool,
}

/// Walk the transitive .drv closure of `roots`, apply the `needed_set`
/// filter (union of `neededBuilds` across all top-level attrs from
/// nix-eval-jobs), and return every drv in the intersection along with
/// its filtered edges.
///
/// `parsed_cache` is reused across calls so overlapping closures read
/// each `.drv` file exactly once — critical for nixpkgs-scale ingest
/// where dozens of attrs share stdenv + toolchain.
pub fn walk_filtered(
    roots: &[String],
    needed_set: &HashSet<String>,
    parsed_cache: &mut HashMap<String, ParsedDrv>,
) -> Result<Vec<WalkedDrv>> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = roots.to_vec();
    let mut fod_paths: HashSet<String> = HashSet::new();

    while let Some(drv_path) = stack.pop() {
        if !visited.insert(drv_path.clone()) {
            continue;
        }
        if !parsed_cache.contains_key(&drv_path) {
            let bytes = match std::fs::read(&drv_path) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        drv = %drv_path,
                        error = %e,
                        "cannot read .drv file; skipping"
                    );
                    continue;
                }
            };
            match drv_parser::parse(&bytes) {
                Ok(parsed) => {
                    parsed_cache.insert(drv_path.clone(), parsed);
                }
                Err(e) => {
                    tracing::warn!(
                        drv = %drv_path,
                        error = %e,
                        "cannot parse .drv file; skipping"
                    );
                    continue;
                }
            }
        }
        let parsed = &parsed_cache[&drv_path];
        if parsed.is_fod() {
            fod_paths.insert(drv_path.clone());
            continue;
        }
        for (dep_path, _) in &parsed.input_drvs {
            if !visited.contains(dep_path) {
                stack.push(dep_path.clone());
            }
        }
    }

    let root_set: HashSet<&str> = roots.iter().map(|s| s.as_str()).collect();

    let mut out: Vec<WalkedDrv> = Vec::with_capacity(needed_set.len());
    for drv_path in &visited {
        if fod_paths.contains(drv_path) {
            continue;
        }
        if !needed_set.contains(drv_path) {
            continue;
        }
        let Some(parsed) = parsed_cache.get(drv_path) else {
            continue;
        };

        // Edges: keep only inputs in the needed set (and not FODs).
        let input_drvs: Vec<String> = parsed
            .input_drvs
            .iter()
            .filter_map(|(dep, _outs)| {
                if !fod_paths.contains(dep) && needed_set.contains(dep) {
                    Some(dep.clone())
                } else {
                    None
                }
            })
            .collect();

        out.push(WalkedDrv {
            drv_path: drv_path.clone(),
            drv_name: parsed.name.clone(),
            system: parsed.system.clone(),
            input_drvs,
            required_features: Vec::new(),
            is_root: root_set.contains(drv_path.as_str()),
        });
    }

    // Deterministic order: sort by drv_path so the submitter's batch
    // payload is reproducible across runs. Helpful for log diffing
    // and for tests that snapshot submissions.
    out.sort_by(|a, b| a.drv_path.cmp(&b.drv_path));

    tracing::debug!(
        roots = roots.len(),
        needed = needed_set.len(),
        walked = out.len(),
        fod_skipped = fod_paths.len(),
        "walk_filtered complete"
    );

    Ok(out)
}

/// Convenience: turn a [`WalkedDrv`] into the wire form the server
/// accepts on the batch-ingest endpoint.
impl WalkedDrv {
    pub fn into_request(self) -> IngestDrvRequest {
        IngestDrvRequest {
            drv_path: self.drv_path,
            drv_name: self.drv_name,
            system: self.system,
            required_features: self.required_features,
            input_drvs: self.input_drvs,
            is_root: self.is_root,
            cache_status: None,
        }
    }
}
