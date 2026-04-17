//! Post-run artifact collection for CI integrations.
//!
//! When `--artifacts-dir` is set, we write failure logs and eval
//! diagnostics to disk so CCI (or any CI system) can pick them up
//! as build artifacts. The operator sees clean runner output on
//! stdout; noisy details are in browseable files.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::client::CoordinatorClient;
use crate::types::{DrvFailure, JobId, JobStatusResponse};

/// Collect failure logs from the coordinator and write them to
/// `<artifacts_dir>/build_logs/<drv_name>.log`. Best-effort: a
/// failed fetch writes a placeholder, never aborts the run.
pub async fn collect_failure_logs(
    client: &Arc<CoordinatorClient>,
    job_id: JobId,
    snap: &JobStatusResponse,
    artifacts_dir: &Path,
) {
    let log_dir = artifacts_dir.join("build_logs");
    if let Err(e) = std::fs::create_dir_all(&log_dir) {
        tracing::warn!(error = %e, "artifacts: failed to create build_logs dir");
        return;
    }

    let originating: Vec<&DrvFailure> = snap
        .failures
        .iter()
        .filter(|f| {
            !matches!(
                f.error_category,
                crate::types::ErrorCategory::PropagatedFailure
            )
        })
        .collect();

    if originating.is_empty() {
        return;
    }

    let mut used_names: HashSet<String> = HashSet::new();
    let mut written = 0u32;

    for failure in &originating {
        let filename = unique_filename(&failure.drv_name, &mut used_names);
        let path = log_dir.join(&filename);

        let content = match fetch_failure_log(client, job_id, failure).await {
            Ok(log) => log,
            Err(msg) => format!("[log not available: {msg}]"),
        };

        match std::fs::write(&path, &content) {
            Ok(()) => {
                written += 1;
                tracing::debug!(drv = %failure.drv_name, path = %path.display(), "artifact: wrote failure log");
            }
            Err(e) => {
                tracing::warn!(error = %e, drv = %failure.drv_name, "artifact: failed to write log");
            }
        }
    }

    if written > 0 {
        tracing::info!(
            count = written,
            dir = %log_dir.display(),
            "artifacts: wrote {} failure log(s)",
            written
        );
    }
}

/// Fetch the most recent attempt's log for a failed drv. Returns the
/// decompressed text or an error message string.
async fn fetch_failure_log(
    client: &Arc<CoordinatorClient>,
    job_id: JobId,
    failure: &DrvFailure,
) -> Result<String, String> {
    let listing = client
        .list_drv_logs(job_id, &failure.drv_hash)
        .await
        .map_err(|e| format!("list_drv_logs: {e}"))?;

    if listing.attempts.is_empty() {
        // No archived log — fall back to the inline tail from the
        // terminal snapshot (if present).
        return failure
            .log_tail
            .clone()
            .ok_or_else(|| "no archived log and no inline log_tail".to_string());
    }

    let newest = &listing.attempts[0];
    client
        .fetch_log(job_id, newest.claim_id)
        .await
        .map_err(|e| format!("fetch_log: {e}"))
}

/// Produce a unique `<drv_name>.log` filename. If `drv_name` was
/// already used (two different hashes, same name — rare but
/// possible), append a numeric suffix.
fn unique_filename(drv_name: &str, used: &mut HashSet<String>) -> String {
    let base = sanitize_filename(drv_name);
    let candidate = format!("{base}.log");
    if used.insert(candidate.clone()) {
        return candidate;
    }
    for i in 2.. {
        let suffixed = format!("{base}-{i}.log");
        if used.insert(suffixed.clone()) {
            return suffixed;
        }
    }
    unreachable!()
}

/// Replace characters that are problematic in filenames and defuse
/// bare `.` / `..` components so `Path::join(base, result)` cannot
/// escape `base`. Nix drv_names are normally alphanumeric + `-` + `.`
/// + `_`, but a hostile flake can declare a derivation whose `name`
/// is `..` / `../../etc/passwd` / similar — left unsanitized, that
/// becomes a traversal when the runner writes
/// `<artifacts_dir>/build_logs/<name>.log`.
fn sanitize_filename(s: &str) -> String {
    let sanitized: String = s
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c => c,
        })
        .collect();
    // Reject standalone `.` / `..` — those would be interpreted as
    // path components by the filesystem even in the absence of `/`
    // separators in the filename itself. Embedded `..` (e.g.
    // `.._.._etc_passwd`) is harmless: it's a single filename with
    // no separators, so `Path::join(base, that)` stays in `base`.
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "_".to_string()
    } else {
        sanitized
    }
}

/// Set up the artifacts directory structure. Call once at the start of
/// a run (before eval) so `eval.stderr` can be opened immediately.
/// Returns the path to the eval stderr file (if artifacts are enabled).
pub fn prepare_artifacts_dir(artifacts_dir: &Path) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(artifacts_dir)?;
    let eval_stderr = artifacts_dir.join("eval.stderr");
    Ok(eval_stderr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_filename_no_collision() {
        let mut used = HashSet::new();
        assert_eq!(unique_filename("hello", &mut used), "hello.log");
        assert_eq!(unique_filename("world", &mut used), "world.log");
    }

    #[test]
    fn unique_filename_collision_adds_counter() {
        let mut used = HashSet::new();
        assert_eq!(unique_filename("foo", &mut used), "foo.log");
        assert_eq!(unique_filename("foo", &mut used), "foo-2.log");
        assert_eq!(unique_filename("foo", &mut used), "foo-3.log");
    }

    #[test]
    fn sanitize_replaces_dangerous_chars() {
        assert_eq!(sanitize_filename("a/b:c*d"), "a_b_c_d");
        assert_eq!(sanitize_filename("normal-name_1.0"), "normal-name_1.0");
    }
}
