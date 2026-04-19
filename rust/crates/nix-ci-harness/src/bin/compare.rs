//! nix-ci-compare: diff two `JobStatusResponse` JSONs and emit a
//! shadow-mode divergence report.
//!
//! Usage:
//!   nix-ci-compare primary.json shadow.json
//!   nix-ci-compare --primary <p> --shadow <s> [--output report.json]
//!
//! Designed to be piped from CCI post-build hooks:
//!
//!     curl $CCI/jobs/$ID > primary.json
//!     curl $SHADOW_COORD/jobs/$ID > shadow.json
//!     nix-ci-compare primary.json shadow.json | tee divergence.json
//!
//! Exit codes:
//!   0 — matched (no divergence)
//!   1 — minor divergence (counts differ; status matches)
//!   2 — major divergence (failures differ)
//!   3 — critical divergence (terminal status mismatch)
//!   4 — input parse error

use std::path::PathBuf;

use clap::Parser;
use nix_ci_core::types::{ErrorCategory, JobStatus, JobStatusResponse};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(name = "nix-ci-compare", about = "Diff two JobStatusResponse JSONs")]
struct Args {
    /// Primary (incumbent CI) outcome JSON.
    #[arg(long)]
    primary: Option<PathBuf>,

    /// Shadow (nix-ci) outcome JSON.
    #[arg(long)]
    shadow: Option<PathBuf>,

    /// Output divergence report to this path; omit for stdout.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Positional fallback — if neither --primary nor --shadow are
    /// given, accept `primary.json shadow.json` directly.
    positional: Vec<PathBuf>,
}

#[derive(Debug, Serialize)]
struct Report {
    matched: bool,
    severity: Severity,
    /// Terminal-status comparison.
    status: StatusCompare,
    /// Count deltas (shadow - primary).
    counts_delta: CountsDelta,
    /// Drvs failed in primary but not in shadow (shadow says they passed or never ran).
    failures_only_in_primary: Vec<FailureRef>,
    /// Drvs failed in shadow but not in primary.
    failures_only_in_shadow: Vec<FailureRef>,
    /// Drvs failed in both but with different error categories.
    failure_category_mismatches: Vec<CategoryMismatch>,
    /// Suspected-infra attribution from each run (when non-null).
    /// A `shadow`-only flag is the most useful shape: nix-ci detected
    /// infra the primary didn't classify.
    suspected_worker_infra: InfraCompare,
    /// Human-readable summary of the top divergences.
    summary: Vec<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Severity {
    Matched,
    Minor,
    Major,
    Critical,
}

#[derive(Debug, Serialize)]
struct StatusCompare {
    primary: JobStatus,
    shadow: JobStatus,
    mismatch: bool,
}

#[derive(Debug, Serialize)]
struct CountsDelta {
    total: i64,
    pending: i64,
    building: i64,
    done: i64,
    failed: i64,
}

#[derive(Debug, Serialize)]
struct FailureRef {
    drv_hash: String,
    drv_name: String,
    category: ErrorCategory,
}

#[derive(Debug, Serialize)]
struct CategoryMismatch {
    drv_hash: String,
    drv_name: String,
    primary_category: ErrorCategory,
    shadow_category: ErrorCategory,
}

#[derive(Debug, Serialize)]
struct InfraCompare {
    primary: Option<String>,
    shadow: Option<String>,
}

fn load(path: &PathBuf) -> anyhow::Result<JobStatusResponse> {
    let bytes = std::fs::read(path).map_err(|e| {
        anyhow::anyhow!("read {}: {e}", path.display())
    })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        anyhow::anyhow!("parse {} as JobStatusResponse: {e}", path.display())
    })
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let (primary_path, shadow_path) = resolve_paths(&args)?;
    let primary = load(&primary_path)?;
    let shadow = load(&shadow_path)?;
    let report = compare(&primary, &shadow);

    let json = serde_json::to_string_pretty(&report)?;
    match &args.output {
        Some(p) => std::fs::write(p, &json)?,
        None => println!("{json}"),
    }
    // Exit code drives CI-friendly gating.
    std::process::exit(match report.severity {
        Severity::Matched => 0,
        Severity::Minor => 1,
        Severity::Major => 2,
        Severity::Critical => 3,
    });
}

fn resolve_paths(args: &Args) -> anyhow::Result<(PathBuf, PathBuf)> {
    match (&args.primary, &args.shadow, args.positional.as_slice()) {
        (Some(p), Some(s), _) => Ok((p.clone(), s.clone())),
        (None, None, [p, s, ..]) => Ok((p.clone(), s.clone())),
        _ => anyhow::bail!(
            "specify either --primary X --shadow Y, or positionally: nix-ci-compare primary.json shadow.json"
        ),
    }
}

/// Pure-function comparison — unit-testable without file I/O.
pub(crate) fn compare(primary: &JobStatusResponse, shadow: &JobStatusResponse) -> Report {
    let status_mismatch = primary.status != shadow.status;

    let counts_delta = CountsDelta {
        total: shadow.counts.total as i64 - primary.counts.total as i64,
        pending: shadow.counts.pending as i64 - primary.counts.pending as i64,
        building: shadow.counts.building as i64 - primary.counts.building as i64,
        done: shadow.counts.done as i64 - primary.counts.done as i64,
        failed: shadow.counts.failed as i64 - primary.counts.failed as i64,
    };

    // Build sets keyed on drv_hash. Ignore propagated failures for the
    // set comparison — they're a consequence of originating failures,
    // so if the originating failures match the propagated set will too.
    let primary_origin: std::collections::HashMap<&str, &nix_ci_core::types::DrvFailure> = primary
        .failures
        .iter()
        .filter(|f| f.propagated_from.is_none() && f.drv_name != "<truncated>")
        .map(|f| (f.drv_hash.as_str(), f))
        .collect();
    let shadow_origin: std::collections::HashMap<&str, &nix_ci_core::types::DrvFailure> = shadow
        .failures
        .iter()
        .filter(|f| f.propagated_from.is_none() && f.drv_name != "<truncated>")
        .map(|f| (f.drv_hash.as_str(), f))
        .collect();

    let failures_only_in_primary: Vec<FailureRef> = primary_origin
        .iter()
        .filter(|(k, _)| !shadow_origin.contains_key(*k))
        .map(|(_, f)| FailureRef {
            drv_hash: f.drv_hash.as_str().to_string(),
            drv_name: f.drv_name.clone(),
            category: f.error_category,
        })
        .collect();
    let failures_only_in_shadow: Vec<FailureRef> = shadow_origin
        .iter()
        .filter(|(k, _)| !primary_origin.contains_key(*k))
        .map(|(_, f)| FailureRef {
            drv_hash: f.drv_hash.as_str().to_string(),
            drv_name: f.drv_name.clone(),
            category: f.error_category,
        })
        .collect();

    let failure_category_mismatches: Vec<CategoryMismatch> = primary_origin
        .iter()
        .filter_map(|(k, pf)| {
            let sf = shadow_origin.get(*k)?;
            if pf.error_category != sf.error_category {
                Some(CategoryMismatch {
                    drv_hash: pf.drv_hash.as_str().to_string(),
                    drv_name: pf.drv_name.clone(),
                    primary_category: pf.error_category,
                    shadow_category: sf.error_category,
                })
            } else {
                None
            }
        })
        .collect();

    // Severity: worst-case escalation.
    let severity = if status_mismatch {
        Severity::Critical
    } else if !failures_only_in_primary.is_empty() || !failures_only_in_shadow.is_empty() {
        Severity::Major
    } else if !failure_category_mismatches.is_empty()
        || counts_delta.done != 0
        || counts_delta.failed != 0
    {
        Severity::Minor
    } else {
        Severity::Matched
    };

    let matched = severity == Severity::Matched;

    let mut summary = Vec::new();
    if status_mismatch {
        summary.push(format!(
            "terminal status differs: primary={:?} shadow={:?}",
            primary.status, shadow.status
        ));
    }
    if !failures_only_in_shadow.is_empty() {
        summary.push(format!(
            "{} drv(s) failed in shadow but not primary (first: {})",
            failures_only_in_shadow.len(),
            failures_only_in_shadow
                .first()
                .map(|f| f.drv_name.as_str())
                .unwrap_or("")
        ));
    }
    if !failures_only_in_primary.is_empty() {
        summary.push(format!(
            "{} drv(s) failed in primary but not shadow (first: {})",
            failures_only_in_primary.len(),
            failures_only_in_primary
                .first()
                .map(|f| f.drv_name.as_str())
                .unwrap_or("")
        ));
    }
    if let Some(worker) = shadow.suspected_worker_infra.as_deref() {
        if primary.suspected_worker_infra.as_deref() != Some(worker) {
            summary.push(format!(
                "shadow flagged worker infra suspicion: {worker}"
            ));
        }
    }

    Report {
        matched,
        severity,
        status: StatusCompare {
            primary: primary.status,
            shadow: shadow.status,
            mismatch: status_mismatch,
        },
        counts_delta,
        failures_only_in_primary,
        failures_only_in_shadow,
        failure_category_mismatches,
        suspected_worker_infra: InfraCompare {
            primary: primary.suspected_worker_infra.clone(),
            shadow: shadow.suspected_worker_infra.clone(),
        },
        summary,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix_ci_core::types::{DrvFailure, DrvHash, JobCounts, JobId};

    fn base(status: JobStatus) -> JobStatusResponse {
        JobStatusResponse {
            id: JobId::new(),
            status,
            sealed: true,
            counts: JobCounts {
                total: 10,
                pending: 0,
                building: 0,
                done: 10,
                failed: 0,
            },
            failures: Vec::new(),
            eval_error: None,
            eval_errors: Vec::new(),
            suspected_worker_infra: None,
        }
    }

    fn fail(drv_hash: &str, drv_name: &str, cat: ErrorCategory) -> DrvFailure {
        DrvFailure {
            drv_hash: DrvHash::new(drv_hash),
            drv_name: drv_name.into(),
            error_category: cat,
            error_message: Some("failed".into()),
            log_tail: None,
            propagated_from: None,
            worker_id: None,
        }
    }

    #[test]
    fn identical_jobs_match() {
        let p = base(JobStatus::Done);
        let s = base(JobStatus::Done);
        let r = compare(&p, &s);
        assert!(r.matched);
        assert!(matches!(r.severity, Severity::Matched));
    }

    #[test]
    fn status_mismatch_is_critical() {
        let p = base(JobStatus::Done);
        let mut s = base(JobStatus::Failed);
        s.counts.failed = 1;
        s.counts.done = 9;
        let r = compare(&p, &s);
        assert!(!r.matched);
        assert!(matches!(r.severity, Severity::Critical));
        assert!(r.status.mismatch);
    }

    #[test]
    fn failure_only_in_shadow_is_major() {
        let p = base(JobStatus::Failed);
        let mut s = base(JobStatus::Failed);
        s.failures.push(fail("aaa", "gcc", ErrorCategory::BuildFailure));
        let r = compare(&p, &s);
        assert!(matches!(r.severity, Severity::Major));
        assert_eq!(r.failures_only_in_shadow.len(), 1);
        assert_eq!(r.failures_only_in_primary.len(), 0);
    }

    #[test]
    fn failure_category_mismatch_is_minor() {
        let mut p = base(JobStatus::Failed);
        p.failures
            .push(fail("aaa", "gcc", ErrorCategory::BuildFailure));
        let mut s = base(JobStatus::Failed);
        s.failures.push(fail("aaa", "gcc", ErrorCategory::Transient));
        let r = compare(&p, &s);
        assert!(matches!(r.severity, Severity::Minor));
        assert_eq!(r.failure_category_mismatches.len(), 1);
        assert_eq!(r.failures_only_in_primary.len(), 0);
        assert_eq!(r.failures_only_in_shadow.len(), 0);
    }

    #[test]
    fn truncated_markers_and_propagated_excluded_from_set_compare() {
        // cap_failures can append a synthetic "<truncated>" row.
        // propagate_failure_inmem sets propagated_from. Neither
        // should show up as a "failure only in X" — they're
        // bookkeeping, not evidence of divergence.
        let mut p = base(JobStatus::Failed);
        p.failures.push(DrvFailure {
            drv_hash: DrvHash::new("aaa"),
            drv_name: "origin".into(),
            error_category: ErrorCategory::BuildFailure,
            error_message: None,
            log_tail: None,
            propagated_from: None,
            worker_id: None,
        });
        // propagated + truncated rows on primary only.
        p.failures.push(DrvFailure {
            drv_hash: DrvHash::new("bbb"),
            drv_name: "child".into(),
            error_category: ErrorCategory::PropagatedFailure,
            error_message: None,
            log_tail: None,
            propagated_from: Some(DrvHash::new("aaa")),
            worker_id: None,
        });
        p.failures.push(DrvFailure {
            drv_hash: DrvHash::new("<truncated>"),
            drv_name: "<truncated>".into(),
            error_category: ErrorCategory::PropagatedFailure,
            error_message: None,
            log_tail: None,
            propagated_from: None,
            worker_id: None,
        });
        let mut s = base(JobStatus::Failed);
        s.failures.push(DrvFailure {
            drv_hash: DrvHash::new("aaa"),
            drv_name: "origin".into(),
            error_category: ErrorCategory::BuildFailure,
            error_message: None,
            log_tail: None,
            propagated_from: None,
            worker_id: None,
        });
        let r = compare(&p, &s);
        // Origin failure matches; propagated + truncated are filtered.
        assert_eq!(r.failures_only_in_primary.len(), 0);
        assert_eq!(r.failures_only_in_shadow.len(), 0);
        assert!(matches!(r.severity, Severity::Matched | Severity::Minor));
    }

    #[test]
    fn shadow_flags_infra_that_primary_missed_surfaces_in_summary() {
        let p = base(JobStatus::Failed);
        let mut s = base(JobStatus::Failed);
        s.suspected_worker_infra = Some("host-3".into());
        let r = compare(&p, &s);
        assert!(
            r.summary.iter().any(|line| line.contains("host-3")),
            "shadow's infra suspicion must surface in summary: {:?}",
            r.summary
        );
    }
}
