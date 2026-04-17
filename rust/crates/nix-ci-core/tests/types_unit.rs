//! Unit tests for the pure helpers on wire types (parsing + enum
//! predicates). These have no DB or HTTP dependency, so they run
//! instantly and catch contract drift (serde tag names, retry
//! classification, drv_hash extraction) the moment it happens rather
//! than in an e2e test three levels up.

use nix_ci_core::types::{
    drv_hash_from_path, ClaimQuery, DrvState, ErrorCategory, JobStatus,
};

// ─── drv_hash_from_path ───────────────────────────────────────────────

#[test]
fn drv_hash_from_standard_nix_store_path() {
    let got = drv_hash_from_path("/nix/store/abc123def-hello-2.12.1.drv");
    assert_eq!(got.as_ref().map(|h| h.as_str()), Some("abc123def-hello-2.12.1.drv"));
}

#[test]
fn drv_hash_from_bare_basename() {
    // Callers sometimes pass just the basename. As long as it looks
    // like a drv (contains `-`, ends `.drv`) we accept it.
    let got = drv_hash_from_path("abc123-x.drv");
    assert_eq!(got.as_ref().map(|h| h.as_str()), Some("abc123-x.drv"));
}

#[test]
fn drv_hash_returns_none_for_missing_drv_suffix() {
    assert!(drv_hash_from_path("/nix/store/abc-hello").is_none());
    assert!(drv_hash_from_path("/nix/store/abc-hello.txt").is_none());
}

#[test]
fn drv_hash_returns_none_for_missing_dash_separator() {
    // A real Nix drv basename always has `<hash>-<name>.drv`. No dash
    // means the input isn't a Nix store path — reject rather than
    // silently accept.
    assert!(drv_hash_from_path("/nix/store/abc.drv").is_none());
    assert!(drv_hash_from_path("abc.drv").is_none());
}

#[test]
fn drv_hash_returns_none_for_empty_basename() {
    // A trailing slash leaves the basename empty; must not panic or
    // succeed with an empty hash.
    assert!(drv_hash_from_path("/nix/store/").is_none());
    assert!(drv_hash_from_path("").is_none());
    assert!(drv_hash_from_path("/").is_none());
}

#[test]
fn drv_hash_extracts_basename_only_ignoring_parent_dirs() {
    // The directory part is irrelevant — even a weird prefix shouldn't
    // affect the result (the basename parser is path-component only).
    let got = drv_hash_from_path("/some/weird/path/xyz-foo-1.0.drv");
    assert_eq!(got.as_ref().map(|h| h.as_str()), Some("xyz-foo-1.0.drv"));
}

// ─── JobStatus ────────────────────────────────────────────────────────

#[test]
fn job_status_is_terminal_matches_exactly_terminal_variants() {
    assert!(!JobStatus::Pending.is_terminal());
    assert!(!JobStatus::Building.is_terminal());
    assert!(JobStatus::Done.is_terminal());
    assert!(JobStatus::Failed.is_terminal());
    assert!(JobStatus::Cancelled.is_terminal());
}

#[test]
fn job_status_as_str_matches_serde_lowercase_rename() {
    // The DB schema's `status` column stores exactly these strings.
    // A drift here would silently corrupt terminal-state writeback.
    for (variant, want) in [
        (JobStatus::Pending, "pending"),
        (JobStatus::Building, "building"),
        (JobStatus::Done, "done"),
        (JobStatus::Failed, "failed"),
        (JobStatus::Cancelled, "cancelled"),
    ] {
        assert_eq!(variant.as_str(), want);
        // Cross-check that serde agrees with as_str — they're two
        // separate sources of truth that MUST NOT diverge.
        let j = serde_json::to_string(&variant).unwrap();
        assert_eq!(j, format!("\"{want}\""));
    }
}

#[test]
fn drv_state_as_str_matches_serde() {
    for (variant, want) in [
        (DrvState::Pending, "pending"),
        (DrvState::Building, "building"),
        (DrvState::Done, "done"),
        (DrvState::Failed, "failed"),
    ] {
        assert_eq!(variant.as_str(), want);
        let j = serde_json::to_string(&variant).unwrap();
        assert_eq!(j, format!("\"{want}\""));
    }
}

// ─── ErrorCategory ────────────────────────────────────────────────────

#[test]
fn error_category_is_retryable_matches_transient_and_diskfull() {
    // Retry policy is the load-bearing contract here. BuildFailure and
    // PropagatedFailure MUST NOT retry (they'd waste a worker and
    // re-fail deterministically).
    assert!(!ErrorCategory::BuildFailure.is_retryable());
    assert!(ErrorCategory::Transient.is_retryable());
    assert!(ErrorCategory::DiskFull.is_retryable());
    assert!(!ErrorCategory::PropagatedFailure.is_retryable());
}

#[test]
fn error_category_as_str_matches_serde_snake_case() {
    for (variant, want) in [
        (ErrorCategory::BuildFailure, "build_failure"),
        (ErrorCategory::Transient, "transient"),
        (ErrorCategory::DiskFull, "disk_full"),
        (ErrorCategory::PropagatedFailure, "propagated_failure"),
    ] {
        assert_eq!(variant.as_str(), want);
        let j = serde_json::to_string(&variant).unwrap();
        assert_eq!(j, format!("\"{want}\""));
    }
}

// ─── ClaimQuery parsing ───────────────────────────────────────────────

fn mk_claim_query(system: &str, features: &str) -> ClaimQuery {
    ClaimQuery {
        wait: 30,
        system: system.into(),
        features: features.into(),
        worker: None,
    }
}

#[test]
fn features_vec_empty_string_is_empty() {
    assert!(mk_claim_query("x86_64-linux", "").features_vec().is_empty());
}

#[test]
fn features_vec_single_feature_preserved() {
    assert_eq!(
        mk_claim_query("x86_64-linux", "big-parallel").features_vec(),
        vec!["big-parallel".to_string()]
    );
}

#[test]
fn features_vec_multiple_features_split_on_comma() {
    assert_eq!(
        mk_claim_query("x86_64-linux", "a,b,c").features_vec(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
}

#[test]
fn features_vec_strips_whitespace_and_empty_entries() {
    // Operators may accidentally paste spaces; shouldn't produce empty
    // or whitespace-padded entries that fail to match the drv's
    // required_features set comparison downstream.
    assert_eq!(
        mk_claim_query("x86_64-linux", " a , , b ,").features_vec(),
        vec!["a".to_string(), "b".to_string()]
    );
}

#[test]
fn systems_vec_single_system_produces_one_element() {
    // The 99% case: one system string from a bare worker. Order-
    // preserving so the match-priority hint downstream is stable.
    assert_eq!(
        mk_claim_query("x86_64-linux", "").systems_vec(),
        vec!["x86_64-linux".to_string()]
    );
}

#[test]
fn systems_vec_multi_system_preserves_order() {
    // A cross-compilation host advertises multiple systems; the
    // coordinator's claim picker uses vector order as a preference
    // ranking. Order must be stable from the wire input.
    assert_eq!(
        mk_claim_query("x86_64-linux,aarch64-linux", "").systems_vec(),
        vec!["x86_64-linux".to_string(), "aarch64-linux".to_string()]
    );
}

#[test]
fn systems_vec_empty_string_is_empty() {
    assert!(mk_claim_query("", "").systems_vec().is_empty());
}

#[test]
fn systems_vec_strips_whitespace_and_empty_entries() {
    assert_eq!(
        mk_claim_query(" x86_64-linux , ,aarch64-linux ", "").systems_vec(),
        vec!["x86_64-linux".to_string(), "aarch64-linux".to_string()]
    );
}
