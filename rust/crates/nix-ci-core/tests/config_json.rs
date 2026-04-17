//! JSON config file: load, partial overrides, validation, round-trip.

use std::io::Write;

use nix_ci_core::config::ServerConfig;

fn write_tmp(json: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(json.as_bytes()).unwrap();
    f
}

#[test]
fn load_full_config_roundtrips_through_json() {
    let original = ServerConfig {
        database_url: "postgres://x/y".into(),
        retention_days: 99,
        ..ServerConfig::default()
    };
    let json = serde_json::to_string(&original).unwrap();
    let f = write_tmp(&json);
    let loaded = ServerConfig::load_json(f.path()).unwrap();
    assert_eq!(loaded.database_url, "postgres://x/y");
    assert_eq!(loaded.retention_days, 99);
    // Untouched fields match the default.
    assert_eq!(loaded.lock_key, ServerConfig::default().lock_key);
}

#[test]
fn load_partial_config_inherits_defaults_for_missing_fields() {
    // Only database_url + retention_days specified — every other knob
    // must come from `Default`. This is the operational sweet spot:
    // ops only writes what they actively want to override.
    let json = r#"{
        "database_url": "postgres://only/two",
        "retention_days": 30
    }"#;
    let f = write_tmp(json);
    let cfg = ServerConfig::load_json(f.path()).unwrap();
    assert_eq!(cfg.database_url, "postgres://only/two");
    assert_eq!(cfg.retention_days, 30);
    assert_eq!(cfg.max_attempts, ServerConfig::default().max_attempts);
    assert_eq!(
        cfg.claim_deadline_secs,
        ServerConfig::default().claim_deadline_secs
    );
}

#[test]
fn load_rejects_unknown_field_with_helpful_error() {
    // Typo: `max_attemps` instead of `max_attempts`. With
    // `deny_unknown_fields` we surface the typo at parse time
    // instead of silently inheriting the default.
    let json = r#"{ "max_attemps": 5 }"#;
    let f = write_tmp(json);
    let err = ServerConfig::load_json(f.path()).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("max_attemps") || msg.contains("unknown field"),
        "expected unknown-field diagnostic, got {msg}"
    );
}

#[test]
fn load_rejects_invalid_socket_addr_at_parse_time() {
    let json = r#"{ "listen": "not-a-socket-addr" }"#;
    let f = write_tmp(json);
    let err = ServerConfig::load_json(f.path()).unwrap_err();
    assert!(format!("{err}").contains("listen") || format!("{err}").contains("addr"));
}

#[test]
fn load_missing_file_errors_cleanly() {
    let err =
        ServerConfig::load_json(std::path::Path::new("/nonexistent/nix-ci.json")).unwrap_err();
    assert!(format!("{err}").contains("/nonexistent/nix-ci.json"));
}

#[test]
fn validate_default_config_passes() {
    // The shipped defaults must always be valid — otherwise
    // a fresh deployment with no config file fails to start.
    ServerConfig::default()
        .validate()
        .expect("defaults must validate");
}

#[test]
fn validate_catches_zero_timeouts() {
    let mut cfg = ServerConfig {
        claim_deadline_secs: 0,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err.errors.iter().any(|e| e.contains("claim_deadline_secs")));

    cfg.claim_deadline_secs = 100;
    cfg.reaper_interval_secs = 0;
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("reaper_interval_secs")));
}

#[test]
fn validate_catches_reaper_outpacing_deadlines() {
    // reaper_interval_secs >= claim_deadline_secs would reap claims
    // before they could possibly finish — caught explicitly.
    let cfg = ServerConfig {
        reaper_interval_secs: 60,
        claim_deadline_secs: 30,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("less than claim_deadline_secs")));
}

#[test]
fn validate_reports_all_problems_at_once() {
    // Operators shouldn't have to fix-rerun-fix-rerun.
    let cfg = ServerConfig {
        database_url: String::new(),
        max_attempts: 0,
        max_drv_path_bytes: 1,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(
        err.errors.len() >= 3,
        "expected multiple errors, got {:?}",
        err.errors
    );
}

#[test]
fn validate_catches_negative_backoff() {
    let cfg = ServerConfig {
        flaky_retry_backoff_step_ms: -1,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("flaky_retry_backoff_step_ms")));
}

#[test]
fn print_config_emits_valid_json() {
    let cfg = ServerConfig::default();
    let pretty = cfg.to_json_pretty();
    // Round-trip through serde to confirm the printed form is parseable.
    let _: ServerConfig = serde_json::from_str(&pretty)
        .unwrap_or_else(|e| panic!("--print-config emitted unparseable JSON: {e}\n{pretty}"));
}

#[test]
fn print_config_redacts_bearer_tokens() {
    // Operators paste `--print-config` output into tickets. The
    // plaintext token MUST NOT appear even if the operator forgets to
    // strip it. Both the worker bearer and the optional admin bearer
    // have to be redacted, and the redacted form still has to parse.
    let cfg = ServerConfig {
        auth_bearer: Some("super-secret-worker".into()),
        admin_bearer: Some("super-secret-admin".into()),
        ..ServerConfig::default()
    };
    let pretty = cfg.to_json_pretty();
    assert!(
        !pretty.contains("super-secret-worker"),
        "auth_bearer plaintext leaked: {pretty}"
    );
    assert!(
        !pretty.contains("super-secret-admin"),
        "admin_bearer plaintext leaked: {pretty}"
    );
    assert!(pretty.contains("<redacted>"));
    // Round-trips as valid JSON (with redacted values in place).
    let _: ServerConfig = serde_json::from_str(&pretty)
        .unwrap_or_else(|e| panic!("redacted config is unparseable: {e}\n{pretty}"));
}

#[test]
fn validate_rejects_log_retention_longer_than_job_retention() {
    // The build_logs_job_fk CASCADE deletes logs when their parent job
    // is pruned. If log retention outlives job retention, the cascade
    // fires before the log-cutoff fires, defeating the separate
    // retention knob. Surface it at boot, not via missing logs in prod.
    let cfg = ServerConfig {
        retention_days: 7,
        build_log_retention_days: 30,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(
        err.errors
            .iter()
            .any(|e| e.contains("build_log_retention_days") && e.contains("retention_days")),
        "expected log-vs-job retention ordering error; got {:?}",
        err.errors
    );
}
