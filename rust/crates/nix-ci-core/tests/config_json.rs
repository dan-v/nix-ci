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

// ─── validate(): further zero / range checks ─────────────────────────
// Each validate() branch is a separate deployment-safety gate. A gap
// means a misconfigured coordinator that boots and then fails in some
// surprising way at runtime. Cover every zero-check individually.

#[test]
fn validate_catches_zero_retention_days() {
    let cfg = ServerConfig {
        retention_days: 0,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err.errors.iter().any(|e| e.contains("retention_days")));
}

#[test]
fn validate_catches_zero_progress_tick() {
    let cfg = ServerConfig {
        progress_tick_secs: 0,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("progress_tick_secs")));
}

#[test]
fn validate_catches_zero_submission_event_capacity() {
    let cfg = ServerConfig {
        submission_event_capacity: 0,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("submission_event_capacity")));
}

#[test]
fn validate_catches_max_drvs_per_job_zero() {
    // Some(0) is a misconfiguration — use None to disable the cap.
    let cfg = ServerConfig {
        max_drvs_per_job: Some(0),
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("max_drvs_per_job")));
}

#[test]
fn validate_catches_max_drvs_per_job_below_warn_threshold() {
    // If the hard cap fires *before* the soft warn, ops never see a
    // warning before hitting the 413. Always ensure warn <= cap.
    let cfg = ServerConfig {
        max_drvs_per_job: Some(100),
        submission_warn_threshold: 1000,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("submission_warn_threshold")));
}

#[test]
fn validate_accepts_max_drvs_per_job_none() {
    // `None` explicitly disables the cap. Must not produce a validate
    // error (otherwise fresh deployments on None would refuse to boot).
    let cfg = ServerConfig {
        max_drvs_per_job: None,
        ..ServerConfig::default()
    };
    assert!(cfg.validate().is_ok());
}

#[test]
fn validate_catches_reaper_equal_to_heartbeat_timeout() {
    // Boundary: "less than" means strictly less. Equal is NOT enough —
    // a reaper that fires on the same tick as the heartbeat window
    // observes borderline-stale jobs as fresh on first pass and
    // reaps them on second, doubling worst-case reap latency.
    let cfg = ServerConfig {
        reaper_interval_secs: 30,
        job_heartbeat_timeout_secs: 30,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("job_heartbeat_timeout_secs")));
}

#[test]
fn validate_catches_short_max_drv_path() {
    // Real Nix store paths are ~100+ bytes. A cap of 1 would reject
    // every legitimate drv at ingest.
    let cfg = ServerConfig {
        max_drv_path_bytes: 1,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("max_drv_path_bytes")));
}

#[test]
fn validate_catches_too_small_max_identifier_bytes() {
    // The 16-byte floor exists because a UUID is 36 chars; less
    // wouldn't even fit a single job id in external_ref.
    let cfg = ServerConfig {
        max_identifier_bytes: 15,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err
        .errors
        .iter()
        .any(|e| e.contains("max_identifier_bytes")));
}

// ─── ConfigErrors::Display ────────────────────────────────────────────

#[test]
fn config_errors_display_lists_each_error_on_its_own_line() {
    // Operators grep this output. Every error must be on a separate
    // bulleted line so --validate output stays readable.
    let cfg = ServerConfig {
        retention_days: 0,
        progress_tick_secs: 0,
        max_attempts: 0,
        ..ServerConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    let s = format!("{err}");
    // One leading "  - " bullet per error.
    let bullets = s.matches("  - ").count();
    assert_eq!(
        bullets,
        err.errors.len(),
        "expected one bullet per error, got {bullets} for\n{s}"
    );
    // And each error text appears verbatim in the rendered output.
    for e in &err.errors {
        assert!(s.contains(e), "{e:?} missing from:\n{s}");
    }
}

// ─── apply_bearer_files: systemd LoadCredential integration ───────────
// These tests mutate process env vars. std::env::set_var is unsafe in
// the 2024 edition; serialize access via a module-local mutex so tests
// inside this file don't race with each other. Other test binaries run
// in separate processes so there's no cross-binary contention.

use std::sync::Mutex;
static BEARER_ENV_LOCK: Mutex<()> = Mutex::new(());

const AUTH_VAR: &str = "NIX_CI_AUTH_BEARER_FILE";
const ADMIN_VAR: &str = "NIX_CI_ADMIN_BEARER_FILE";

fn clear_bearer_env() {
    // SAFETY: called only under BEARER_ENV_LOCK, so no other test in
    // this binary is reading the variable concurrently.
    unsafe {
        std::env::remove_var(AUTH_VAR);
        std::env::remove_var(ADMIN_VAR);
    }
}

#[test]
fn apply_bearer_files_loads_token_from_worker_credential_file() {
    // systemd's LoadCredential writes the token to a path it passes via
    // NIX_CI_AUTH_BEARER_FILE. The config struct's inline
    // `auth_bearer` MUST be overridden by the file so a deployment
    // that keeps the secret out of the JSON still works.
    let _g = BEARER_ENV_LOCK.lock().unwrap();
    clear_bearer_env();
    let f = write_tmp("worker-token\n");
    unsafe {
        std::env::set_var(AUTH_VAR, f.path());
    }
    let mut cfg = ServerConfig {
        auth_bearer: Some("JSON-inline-value".into()),
        ..ServerConfig::default()
    };
    cfg.apply_bearer_files();
    // File wins over inline.
    assert_eq!(cfg.auth_bearer.as_deref(), Some("worker-token"));
    clear_bearer_env();
}

#[test]
fn apply_bearer_files_trims_trailing_whitespace_and_newlines() {
    // Operators write tokens with `echo > file`, which appends \n.
    // The comparison against the Authorization header must not be
    // broken by whitespace the shell added.
    let _g = BEARER_ENV_LOCK.lock().unwrap();
    clear_bearer_env();
    let f = write_tmp("admin-token \t\r\n");
    unsafe {
        std::env::set_var(ADMIN_VAR, f.path());
    }
    let mut cfg = ServerConfig::default();
    cfg.apply_bearer_files();
    assert_eq!(cfg.admin_bearer.as_deref(), Some("admin-token"));
    clear_bearer_env();
}

#[test]
fn apply_bearer_files_empty_file_leaves_inline_unchanged() {
    // A zero-byte credential file (or one that's just whitespace)
    // must NOT clobber a valid inline token with empty — that would
    // silently lock operators out.
    let _g = BEARER_ENV_LOCK.lock().unwrap();
    clear_bearer_env();
    let f = write_tmp("   \n\n");
    unsafe {
        std::env::set_var(AUTH_VAR, f.path());
    }
    let mut cfg = ServerConfig {
        auth_bearer: Some("inline-fallback".into()),
        ..ServerConfig::default()
    };
    cfg.apply_bearer_files();
    assert_eq!(cfg.auth_bearer.as_deref(), Some("inline-fallback"));
    clear_bearer_env();
}

#[test]
fn apply_bearer_files_unreadable_path_leaves_inline_unchanged() {
    // An unreadable credential file (typo in systemd unit, permissions
    // wrong) must fall back to the inline value rather than nuke it —
    // otherwise the coordinator would refuse every request, including
    // the admin override used to fix the misconfiguration.
    let _g = BEARER_ENV_LOCK.lock().unwrap();
    clear_bearer_env();
    unsafe {
        std::env::set_var(ADMIN_VAR, "/definitely/not/a/real/path/bearer.txt");
    }
    let mut cfg = ServerConfig {
        admin_bearer: Some("inline-admin".into()),
        ..ServerConfig::default()
    };
    cfg.apply_bearer_files();
    assert_eq!(cfg.admin_bearer.as_deref(), Some("inline-admin"));
    clear_bearer_env();
}

#[test]
fn apply_bearer_files_no_env_set_is_no_op() {
    // Deployments without systemd LoadCredential leave the env vars
    // unset. apply_bearer_files must not modify the config at all
    // (and must not panic trying to read a missing env var).
    let _g = BEARER_ENV_LOCK.lock().unwrap();
    clear_bearer_env();
    let mut cfg = ServerConfig {
        auth_bearer: Some("keep-me".into()),
        admin_bearer: None,
        ..ServerConfig::default()
    };
    cfg.apply_bearer_files();
    assert_eq!(cfg.auth_bearer.as_deref(), Some("keep-me"));
    assert!(cfg.admin_bearer.is_none());
}

#[test]
fn apply_bearer_files_whitespace_only_env_path_is_ignored() {
    // A variable that accidentally got `export X=" "` should be
    // treated as unset — otherwise we'd try to open a whitespace path
    // and log an unreadable-file warning on every boot.
    let _g = BEARER_ENV_LOCK.lock().unwrap();
    clear_bearer_env();
    unsafe {
        std::env::set_var(AUTH_VAR, "    ");
    }
    let mut cfg = ServerConfig {
        auth_bearer: Some("inline".into()),
        ..ServerConfig::default()
    };
    cfg.apply_bearer_files();
    assert_eq!(cfg.auth_bearer.as_deref(), Some("inline"));
    clear_bearer_env();
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
