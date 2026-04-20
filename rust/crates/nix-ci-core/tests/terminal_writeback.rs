//! Terminal snapshot persistence and idempotency.

mod common;

use std::time::Duration;

use common::{drv_path, ingest};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, ErrorCategory, JobStatus};
use sqlx::PgPool;

#[sqlx::test]
async fn terminal_snapshot_caps_failures(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_failures_in_result = 3;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // Cancel with a synthetic failures list would be hard — instead,
    // ingest 5 drvs, fail each, then check the snapshot is capped. Make
    // them independent (no deps) so each ingest + complete is a pure
    // terminal failure.
    let mut drvs = Vec::new();
    for i in 0..5 {
        let p = drv_path(&format!("cap{i}"), &format!("pkg-{i}"));
        client
            .ingest_drv(job.id, &ingest(&p, &format!("pkg-{i}"), &[], true))
            .await
            .unwrap();
        drvs.push(p);
    }
    client.seal(job.id).await.unwrap();

    for _ in 0..5 {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 5)
            .await
            .unwrap()
            .expect("claim");
        client
            .complete(
                job.id,
                c.claim_id,
                &CompleteRequest {
                    success: false,
                    duration_ms: 1,
                    exit_code: Some(1),
                    error_category: Some(ErrorCategory::BuildFailure),
                    error_message: Some("nope".into()),
                    log_tail: None,
                },
            )
            .await
            .unwrap();
    }

    // Wait for terminal.
    for _ in 0..40 {
        let s = client.status(job.id).await.unwrap();
        if s.status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Post-terminal: the submission is gone from memory; status reads
    // from jobs.result JSONB. The capped list must be <= cap+1 (the +1
    // being the truncation marker).
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Failed);
    assert!(
        status.failures.len() <= 4,
        "failures list must be capped (got {})",
        status.failures.len()
    );
    // Count is cap + 1 (marker). The marker's error_message must
    // include the EXACT overflow count (5 total − 3 cap = 2) so an
    // arithmetic mutation (`-` → `/`, `/` → `*`, etc.) flips the
    // number and fails here.
    let marker = status
        .failures
        .iter()
        .find(|f| f.drv_name == "<truncated>")
        .expect("must contain a truncation marker entry");
    let msg = marker
        .error_message
        .as_ref()
        .expect("marker must have error_message");
    assert!(
        msg.starts_with("2 "),
        "marker must report overflow=2 (5 - 3); got {msg:?}"
    );
}

#[sqlx::test]
async fn transition_job_terminal_is_idempotent(pool: PgPool) {
    // First call must transition (return true); second must be a no-op
    // (return false) because `WHERE done_at IS NULL` rejects it. The
    // complete handler relies on this to decide whether to publish
    // JobEvent::JobDone exactly once.
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let job_id = nix_ci_core::types::JobId::new();
    nix_ci_core::durable::writeback::upsert_job(&pool, job_id, None)
        .await
        .unwrap();
    let snap = serde_json::json!({ "id": job_id.0.to_string() });
    let first =
        nix_ci_core::durable::writeback::transition_job_terminal(&pool, job_id, "done", &snap)
            .await
            .unwrap();
    assert!(first, "first transition must return true");
    let second =
        nix_ci_core::durable::writeback::transition_job_terminal(&pool, job_id, "done", &snap)
            .await
            .unwrap();
    assert!(!second, "second transition must return false (idempotent)");

    // Non-existent job: also returns false (no row affected).
    let missing_id = nix_ci_core::types::JobId::new();
    let ghost =
        nix_ci_core::durable::writeback::transition_job_terminal(&pool, missing_id, "done", &snap)
            .await
            .unwrap();
    assert!(!ghost, "non-existent job must return false");
}
