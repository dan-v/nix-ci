//! End-to-end tests for `runner::artifacts::collect_failure_logs`.
//!
//! The in-file unit tests for `unique_filename` / `sanitize_filename`
//! cover the pure helpers; this file covers the async path that talks
//! to the coordinator's log endpoints. Without these tests, the
//! "coordinator-returned-a-log" vs "fall-back-to-inline-tail" vs
//! "write-placeholder-for-missing-log" branches are all unexercised.

mod common;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use common::{drv_path, spawn_server};
use nix_ci_core::client::{BuildLogUploadMeta, CoordinatorClient};
use nix_ci_core::runner::artifacts::{collect_failure_logs, prepare_artifacts_dir};
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, DrvHash, ErrorCategory, IngestDrvRequest,
};
use sqlx::PgPool;

fn gz(s: &str) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(s.as_bytes()).unwrap();
    e.finish().unwrap()
}

fn ingest_root(drv: &str, name: &str) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        attr: None,
    }
}

async fn wait_terminal(client: &CoordinatorClient, job_id: nix_ci_core::types::JobId) {
    for _ in 0..80 {
        let s = client.status(job_id).await.unwrap();
        if s.status.is_terminal() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("job did not reach terminal within budget");
}

// ─── happy path: originating failure with archived log ────────────────

#[sqlx::test]
async fn writes_archived_log_for_originating_failure(pool: PgPool) {
    // With a real uploaded log, collect_failure_logs must pull the
    // archived bytes back — NOT the inline log_tail. Verifies the
    // primary success path end-to-end: list_drv_logs → fetch_log →
    // disk write.
    let h = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&h.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("aa1", "broken-pkg");
    let drv_hash = DrvHash::new("aa1-broken-pkg.drv");
    client
        .ingest_drv(job.id, &ingest_root(&drv, "broken-pkg"))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    let payload = "FULL ARCHIVED LOG BODY\nerror: build failed\n";
    client
        .upload_log(
            job.id,
            c.claim_id,
            BuildLogUploadMeta {
                drv_hash: &drv_hash,
                attempt: c.attempt,
                original_size: payload.len() as u32,
                truncated: false,
                success: false,
                exit_code: Some(1),
                started_at: Utc::now() - chrono::Duration::seconds(5),
                ended_at: Utc::now(),
            },
            gz(payload),
        )
        .await
        .unwrap();
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 10,
                exit_code: Some(1),
                error_category: Some(ErrorCategory::BuildFailure),
                error_message: Some("build failed".into()),
                log_tail: Some("INLINE TAIL (should not be used when archive present)".into()),
            },
        )
        .await
        .unwrap();

    // Make sure max_attempts is satisfied — default is 2 and the drv
    // is a BuildFailure (non-retryable) so one complete is sufficient.

    wait_terminal(&client, job.id).await;

    let tmp = tempfile::tempdir().unwrap();
    let snap = client.status(job.id).await.unwrap();
    collect_failure_logs(&client, job.id, &snap, tmp.path()).await;

    let log_path = tmp.path().join("build_logs").join("broken-pkg.log");
    let body = std::fs::read_to_string(&log_path)
        .unwrap_or_else(|e| panic!("expected file at {log_path:?}: {e}"));
    assert!(
        body.contains("FULL ARCHIVED LOG"),
        "must pull archived log, got {body:?}"
    );
    assert!(
        !body.contains("INLINE TAIL"),
        "archived log must WIN over the inline log_tail"
    );
}

// ─── fallback: no archive, inline log_tail present ────────────────────

#[sqlx::test]
async fn falls_back_to_inline_log_tail_when_no_archive(pool: PgPool) {
    // If the worker never uploaded (crashed, OOM'd), list_drv_logs
    // returns no attempts — the helper must use the terminal
    // snapshot's inline `log_tail` instead of writing a placeholder.
    let h = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&h.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("aa2", "tail-only-pkg");
    client
        .ingest_drv(job.id, &ingest_root(&drv, "tail-only-pkg"))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    // Deliberately DO NOT upload a log.
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 5,
                exit_code: Some(1),
                error_category: Some(ErrorCategory::BuildFailure),
                error_message: Some("nope".into()),
                log_tail: Some("INLINE FALLBACK LOG TAIL".into()),
            },
        )
        .await
        .unwrap();
    wait_terminal(&client, job.id).await;

    let tmp = tempfile::tempdir().unwrap();
    let snap = client.status(job.id).await.unwrap();
    collect_failure_logs(&client, job.id, &snap, tmp.path()).await;

    let log_path = tmp.path().join("build_logs").join("tail-only-pkg.log");
    let body = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        body.contains("INLINE FALLBACK"),
        "must fall back to inline log_tail; got {body:?}"
    );
}

// ─── placeholder: no archive, no inline tail ──────────────────────────

#[sqlx::test]
async fn writes_placeholder_when_no_log_available(pool: PgPool) {
    // A failure with neither uploaded archive nor inline log_tail
    // (possible when the worker reported a failure but truncated the
    // tail out, or a synthetic coordinator-originated failure) must
    // still produce a file, containing a `[log not available: ...]`
    // marker — otherwise the CI artifact directory silently omits
    // the failure, making the build look "clean" to downstream tools.
    let h = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&h.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("aa3", "silent-fail");
    client
        .ingest_drv(job.id, &ingest_root(&drv, "silent-fail"))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 5,
                exit_code: Some(1),
                error_category: Some(ErrorCategory::BuildFailure),
                error_message: Some("no log at all".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();
    wait_terminal(&client, job.id).await;

    let tmp = tempfile::tempdir().unwrap();
    let snap = client.status(job.id).await.unwrap();
    collect_failure_logs(&client, job.id, &snap, tmp.path()).await;

    let log_path = tmp.path().join("build_logs").join("silent-fail.log");
    let body = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        body.starts_with("[log not available"),
        "placeholder must mark missing log; got {body:?}"
    );
}

// ─── no originating failures → no directory side effects ──────────────

#[sqlx::test]
async fn skips_when_only_propagated_failures(pool: PgPool) {
    // If every failure in the snapshot is category=PropagatedFailure,
    // there's nothing originating to save. The helper must not write
    // any files at all (the CI bundle ends up clean rather than full
    // of meaningless "[log not available]" placeholders for every
    // downstream drv).
    let h = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&h.base_url));
    // Create a 2-drv chain: failing_leaf ← depending_root. One
    // BuildFailure at the leaf, one PropagatedFailure at the root.
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let leaf = drv_path("prop1", "leaf");
    let root = drv_path("prop2", "top");
    client
        .ingest_drv(
            job.id,
            &IngestDrvRequest {
                drv_path: leaf.clone(),
                drv_name: "leaf".into(),
                system: "x86_64-linux".into(),
                required_features: vec![],
                input_drvs: vec![],
                is_root: false,
                attr: None,
            },
        )
        .await
        .unwrap();
    client
        .ingest_drv(
            job.id,
            &IngestDrvRequest {
                drv_path: root.clone(),
                drv_name: "top".into(),
                system: "x86_64-linux".into(),
                required_features: vec![],
                input_drvs: vec![leaf.clone()],
                is_root: true,
                attr: None,
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 5,
                exit_code: Some(1),
                error_category: Some(ErrorCategory::BuildFailure),
                error_message: Some("leaf blew up".into()),
                log_tail: Some("leaf error tail".into()),
            },
        )
        .await
        .unwrap();
    wait_terminal(&client, job.id).await;

    let snap = client.status(job.id).await.unwrap();
    // Filter the snapshot to ONLY the propagated failures so the
    // helper sees nothing originating. This mirrors the decision the
    // helper itself makes on the caller side.
    let mut propagated_only = snap.clone();
    propagated_only
        .failures
        .retain(|f| matches!(f.error_category, ErrorCategory::PropagatedFailure));
    assert!(
        !propagated_only.failures.is_empty(),
        "precondition: we need at least one propagated failure in the snapshot"
    );
    let tmp = tempfile::tempdir().unwrap();
    collect_failure_logs(&Arc::clone(&client), job.id, &propagated_only, tmp.path()).await;

    let build_logs_dir = tmp.path().join("build_logs");
    // Directory may or may not exist — but if it does, it must be empty.
    if build_logs_dir.exists() {
        let entries: Vec<_> = std::fs::read_dir(&build_logs_dir)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            entries.is_empty(),
            "expected no files for propagated-only snapshot, got {entries:?}"
        );
    }
}

// ─── filename dedup for duplicate drv_names ───────────────────────────

#[sqlx::test]
async fn deduplicates_filenames_for_same_drv_name_different_hash(pool: PgPool) {
    // Two originating failures with the same `drv_name` but different
    // `drv_hash`. The helper must produce `hello.log` for the first and
    // `hello-2.log` for the second; otherwise the second file silently
    // overwrites the first and we lose half the failure evidence.
    let h = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&h.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // Two separate drvs, same `drv_name=hello`.
    let drv_a = drv_path("dedup1", "hello");
    let drv_b = drv_path("dedup2", "hello");
    client
        .ingest_drv(job.id, &ingest_root(&drv_a, "hello"))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest_root(&drv_b, "hello"))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Fail each in turn.
    for _ in 0..2 {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 3)
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
                    error_message: Some(format!("nope-{}", c.claim_id)),
                    log_tail: Some(format!("inline tail {}", c.claim_id)),
                },
            )
            .await
            .unwrap();
    }
    wait_terminal(&client, job.id).await;

    let tmp = tempfile::tempdir().unwrap();
    let snap = client.status(job.id).await.unwrap();
    collect_failure_logs(&client, job.id, &snap, tmp.path()).await;

    let build_logs_dir = tmp.path().join("build_logs");
    let first = build_logs_dir.join("hello.log");
    let second = build_logs_dir.join("hello-2.log");
    assert!(first.exists(), "first failure must land at hello.log");
    assert!(
        second.exists(),
        "second failure must get a unique filename (hello-2.log), not overwrite the first"
    );
    let body_a = std::fs::read_to_string(&first).unwrap();
    let body_b = std::fs::read_to_string(&second).unwrap();
    assert_ne!(
        body_a, body_b,
        "the two .log files must contain different content (no overwrite)"
    );
}

// ─── prepare_artifacts_dir ────────────────────────────────────────────

#[tokio::test]
async fn prepare_artifacts_dir_creates_missing_parent() {
    // std::fs::create_dir_all semantics: a nested path must be created
    // in one call. Confirms that passing a fresh
    // `<tmp>/ci-run-<id>/artifacts` produces a valid `eval.stderr`
    // path without the caller having to pre-create the directory.
    let tmp = tempfile::tempdir().unwrap();
    let artifacts = tmp.path().join("ci-run-123").join("artifacts");
    let stderr_path = prepare_artifacts_dir(&artifacts).unwrap();
    assert!(artifacts.is_dir(), "artifacts dir must exist after call");
    assert_eq!(stderr_path, artifacts.join("eval.stderr"));
    // The stderr file itself isn't pre-created — callers open it for
    // writing when they spawn nix-eval-jobs. Just verify the path is
    // in the expected place.
    assert_eq!(stderr_path.parent(), Some(artifacts.as_path()));
}

#[tokio::test]
async fn prepare_artifacts_dir_is_idempotent() {
    // Operator reruns shouldn't care if the directory already exists.
    let tmp = tempfile::tempdir().unwrap();
    let artifacts = tmp.path().join("reused");
    std::fs::create_dir_all(&artifacts).unwrap();
    let first = prepare_artifacts_dir(&artifacts).unwrap();
    let second = prepare_artifacts_dir(&artifacts).unwrap();
    assert_eq!(first, second);
}
