//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: oversized-payload caps (per-job drv cap, body size, identifier lengths).

mod common;

use common::{drv_path, ingest};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, ErrorCategory, IngestBatchRequest, JobStatus,
    MAX_LOG_TAIL_BYTES,
};
use sqlx::PgPool;

/// When a batch would push a job's member count over `max_drvs_per_job`,
/// the coordinator must auto-fail the job with `eval_too_large` and
/// return 413. This is the runaway-eval guard: nothing else can stop a
/// 10M-drv accidental closure from OOMing the coordinator.
#[sqlx::test]
async fn ingest_drv_cap_auto_fails_job(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.submission_warn_threshold = 4;
        cfg.max_drvs_per_job = Some(8); // very small cap for test speed
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

    // First batch: 6 drvs (under cap).
    let batch1: Vec<_> = (0..6u32)
        .map(|i| {
            ingest(
                &drv_path(&format!("h{i:04}a"), &format!("d{i}")),
                "d",
                &[],
                true,
            )
        })
        .collect();
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: batch1,
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();

    // Second batch: 3 more → 6+3=9 > 8 cap → must 413 + auto-fail.
    let batch2: Vec<_> = (10..13u32)
        .map(|i| {
            ingest(
                &drv_path(&format!("h{i:04}b"), &format!("d{i}")),
                "d",
                &[],
                true,
            )
        })
        .collect();
    let err = client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: batch2,
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap_err();
    match err {
        nix_ci_core::Error::PayloadTooLarge(ref msg) => {
            assert!(
                msg.contains("eval_too_large"),
                "expected eval_too_large, got: {msg}"
            );
        }
        other => panic!("expected PayloadTooLarge, got {other:?}"),
    }

    // The job must be terminal=Failed with the sentinel eval_error set
    // (so the caller can distinguish this from a regular build failure).
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Failed);
    assert!(
        status
            .eval_error
            .as_deref()
            .unwrap_or("")
            .contains("eval_too_large"),
        "expected eval_too_large in eval_error, got: {:?}",
        status.eval_error
    );

    // Subsequent ingest must now be rejected with Gone (terminal), not
    // PayloadTooLarge — proves the terminal write actually happened.
    let retry: Vec<_> = (20..21u32)
        .map(|i| {
            ingest(
                &drv_path(&format!("h{i:04}c"), &format!("d{i}")),
                "d",
                &[],
                true,
            )
        })
        .collect();
    match client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: retry,
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap_err()
    {
        nix_ci_core::Error::Gone(_) => {}
        other => panic!("expected Gone after auto-fail, got {other:?}"),
    }
}

/// With the cap disabled (None), huge ingests succeed. Guards against
/// a refactor that accidentally flips the default off-switch.
#[sqlx::test]
async fn ingest_drv_cap_disabled_allows_large_batch(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.submission_warn_threshold = 10;
        cfg.max_drvs_per_job = None;
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

    let big: Vec<_> = (0..50u32)
        .map(|i| {
            ingest(
                &drv_path(&format!("h{i:04}u"), &format!("d{i}")),
                "d",
                &[],
                true,
            )
        })
        .collect();
    let resp = client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: big,
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(resp.new_drvs, 50);
}

#[sqlx::test]
async fn oversized_batch_rejected_with_413(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_request_body_bytes = 16 * 1024; // 16 KiB — tiny for test speed
    })
    .await;
    let client = reqwest::Client::new();
    let job_resp: nix_ci_core::types::CreateJobResponse = client
        .post(format!("{}/jobs", handle.base_url))
        .json(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // 200 KiB of payload — well over the configured 16 KiB cap.
    let big = "x".repeat(200 * 1024);
    let resp = client
        .post(format!(
            "{}/jobs/{}/drvs/batch",
            handle.base_url, job_resp.id
        ))
        .header("content-type", "application/json")
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "body above max_request_body_bytes must return 413"
    );
}

/// external_ref above max_identifier_bytes must be rejected with 400.
/// Prevents a broken / hostile client from bloating DB rows, log lines,
/// and JSONB snapshots with megabytes of identifier.
#[sqlx::test]
async fn create_rejects_oversized_external_ref(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_identifier_bytes = 32; // tight for test speed
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let big = "x".repeat(1024);
    let err = client
        .create_job(&CreateJobRequest {
            external_ref: Some(big),
            ..Default::default()
        })
        .await
        .unwrap_err();
    match err {
        nix_ci_core::Error::BadRequest(ref msg) => {
            assert!(
                msg.contains("external_ref") && msg.contains("max_identifier_bytes"),
                "expected external_ref size diagnostic, got: {msg}"
            );
        }
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

/// worker_id above max_identifier_bytes must be rejected with 400 on
/// /claim endpoints.
#[sqlx::test]
async fn claim_rejects_oversized_worker_id(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_identifier_bytes = 32;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let huge_worker = "w".repeat(1024);
    let err = client
        .claim_as_worker(job.id, "x86_64-linux", &[], 1, Some(&huge_worker))
        .await
        .unwrap_err();
    match err {
        nix_ci_core::Error::BadRequest(_) => {}
        other => panic!("expected BadRequest for oversized worker_id, got {other:?}"),
    }
}

/// Oversized `attr` in an ingest batch must be counted as `errored`
/// (batch continues for the other drvs), not crash the batch.
#[sqlx::test]
async fn ingest_rejects_oversized_attr(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_identifier_bytes = 32;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let mut good = ingest(&drv_path("gooddrva1", "g"), "g", &[], true);
    let mut bad = ingest(&drv_path("baddrvbb1", "b"), "b", &[], true);
    bad.attr = Some("a".repeat(1024));
    good.attr = Some("packages.x86_64-linux.hello".into());
    let resp = client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![good, bad],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(resp.new_drvs, 1, "the good drv must still be ingested");
    assert_eq!(resp.errored, 1, "oversized attr must count as errored");
}

/// R-TERMINAL-JSONB-BOUNDED: a catastrophic-failure job whose workers
/// upload realistic 64 KiB log_tails must produce a persisted
/// `jobs.result` JSONB under 1 MiB. Without log_tail stripping past
/// the triage head, 30 originating failures × 64 KiB tail = ~1.9 MiB
/// of JSONB per row — over the bar and slow to re-read on every status
/// poll.
///
/// The existing scale test `scale_failures_vec_under_catastrophic_job`
/// submits `log_tail: None` from its mock worker, which hides this
/// regression entirely. Real workers always attach a tail (see
/// `runner::worker::handle_failure`), so this smaller variant with
/// real tails is the per-PR enforcement gate.
#[sqlx::test]
async fn catastrophic_failure_snapshot_stays_under_1mib_with_real_log_tails(pool: PgPool) {
    const N_FAILING: usize = 30;

    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    // 30 independent leaf drvs — no shared deps, each fails on its own
    // worker-reported BuildFailure. Shape is "bad overlay broke 30
    // packages that all eval cleanly" — every attr has its own
    // originating failure with a real log_tail.
    let mut drvs = Vec::with_capacity(N_FAILING);
    for i in 0..N_FAILING {
        drvs.push(ingest(
            &drv_path(&format!("fat{i:02}"), &format!("fat{i}")),
            &format!("fat{i}"),
            &[],
            true,
        ));
    }
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs,
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // 64 KiB of 'x' — matches what a realistic failing build's stderr
    // would look like at the worker's tail cap.
    let big_tail = "x".repeat(MAX_LOG_TAIL_BYTES);

    // Claim + fail each drv with a real log_tail.
    for _ in 0..N_FAILING {
        let c = match client.claim(job.id, "x86_64-linux", &[], 5).await {
            Ok(Some(c)) => c,
            Ok(None) => panic!("drv must be claimable"),
            Err(nix_ci_core::Error::Gone(_)) => break, // job went terminal early
            Err(e) => panic!("unexpected claim error: {e}"),
        };
        let resp = client
            .complete(
                job.id,
                c.claim_id,
                &CompleteRequest {
                    success: false,
                    duration_ms: 1,
                    exit_code: Some(1),
                    error_category: Some(ErrorCategory::BuildFailure),
                    error_message: Some(format!("{} failed", c.drv_hash)),
                    log_tail: Some(big_tail.clone()),
                },
            )
            .await
            .unwrap();
        // A BuildFailure on the last drv terminalizes the job — that
        // response is still Ok + ignored=false for the winning caller.
        let _ = resp;
    }

    // Wait for terminal state.
    let mut status = client.status(job.id).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !status.status.is_terminal() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        status = client.status(job.id).await.unwrap();
    }
    assert_eq!(
        status.status,
        JobStatus::Failed,
        "catastrophic-failure job must terminate Failed"
    );

    // Read the persisted JSONB directly — that's the thing the bar
    // promises. An in-memory status response could look fine while the
    // durable row is 32 MiB.
    let row: (Option<serde_json::Value>,) =
        sqlx::query_as("SELECT result FROM jobs WHERE id = $1")
            .bind(job.id.0)
            .fetch_one(&handle.pool)
            .await
            .unwrap();
    let result = row.0.expect("terminal jobs must carry a result JSONB");
    let failures = result
        .get("failures")
        .and_then(|v| v.as_array())
        .expect("result.failures must be an array");
    let result_bytes = serde_json::to_vec(&result).unwrap().len();

    // Bar: < 1 MiB. Without the fix (every failure keeps its 64 KiB
    // tail) we'd see ~30 × 64 KiB ≈ 1.92 MiB.
    assert!(
        result_bytes < 1_048_576,
        "terminal JSONB is {result_bytes} bytes (> 1 MiB); log_tail stripping regressed"
    );

    // Lower-bound sanity: at least the first few failures kept their
    // full tails so operators get real triage context. Without SOME
    // tails the fix would have over-stripped.
    let with_tail = failures
        .iter()
        .filter(|f| f.get("log_tail").is_some_and(|v| !v.is_null()))
        .count();
    assert!(
        with_tail > 0,
        "at least one failure in the snapshot must retain its log_tail for triage"
    );
    // Head is capped at 10 by the cap_failures implementation.
    assert!(
        with_tail <= 10,
        "snapshot retained more tails than the head allows: {with_tail}"
    );
}
