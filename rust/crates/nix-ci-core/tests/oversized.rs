//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: oversized-payload caps (per-job drv cap, body size, identifier lengths).

mod common;


use common::{drv_path, ingest};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CreateJobRequest, IngestBatchRequest,
    JobStatus,
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
        .ingest_batch(job.id, &IngestBatchRequest { drvs: batch1, eval_errors: Vec::new() })
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
        .ingest_batch(job.id, &IngestBatchRequest { drvs: batch2, eval_errors: Vec::new() })
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
        .ingest_batch(job.id, &IngestBatchRequest { drvs: retry, eval_errors: Vec::new() })
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
        .ingest_batch(job.id, &IngestBatchRequest { drvs: big, eval_errors: Vec::new() })
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
