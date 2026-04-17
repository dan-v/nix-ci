//! H10.1 + H10.2: drain and fence operator endpoints.
//!
//! Drain is the "rolling upgrade" primitive: POST /admin/drain stops
//! accepting new jobs / new claims, but existing in-flight work
//! finishes normally. Fence is the per-worker complement: a specific
//! host can be taken out of rotation without affecting the rest.

use std::time::Duration;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CreateJobRequest, IngestBatchRequest, IngestDrvRequest};
use sqlx::PgPool;

mod common;

fn drv_path(hash: &str, name: &str) -> String {
    format!("/nix/store/{hash}-{name}.drv")
}

fn ingest(drv: &str, name: &str, is_root: bool) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root,
        attr: None,
    }
}

/// After drain, POST /jobs returns 503 for new external_refs, and
/// GET /claim returns 204 immediately without waiting for the long-
/// poll deadline. Existing submissions, seals, completes, and
/// extends all continue to work.
#[sqlx::test]
async fn drain_blocks_new_jobs_and_claims(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let http = reqwest::Client::new();

    // Create a job before drain — this one should remain fully
    // usable through drain.
    let preexisting = client
        .create_job(&CreateJobRequest {
            external_ref: Some("pre".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("aaa", "pre");
    client
        .ingest_batch(
            preexisting.id,
            &IngestBatchRequest {
                drvs: vec![ingest(&drv, "pre", true)],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();

    // Drain.
    let resp = http
        .post(format!("{}/admin/drain", handle.base_url))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["draining"], true);

    // New job creation rejected with 503.
    let new_create = http
        .post(format!("{}/jobs", handle.base_url))
        .json(&serde_json::json!({"external_ref": "new"}))
        .send()
        .await
        .unwrap();
    assert_eq!(new_create.status().as_u16(), 503);

    // Existing external_ref still resolves (idempotent lookup is a
    // no-new-work path) — important for retries that lost their
    // response right before drain.
    let retry = http
        .post(format!("{}/jobs", handle.base_url))
        .json(&serde_json::json!({"external_ref": "pre"}))
        .send()
        .await
        .unwrap();
    assert!(retry.status().is_success());
    let body: serde_json::Value = retry.json().await.unwrap();
    assert_eq!(body["id"], preexisting.id.0.to_string());

    // Claim returns 204 fast (not 30s worth of long-poll) — measured
    // to prove the drain shortcut worked rather than hitting the wait
    // deadline coincidentally.
    let start = std::time::Instant::now();
    let c = client
        .claim(preexisting.id, "x86_64-linux", &[], 10)
        .await
        .unwrap();
    assert!(c.is_none());
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "drain must short-circuit claim to 204 within ~ms, not wait 10s; took {elapsed:?}"
    );

    // Fleet claim same shortcut.
    let start = std::time::Instant::now();
    let c = client.claim_any("x86_64-linux", &[], 10).await.unwrap();
    assert!(c.is_none());
    assert!(std::time::Instant::now() - start < Duration::from_secs(2));

    // GET /admin/drain — polling variant returns the same snapshot
    // without flipping anything.
    let status = http
        .get(format!("{}/admin/drain", handle.base_url))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = status.json().await.unwrap();
    assert_eq!(body["draining"], true);
    // There's exactly one submission left (the preexisting one).
    assert_eq!(body["open_submissions"], 1);
    assert_eq!(body["in_flight_claims"], 0);
}

/// A fenced worker_id gets 204 on /claim even when runnable drvs
/// exist; other workers (different worker_id, or no worker_id at
/// all) continue to claim normally.
#[sqlx::test]
async fn fenced_worker_is_skipped_others_continue(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let http = reqwest::Client::new();

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let a = drv_path("a", "a");
    let b = drv_path("b", "b");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![ingest(&a, "a", true), ingest(&b, "b", true)],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();

    // Fence worker-evil.
    let resp = http
        .post(format!(
            "{}/admin/fence?worker_id=worker-evil",
            handle.base_url
        ))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    let fenced: Vec<String> = serde_json::from_value(body["fenced"].clone()).unwrap();
    assert_eq!(fenced, vec!["worker-evil".to_string()]);

    // Fenced worker: 204 regardless of runnable drvs.
    let start = std::time::Instant::now();
    let fenced_claim = client
        .claim_as_worker(job.id, "x86_64-linux", &[], 2, Some("worker-evil"))
        .await
        .unwrap();
    assert!(fenced_claim.is_none());
    assert!(
        std::time::Instant::now() - start < Duration::from_secs(1),
        "fence must short-circuit claim"
    );

    // Different worker: claims normally.
    let other_claim = client
        .claim_as_worker(job.id, "x86_64-linux", &[], 2, Some("worker-good"))
        .await
        .unwrap();
    assert!(other_claim.is_some(), "non-fenced worker must still claim");

    // Unfence and verify the evil worker can claim again.
    let resp = http
        .delete(format!(
            "{}/admin/fence?worker_id=worker-evil",
            handle.base_url
        ))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let after_claim = client
        .claim_as_worker(job.id, "x86_64-linux", &[], 2, Some("worker-evil"))
        .await
        .unwrap();
    assert!(
        after_claim.is_some(),
        "unfenced worker must be able to claim"
    );
}

/// Fencing the same worker twice is a no-op (idempotent). Listing
/// shows exactly one entry. Covers the admin-UI-retry case.
#[sqlx::test]
async fn fence_is_idempotent(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let http = reqwest::Client::new();
    for _ in 0..3 {
        http.post(format!(
            "{}/admin/fence?worker_id=dup-worker",
            handle.base_url
        ))
        .send()
        .await
        .unwrap();
    }
    let list = http
        .get(format!("{}/admin/fence", handle.base_url))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = list.json().await.unwrap();
    let fenced: Vec<String> = serde_json::from_value(body["fenced"].clone()).unwrap();
    assert_eq!(fenced, vec!["dup-worker".to_string()]);
}

/// Malformed fence request (empty worker_id) returns 400.
#[sqlx::test]
async fn fence_rejects_empty_worker_id(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("{}/admin/fence?worker_id=", handle.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
}
