//! H7.4 regression: per-attr eval errors from nix-eval-jobs must
//! surface in the terminal `JobStatusResponse.eval_errors` and force
//! the job to `Failed` even when every successfully-evaluated attr
//! built cleanly. Previously these were logged on the runner side
//! and invisible to the caller, so a user with one broken overlay
//! attr saw a green CCI build.

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, EvalError, IngestBatchRequest, IngestDrvRequest, JobStatus,
};
use sqlx::PgPool;

mod common;

fn drv_path(hash: &str, name: &str) -> String {
    format!("/nix/store/{hash}-{name}.drv")
}

fn ingest_req(drv: &str, name: &str, is_root: bool) -> IngestDrvRequest {
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

/// Baseline: submit one good drv, one eval error, build the drv,
/// seal. Expected: status=Failed (because eval errors present),
/// eval_errors has the one entry, failures is empty (no drv failed).
#[sqlx::test]
async fn eval_errors_force_failed_status_even_when_all_builds_succeed(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let good = drv_path("aaa", "good");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![ingest_req(&good, "good", true)],
                eval_errors: vec![EvalError {
                    attr: "packages.broken".into(),
                    error: "undefined variable 'foo'".into(),
                }],
            },
        )
        .await
        .unwrap();
    // Seal + complete the one good drv.
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    client.seal(job.id).await.unwrap();
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 1,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();

    let status = client.status(job.id).await.unwrap();
    // The whole point: eval_errors must flip the job to Failed.
    assert_eq!(
        status.status,
        JobStatus::Failed,
        "eval errors must make the job Failed even when all builds pass"
    );
    assert_eq!(status.eval_errors.len(), 1);
    assert_eq!(status.eval_errors[0].attr, "packages.broken");
    assert!(status.eval_errors[0].error.contains("undefined variable"));
    // And failures stays empty — no drv actually failed.
    assert!(status.failures.is_empty());
    // `eval_error` (singular) is for coordinator-originated causes
    // (cycle, oversized), not runner-reported ones.
    assert!(status.eval_error.is_none());
}

/// Live-status path: while the job is open, `GET /jobs/{id}` must
/// surface the eval_errors so the runner can display them in
/// progress output without waiting for seal.
#[sqlx::test]
async fn eval_errors_visible_before_seal(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![],
                eval_errors: vec![
                    EvalError {
                        attr: "a".into(),
                        error: "e1".into(),
                    },
                    EvalError {
                        attr: "b".into(),
                        error: "e2".into(),
                    },
                ],
            },
        )
        .await
        .unwrap();
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.eval_errors.len(), 2);
    let attrs: Vec<_> = status.eval_errors.iter().map(|e| &e.attr).collect();
    assert!(attrs.iter().any(|a| *a == "a"));
    assert!(attrs.iter().any(|a| *a == "b"));
}

/// Dedup on attr: reporting the same attr twice doesn't duplicate
/// the entry. Protects against runner-side retries that re-send the
/// same eval_errors payload.
#[sqlx::test]
async fn eval_errors_dedup_on_attr(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    for _ in 0..3 {
        client
            .ingest_batch(
                job.id,
                &IngestBatchRequest {
                    drvs: vec![],
                    eval_errors: vec![EvalError {
                        attr: "same-attr".into(),
                        error: "same-error".into(),
                    }],
                },
            )
            .await
            .unwrap();
    }
    let status = client.status(job.id).await.unwrap();
    assert_eq!(
        status.eval_errors.len(),
        1,
        "same-attr must not duplicate across retries"
    );
}

/// Cap enforcement: submitters that misbehave (or a truly broken
/// overlay with thousands of errors) must not bloat the JSONB snapshot.
/// Past the cap the list stops growing and a synthetic `<truncated>`
/// entry is appended.
#[sqlx::test]
async fn eval_errors_capped_with_truncated_marker(pool: PgPool) {
    use nix_ci_core::dispatch::submission::EVAL_ERRORS_CAP;
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // Send cap + 10 distinct attrs across 3 batches — the last 10
    // should be dropped, and a single <truncated> marker appended.
    let total: usize = EVAL_ERRORS_CAP + 10;
    let mut sent = 0usize;
    while sent < total {
        let n = 100.min(total - sent);
        let errs: Vec<EvalError> = (0..n)
            .map(|i| EvalError {
                attr: format!("attr-{}", sent + i),
                error: "x".into(),
            })
            .collect();
        client
            .ingest_batch(
                job.id,
                &IngestBatchRequest {
                    drvs: vec![],
                    eval_errors: errs,
                },
            )
            .await
            .unwrap();
        sent += n;
    }
    let status = client.status(job.id).await.unwrap();
    // Cap + 1 for the marker entry.
    assert_eq!(status.eval_errors.len(), EVAL_ERRORS_CAP + 1);
    assert_eq!(status.eval_errors.last().unwrap().attr, "<truncated>");
    assert!(status
        .eval_errors
        .last()
        .unwrap()
        .error
        .contains(&EVAL_ERRORS_CAP.to_string()));
}

/// Backward compatibility: the serde `#[serde(default)]` on
/// IngestBatchRequest.eval_errors means older clients can still POST
/// `{ "drvs": [...] }` without an eval_errors field, and it'll
/// deserialize to empty. Protects the wire contract as we evolve.
#[sqlx::test]
async fn old_client_without_eval_errors_field_still_works(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = reqwest::Client::new();
    // Create the job through the typed client — this path is stable.
    let coord = CoordinatorClient::new(handle.base_url.clone());
    let job = coord
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // Now hand-post a body that omits the eval_errors field entirely.
    let body = serde_json::json!({
        "drvs": [{
            "drv_path": drv_path("zzz", "legacy"),
            "drv_name": "legacy",
            "system": "x86_64-linux",
            "is_root": true,
        }]
    });
    let resp = client
        .post(format!("{}/jobs/{}/drvs/batch", handle.base_url, job.id))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "legacy payload without eval_errors must still be accepted: {}",
        resp.status()
    );
    let status = coord.status(job.id).await.unwrap();
    assert!(status.eval_errors.is_empty());
}
