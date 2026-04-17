//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: job cancellation semantics and idempotency.

mod common;


use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest,
    JobStatus,
};
use sqlx::PgPool;


#[sqlx::test]
async fn ingest_after_cancel_returns_gone(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // Cancel the job
    let url = format!("{}/jobs/{}/cancel", handle.base_url, job.id);
    reqwest::Client::new()
        .delete(&url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let drv = drv_path("aaa", "x");
    let err = client
        .ingest_drv(job.id, &ingest(&drv, "x", &[], true))
        .await
        .unwrap_err();
    match err {
        nix_ci_core::Error::Gone(_) => {}
        other => panic!("expected Gone after cancel, got {other:?}"),
    }
}

#[sqlx::test]
async fn heartbeat_after_cancel_returns_gone(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    reqwest::Client::new()
        .delete(format!("{}/jobs/{}/cancel", handle.base_url, job.id))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let err = client.heartbeat(job.id).await.unwrap_err();
    match err {
        nix_ci_core::Error::Gone(_) => {}
        other => panic!("expected Gone heartbeat after cancel, got {other:?}"),
    }
}

#[sqlx::test]
async fn cancel_while_claim_outstanding(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("can", "cancelme");
    client
        .ingest_drv(job.id, &ingest(&drv, "cancelme", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");

    // Cancel the job while the claim is outstanding.
    reqwest::Client::new()
        .delete(format!("{}/jobs/{}/cancel", handle.base_url, job.id))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Worker finishes build successfully and tries to complete.
    // The submission was removed, so the step lookup fails →
    // ignored=true. No PG writes for this completion.
    let resp = client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 5,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();
    assert!(
        resp.ignored,
        "complete after cancel must be ignored (submission gone)"
    );

    // Job status reflects cancellation.
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Cancelled);
}

#[sqlx::test]
async fn cancel_evicts_in_flight_claims_for_that_job(pool: PgPool) {
    // Production observation: a "Done" deployment had submissions==0
    // but `claims_in_flight==1`. Root cause: when a submission is
    // removed (cancel / fail / graceful Done), in-flight claims
    // tied to that job are NOT evicted — they linger until the
    // claim's deadline (potentially many minutes), inflating the
    // gauge and holding the step weak ref. The reaper's heartbeat
    // path evicts claims; the other terminal paths must too.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("orphan", "pkg");
    client
        .ingest_drv(job.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Worker takes a claim — never completes.
    let _claim = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(handle.dispatcher.claims.len(), 1);

    // Cancel the job. The submission is removed; the in-flight claim
    // tied to this job must be evicted in the same step.
    reqwest::Client::new()
        .delete(format!("{}/jobs/{}/cancel", handle.base_url, job.id))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Both invariants must hold without waiting for the reaper.
    assert_eq!(
        handle.dispatcher.claims.len(),
        0,
        "cancel must evict claims tied to the gone job"
    );
    let snap: nix_ci_core::types::AdminSnapshot =
        reqwest::get(format!("{}/admin/snapshot", handle.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(
        snap.active_claims, 0,
        "claims_in_flight gauge must drop to 0 on cancel"
    );
    assert_eq!(snap.submissions, 0);
}
