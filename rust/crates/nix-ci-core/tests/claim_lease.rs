//! H7.1 regression: a worker whose build runs longer than the initial
//! claim deadline can keep its lease alive via `POST .../extend`, and
//! the coordinator must not reap a claim that the worker has just
//! refreshed. Without this, any long nixpkgs build (webkitgtk,
//! chromium, llvm) would be silently reclaimed, re-run on another
//! worker, and the original worker's `/complete` would be discarded
//! as `ignored:true` — a production incident attributable to nix-ci.

use std::sync::atomic::Ordering;
use std::time::Duration;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::config::ServerConfig;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, IngestBatchRequest, IngestDrvRequest,
};
use sqlx::PgPool;

mod common;

fn drv_path(hash: &str, name: &str) -> String {
    format!("/nix/store/{hash}-{name}.drv")
}

/// The core claim: a claim whose lease has been extended must survive
/// a reaper tick that would otherwise evict it based on the original
/// deadline. Uses a 2s deadline + manual reaper invocation so the test
/// is deterministic and fast.
#[sqlx::test]
async fn extend_keeps_claim_alive_past_original_deadline(pool: PgPool) {
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = 2;
        cfg.reaper_interval_secs = 1;
    })
    .await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("aaa", "long-build");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![IngestDrvRequest {
                    drv_path: drv.clone(),
                    drv_name: "long-build".into(),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                }],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");

    // Wait past the original 2s deadline, refreshing once at the halfway
    // mark. The refresh pushes the deadline out by another full window.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let ext = client
        .extend_claim(job.id, c.claim_id)
        .await
        .expect("extend")
        .expect("extend returned Some");
    // The new wall-clock deadline must be in the future.
    assert!(
        ext.deadline > chrono::Utc::now(),
        "extended deadline {} must be in the future",
        ext.deadline
    );

    tokio::time::sleep(Duration::from_millis(1_500)).await;
    // After 2.5s total, the original deadline (2s) has passed. Run the
    // reaper — it must NOT evict, because the extension moved the
    // deadline forward.
    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);
    assert_eq!(
        handle.dispatcher.claims.len(),
        1,
        "extended claim must survive the reaper; instead found {} claims",
        handle.dispatcher.claims.len()
    );

    // Complete succeeds with ignored=false — the worker's result is
    // accepted, which is the entire point of the lease mechanism.
    let resp = client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 2_500,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();
    assert!(
        !resp.ignored,
        "after a lease extension the /complete MUST be accepted (ignored=false)"
    );

    // And the lease-extension counter moved: ops dashboards watch this
    // to distinguish "workers refreshing healthily" from "workers
    // silently expiring".
    let extensions = handle
        .dispatcher
        .metrics
        .inner
        .claim_lease_extensions
        .get();
    assert_eq!(extensions, 1, "exactly one extension recorded");
}

/// Without the extension, the reaper evicts the claim and a subsequent
/// /complete returns ignored=true. This is the DANGEROUS path — the
/// extension mechanism is what prevents this for long builds.
#[sqlx::test]
async fn without_extend_reaper_evicts_long_running_claim(pool: PgPool) {
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = 1;
        cfg.reaper_interval_secs = 1;
    })
    .await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("bbb", "no-refresh");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![IngestDrvRequest {
                    drv_path: drv.clone(),
                    drv_name: "no-refresh".into(),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                }],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");

    // Force the deadline into the past to simulate a build that took
    // too long without ever refreshing the lease — the behavior the
    // H7.1 refresh mechanism was introduced to prevent.
    {
        let claim = handle
            .dispatcher
            .claims
            .take(c.claim_id)
            .expect("claim present");
        *claim.deadline.lock() = std::time::Instant::now() - Duration::from_secs(1);
        handle.dispatcher.claims.insert(claim);
    }
    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);
    assert_eq!(
        handle.dispatcher.claims.len(),
        0,
        "expired non-extended claim must be reaped"
    );

    let resp = client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 10_000,
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
        "stale completion after reaper eviction must be ignored"
    );
}

/// A `/extend` against an unknown claim_id returns 410 Gone. The
/// worker uses this to stop refreshing: any Gone means "another worker
/// has taken over" or "the coordinator restarted." Either way,
/// continuing to refresh is a waste and the worker should exit the
/// refresh loop cleanly.
#[sqlx::test]
async fn extend_unknown_claim_returns_gone(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // ClaimId that was never issued — client collapses 410 → Ok(None).
    let never_issued = nix_ci_core::types::ClaimId::new();
    let resp = client
        .extend_claim(job.id, never_issued)
        .await
        .expect("extend call itself must not error");
    assert!(
        resp.is_none(),
        "extend against unknown claim must signal Gone (None)"
    );
}

/// Extending a completed claim is a no-op (Ok(None)). This is the
/// steady-state case where a refresh races with the final complete:
/// the last refresh before complete succeeds, the next one reports
/// Gone, and the worker's refresh loop exits.
#[sqlx::test]
async fn extend_after_complete_returns_gone(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("ccc", "fast-build");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![IngestDrvRequest {
                    drv_path: drv.clone(),
                    drv_name: "fast-build".into(),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                }],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
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
    let resp = client.extend_claim(job.id, c.claim_id).await.unwrap();
    assert!(resp.is_none(), "extend after complete must return Gone");

    // And no extension was counted (the Ok(None) branch increments
    // nothing — it's not an extension, it's a "stop refreshing" signal).
    assert_eq!(
        handle
            .dispatcher
            .metrics
            .inner
            .claim_lease_extensions
            .get(),
        0
    );
}

/// Plausibility check on the counter: each successful extend bumps
/// exactly one count, regardless of how many claims are live. The
/// counter is the operational signal for "is the lease mechanism
/// actually being exercised?"
#[sqlx::test]
async fn extend_counter_increments_per_success(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("ddd", "count-me");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![IngestDrvRequest {
                    drv_path: drv.clone(),
                    drv_name: "count-me".into(),
                    system: "x86_64-linux".into(),
                    required_features: vec![],
                    input_drvs: vec![],
                    is_root: true,
                    attr: None,
                }],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    for _ in 0..5 {
        client
            .extend_claim(job.id, c.claim_id)
            .await
            .unwrap()
            .expect("still alive");
    }
    let observed = handle
        .dispatcher
        .metrics
        .inner
        .claim_lease_extensions
        .get();
    assert_eq!(
        observed, 5,
        "counter must match the number of successful extends"
    );
    // Prevent an unused-import warning if Ordering drifts out of use.
    let _ = Ordering::Relaxed;
}
