//! Claim long-poll, deadline math, priority, fleet caps, eviction.

mod common;

use std::time::Duration;

use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, IngestBatchRequest, IngestDrvRequest};
use sqlx::PgPool;

#[sqlx::test]
async fn worker_without_required_feature_never_claims(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("kvm", "vm-test");
    let req = IngestDrvRequest {
        drv_path: drv,
        drv_name: "vm-test".into(),
        system: "x86_64-linux".into(),
        required_features: vec!["kvm".into()],
        input_drvs: vec![],
        is_root: true,
        attr: None,
    };
    client.ingest_drv(job.id, &req).await.unwrap();
    client.seal(job.id).await.unwrap();

    // Worker without kvm: gets 204
    let none = client.claim(job.id, "x86_64-linux", &[], 1).await.unwrap();
    assert!(none.is_none(), "worker without kvm must not claim");

    // Worker with kvm: gets the drv
    let some = client
        .claim(job.id, "x86_64-linux", &["kvm".into()], 2)
        .await
        .unwrap();
    assert!(some.is_some(), "worker with kvm must claim");
}

#[sqlx::test]
async fn multi_system_worker_claims_any_matching_system(pool: PgPool) {
    // A single host (e.g. a build server with cross-compilation
    // toolchains) can advertise multiple systems by passing a
    // comma-separated `system` query. The coordinator walks the list
    // in order and returns the first runnable drv from any of them.
    //
    // Regression guard: matches the giant-nixpkgs-overlay contract
    // where one runner slot should be able to serve drvs targeting
    // x86_64-linux, aarch64-linux, and cross targets simultaneously.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // Ingest an aarch64-linux drv — the worker's "native" preference
    // won't match, but its second-preference will.
    client
        .ingest_drv(
            job.id,
            &IngestDrvRequest {
                drv_path: drv_path("aa", "arm-drv"),
                drv_name: "arm-drv".into(),
                system: "aarch64-linux".into(),
                required_features: vec![],
                input_drvs: vec![],
                is_root: true,
                attr: None,
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Single-system worker advertising native first, cross second.
    let resp = client
        .claim(job.id, "x86_64-linux,aarch64-linux", &[], 3)
        .await
        .unwrap()
        .expect("multi-system worker must claim the aarch64 drv");
    assert_eq!(resp.drv_path, drv_path("aa", "arm-drv"));
}

#[sqlx::test]
async fn multi_system_claim_prefers_first_listed_system(pool: PgPool) {
    // Order matters: worker passes `[native, cross]` and both systems
    // have ready drvs. The coordinator must hand out the native one
    // first. Guards against the trivial implementation that iterates
    // HashMap entries in arbitrary order.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let native = IngestDrvRequest {
        drv_path: drv_path("nn", "native"),
        drv_name: "native".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        attr: None,
    };
    let cross = IngestDrvRequest {
        drv_path: drv_path("cc", "cross"),
        drv_name: "cross".into(),
        system: "aarch64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        attr: None,
    };
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![native, cross],
                eval_errors: vec![],
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let first = client
        .claim(job.id, "x86_64-linux,aarch64-linux", &[], 2)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(
        first.drv_path,
        drv_path("nn", "native"),
        "x86_64-linux is preferred over aarch64-linux"
    );
    let second = client
        .claim(job.id, "x86_64-linux,aarch64-linux", &[], 2)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(second.drv_path, drv_path("cc", "cross"));
}

#[sqlx::test]
async fn worker_with_wrong_system_never_claims(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("linuxonly", "kernel");
    client
        .ingest_drv(
            job.id,
            &IngestDrvRequest {
                drv_path: drv,
                drv_name: "kernel".into(),
                system: "x86_64-linux".into(),
                required_features: vec![],
                input_drvs: vec![],
                is_root: true,
                attr: None,
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Worker on darwin: nothing to claim
    let none = client
        .claim(job.id, "aarch64-darwin", &[], 1)
        .await
        .unwrap();
    assert!(none.is_none(), "darwin worker must not claim linux drv");
}

#[sqlx::test]
async fn terminal_jobs_removed_from_in_memory_map(pool: PgPool) {
    // Regression guard: check_and_publish_terminal must drop the
    // submission from dispatcher.submissions. Before this fix,
    // successfully-completed jobs stayed in memory indefinitely,
    // leaking one Arc<Submission> + its member graph per job.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Drive 5 jobs to successful completion, then check the map size.
    for i in 0..5 {
        let job = client
            .create_job(&CreateJobRequest {
                external_ref: Some(format!("terminal-cleanup-{i}")),
                ..Default::default()
            })
            .await
            .unwrap();
        let drv = drv_path(&format!("t{i:02}"), "single");
        client
            .ingest_drv(job.id, &ingest(&drv, "single", &[], true))
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
        for _ in 0..20 {
            if client.status(job.id).await.unwrap().status.is_terminal() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // Give the async terminal-publish a moment to finish the removal.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let snap: nix_ci_core::types::AdminSnapshot =
        reqwest::get(format!("{}/admin/snapshot", handle.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(
        snap.submissions, 0,
        "all 5 terminal submissions must be removed from the map"
    );
}

#[sqlx::test]
async fn shutdown_terminates_in_flight_claim_longpoll(pool: PgPool) {
    // A claim long-poll waits on Dispatcher::notify with a wait-deadline.
    // When the server shuts down mid-poll the request must return
    // promptly rather than hang past the long-poll deadline. This
    // exercises the axum graceful-shutdown path against an open
    // connection.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Kick off a claim with a 30s wait — no drvs, so it would sit at
    // the long-poll for the full deadline.
    let base = handle.base_url.clone();
    let job_id = job.id;
    let claim_task = tokio::spawn(async move {
        let c = CoordinatorClient::new(&base);
        c.claim(job_id, "x86_64-linux", &[], 30).await
    });
    // Let the handler enter its long-poll before we drop the server.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Dropping the handle fires the oneshot shutdown; axum drains.
    drop(handle);

    // The claim must complete (Ok or Err) within a tight window —
    // NOT the full 30s wait deadline.
    let res = tokio::time::timeout(Duration::from_secs(3), claim_task)
        .await
        .expect("claim task must unblock on shutdown");
    // Either the request was cleanly returned with None/Gone/etc.,
    // or the connection was dropped and reqwest errored — both are
    // acceptable outcomes. What's NOT acceptable is the timeout above.
    let _ = res;
}

#[sqlx::test]
async fn claim_response_deadline_matches_config(pool: PgPool) {
    // The ClaimResponse carries a wall-clock `deadline`. It must be
    // `~now + claim_deadline_secs`. A sign flip (`+` → `-`) would put
    // it in the past — worker's heartbeat logic would then see a
    // "stale" claim instantly.
    use nix_ci_core::config::ServerConfig;
    const DEADLINE_SECS: u64 = 42;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = DEADLINE_SECS;
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
    let drv = drv_path("dl", "pkg");
    client
        .ingest_drv(job.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let before = chrono::Utc::now();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    let after = chrono::Utc::now();
    // deadline should lie in the future, roughly [before + DEADLINE, after + DEADLINE + slack].
    let lo = before + chrono::Duration::seconds(DEADLINE_SECS as i64);
    let hi = after + chrono::Duration::seconds(DEADLINE_SECS as i64 + 2);
    assert!(
        c.deadline >= lo && c.deadline <= hi,
        "claim deadline {} outside expected window [{}, {}]",
        c.deadline,
        lo,
        hi
    );

    // And the in-memory `ActiveClaim::deadline` (Instant) must not be
    // already expired — a `+` → `-` flip there would cause the reaper
    // to evict the claim on its next tick. Assert expired_ids(now) is
    // empty.
    assert!(
        handle
            .dispatcher
            .claims
            .expired_ids(tokio::time::Instant::now())
            .is_empty(),
        "freshly-issued claim must not be already past its in-memory deadline"
    );
}

#[sqlx::test]
async fn claim_longpoll_returns_204_after_wait_deadline(pool: PgPool) {
    // With no runnable drvs, a claim with wait=N must return 204 after
    // approximately N seconds — NOT early, NOT late. Guards the
    // `start + Duration::from_secs(q.wait)` deadline math and the
    // `Instant::now() >= deadline` exit check.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    // Deliberately do NOT seal — an empty seal immediately terminates
    // the job (separate test) and we need the submission to stay Live
    // so the long-poll can exercise its deadline.

    let start = tokio::time::Instant::now();
    let res = client
        .claim(job.id, "x86_64-linux", &[], 1) // wait = 1 sec
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(res.is_none(), "no runnable drvs → 204 None");
    // A mutant that makes `deadline = start - wait` would exit
    // immediately; assert we actually waited close to the full second.
    assert!(
        elapsed >= Duration::from_millis(900),
        "claim returned too early: elapsed={elapsed:?} (expected ~1s)"
    );
    // A mutant that makes the exit check `Instant::now() < deadline`
    // would hang forever; assert we returned within a reasonable bound.
    assert!(
        elapsed < Duration::from_secs(4),
        "claim overshot its wait: elapsed={elapsed:?} (expected ≲1s)"
    );
}

#[sqlx::test]
async fn find_job_by_external_ref_roundtrips(pool: PgPool) {
    // `create` uses `find_job_by_external_ref` to make POST /jobs
    // idempotent. A mutant that replaces the function body with
    // `Ok(None)` would silently mint a new id on every retry with the
    // same external_ref, breaking idempotency.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let first = client
        .create_job(&CreateJobRequest {
            external_ref: Some("idempotent-ref-xyz".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let second = client
        .create_job(&CreateJobRequest {
            external_ref: Some("idempotent-ref-xyz".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        first.id, second.id,
        "create with identical external_ref must return the same id"
    );
}

/// Fleet claim must scan higher-priority jobs first, then FIFO within a
/// tier. Without priority support, an urgent hotfix would wait behind a
/// large already-running batch job.
#[sqlx::test]
async fn fleet_claim_honors_priority(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Create low-priority job first (so FIFO by created_at alone would
    // pick it).
    let low = client
        .create_job(&CreateJobRequest {
            external_ref: Some("low-p".into()),
            priority: 0,
            max_workers: None,
            claim_deadline_secs: None,
        })
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    let high = client
        .create_job(&CreateJobRequest {
            external_ref: Some("high-p".into()),
            priority: 100,
            max_workers: None,
            claim_deadline_secs: None,
        })
        .await
        .unwrap();

    let low_drv = drv_path("lowpria01", "low");
    let high_drv = drv_path("highpri01", "high");
    client
        .ingest_drv(low.id, &ingest(&low_drv, "low", &[], true))
        .await
        .unwrap();
    client
        .ingest_drv(high.id, &ingest(&high_drv, "high", &[], true))
        .await
        .unwrap();

    // Fleet claim: must hand back the high-priority drv first even
    // though low was created earlier.
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/claim?wait=2&system=x86_64-linux",
            handle.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: nix_ci_core::types::ClaimResponse = resp.json().await.unwrap();
    assert_eq!(
        body.drv_path, high_drv,
        "high-priority drv must be claimed first; got {}",
        body.drv_path
    );
}

/// max_workers caps per-job concurrency. With cap=1 and 2 claimable
/// drvs, only one claim at a time should be outstanding — the fleet
/// scheduler falls through to the next submission once the cap is hit.
#[sqlx::test]
async fn fleet_claim_respects_max_workers(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("capped".into()),
            priority: 0,
            max_workers: Some(1),
            claim_deadline_secs: None,
        })
        .await
        .unwrap();
    let d1 = drv_path("cappedAA1", "first");
    let d2 = drv_path("cappedBB2", "second");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![
                    ingest(&d1, "first", &[], true),
                    ingest(&d2, "second", &[], true),
                ],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();

    // First fleet claim succeeds.
    let http = reqwest::Client::new();
    let r1 = http
        .get(format!(
            "{}/claim?wait=2&system=x86_64-linux",
            handle.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), reqwest::StatusCode::OK);

    // Second claim (before the first completes) must 204 — submission
    // is at its worker cap and no other submissions exist.
    let r2 = http
        .get(format!(
            "{}/claim?wait=1&system=x86_64-linux",
            handle.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r2.status(),
        reqwest::StatusCode::NO_CONTENT,
        "at max_workers=1, second concurrent claim must 204"
    );
}

/// DELETE /admin/claims/{claim_id} force-expires a live claim so the
/// next reaper tick evicts it. Used to unstick a worker that hasn't
/// crossed the deadline but clearly isn't making progress.
#[sqlx::test]
async fn admin_evict_claim_force_expires(pool: PgPool) {
    use nix_ci_core::config::ServerConfig;
    // Short reaper interval so the eviction completes within the test's
    // timeout budget.
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.reaper_interval_secs = 1;
        cfg.job_heartbeat_timeout_secs = 30;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("stucka001", "hung");
    client
        .ingest_drv(job.id, &ingest(&drv, "hung", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Issue a claim but never complete it.
    let c = client
        .claim(job.id, "x86_64-linux", &[], 2)
        .await
        .unwrap()
        .expect("first claim must succeed");

    // Force-evict via the admin endpoint.
    let http = reqwest::Client::new();
    let resp = http
        .delete(format!("{}/admin/claims/{}", handle.base_url, c.claim_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    // Wait for the reaper tick + re-arm. Poll the claim endpoint; a
    // second claim for the same drv must succeed within ~3s.
    let mut re_claimed = None;
    for _ in 0..30 {
        if let Ok(Some(c2)) = client.claim(job.id, "x86_64-linux", &[], 1).await {
            re_claimed = Some(c2);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let re_claimed = re_claimed.expect("evicted claim must be re-issuable after reaper tick");
    assert_eq!(re_claimed.drv_path, drv);
    assert_ne!(re_claimed.claim_id, c.claim_id, "must be a fresh claim id");

    // Evicting a nonexistent claim 404s.
    let resp = http
        .delete(format!(
            "{}/admin/claims/{}",
            handle.base_url,
            nix_ci_core::types::ClaimId::new()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}
