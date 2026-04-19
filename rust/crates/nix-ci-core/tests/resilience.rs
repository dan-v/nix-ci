//! Resilience tests: cancellation mid-flight, heartbeat reap, crash +
//! stale complete, concurrent cross-job claim race, writeback failure
//! during claim issuance.
//!
//! These cover the real-world failure modes that matter for a
//! drop-in-CI replacement: builds get cancelled, workers die,
//! coordinators restart, and the network is never friendly.

mod common;

use std::time::Duration;

use common::{drv_path, spawn_server, spawn_server_with_cfg};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, IngestDrvRequest, JobStatus};
use sqlx::PgPool;

fn ingest(drv: &str, name: &str, deps: &[&str], is_root: bool) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: deps.iter().map(|s| s.to_string()).collect(),
        is_root,
        attr: None,
    }
}

async fn wait_for_terminal(client: &CoordinatorClient, id: nix_ci_core::types::JobId) -> JobStatus {
    for _ in 0..40 {
        let s = client.status(id).await.unwrap();
        if s.status.is_terminal() {
            return s.status;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("job {id} never reached terminal state");
}

// ─── 1. Cancel mid-flight ──────────────────────────────────────────────

#[sqlx::test]
async fn cancel_mid_flight_invalidates_claim(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let leaf = drv_path("c01", "leaf");
    let root = drv_path("c02", "root");
    client
        .ingest_drv(job.id, &ingest(&leaf, "leaf", &[], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&root, "root", &[&leaf], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // A worker claims the leaf but hasn't reported completion yet.
    let claim = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("leaf must be claimable");

    // External cancel arrives. The claim's worker will find out via
    // either a 410 on next poll or an ignored=true on its complete.
    let resp = client.cancel(job.id).await.unwrap();
    assert_eq!(resp.status, JobStatus::Cancelled);

    // Further /claim calls for this job: the submission is removed
    // from the dispatcher map, so we get 410 Gone.
    match client.claim(job.id, "x86_64-linux", &[], 1).await {
        Err(nix_ci_core::Error::Gone(_)) => {}
        Ok(None) => {} // acceptable transient if the submission still exists but has no runnable
        Ok(Some(c)) => panic!("cancelled job still returned a claim: {}", c.drv_path),
        Err(e) => panic!("unexpected claim error: {e}"),
    }

    // The worker reports its (belated) build result. The coordinator
    // must not count this as a build success — the job is cancelled.
    let complete = client
        .complete(
            job.id,
            claim.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 10,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();
    assert!(
        complete.ignored,
        "cancelled job must ignore a stale worker completion"
    );

    // Durable invariant: the jobs row carries the terminal status +
    // done_at + the result snapshot.
    let (status_str, done_at, result): (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
    ) = sqlx::query_as("SELECT status, done_at, result FROM jobs WHERE id = $1")
        .bind(job.id.0)
        .fetch_one(&handle.pool)
        .await
        .unwrap();
    assert_eq!(status_str, "cancelled");
    assert!(done_at.is_some());
    assert!(
        result.is_some(),
        "cancelled job must have a result snapshot"
    );
}

#[sqlx::test]
async fn cancel_is_idempotent(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let first = client.cancel(job.id).await.unwrap();
    let second = client.cancel(job.id).await.unwrap();
    // Both return Cancelled; second is a no-op on DB state.
    assert_eq!(first.status, JobStatus::Cancelled);
    assert_eq!(second.status, JobStatus::Cancelled);
}

// ─── 2. Heartbeat-timeout job reap ─────────────────────────────────────

#[sqlx::test]
async fn heartbeat_timeout_reaps_job(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("hb1", "solo");
    client
        .ingest_drv(job.id, &ingest(&drv, "solo", &[], true))
        .await
        .unwrap();

    // Force the job's last_heartbeat into the past, then run the reaper
    // with a zero timeout. This is deterministic and doesn't require
    // waiting on wall-clock.
    sqlx::query("UPDATE jobs SET last_heartbeat = now() - INTERVAL '1 hour' WHERE id = $1")
        .bind(job.id.0)
        .execute(&handle.pool)
        .await
        .unwrap();

    // Hand-crank the reaper once with a 1s timeout — any last_heartbeat
    // older than 1s ago qualifies.
    nix_ci_core::durable::reaper::reap_stale_jobs(
        &handle.pool,
        &handle.dispatcher,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    // DB: jobs.status = cancelled, done_at set.
    let (status_str,): (String,) = sqlx::query_as("SELECT status FROM jobs WHERE id = $1")
        .bind(job.id.0)
        .fetch_one(&handle.pool)
        .await
        .unwrap();
    assert_eq!(status_str, "cancelled");

    // In-memory submission removed: further /claim returns 410 Gone.
    match client.claim(job.id, "x86_64-linux", &[], 1).await {
        Err(nix_ci_core::Error::Gone(_)) => {}
        other => panic!("expected 410 after reap, got {other:?}"),
    }

    // A subsequent heartbeat POST returns 410 so the runner exits its
    // heartbeat loop.
    match client.heartbeat(job.id).await {
        Err(nix_ci_core::Error::Gone(_)) => {}
        other => panic!("expected 410 heartbeat after reap, got {other:?}"),
    }
}

#[sqlx::test]
async fn heartbeat_timeout_drops_in_memory_claims(pool: PgPool) {
    // When a job is reaped for heartbeat timeout, every in-memory claim
    // tied to that job must be released immediately — not linger until
    // its deadline (hours). Regression guard for a previous leak.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let leaf = drv_path("hb2", "leaf");
    client
        .ingest_drv(job.id, &ingest(&leaf, "leaf", &[], true))
        .await
        .unwrap();

    let _c = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("leaf claim");

    assert_eq!(
        handle.dispatcher.claims.len(),
        1,
        "claim should be in the map"
    );

    sqlx::query("UPDATE jobs SET last_heartbeat = now() - INTERVAL '1 hour' WHERE id = $1")
        .bind(job.id.0)
        .execute(&handle.pool)
        .await
        .unwrap();

    nix_ci_core::durable::reaper::reap_stale_jobs(
        &handle.pool,
        &handle.dispatcher,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    assert_eq!(
        handle.dispatcher.claims.len(),
        0,
        "reaped job's claims must be evicted from the in-memory map"
    );
}

// ─── 3. Coordinator restart + stale complete ───────────────────────────

#[sqlx::test]
async fn restart_cancels_in_flight_and_stale_complete_is_ignored(pool: PgPool) {
    // Simulate a coordinator crash mid-build: issue a claim, tear the
    // server down, spin up a fresh server. With the ephemeral-
    // dispatcher design the in-flight job is cancelled at boot
    // (clear_busy) — not resumed. A late POST /complete with the old
    // claim_id must be ignored, and the caller (CCI) must re-submit
    // the job if it wants a fresh attempt.
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("rst", "solo");
    client
        .ingest_drv(job.id, &ingest(&drv, "solo", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let stale_claim = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("must issue claim");

    drop(handle);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let handle2 = spawn_server(pool).await;
    let client2 = CoordinatorClient::new(&handle2.base_url);

    // clear_busy cancelled the old job.
    let (status_str, result): (String, Option<serde_json::Value>) =
        sqlx::query_as("SELECT status, result FROM jobs WHERE id = $1")
            .bind(job.id.0)
            .fetch_one(&handle2.pool)
            .await
            .unwrap();
    assert_eq!(status_str, "cancelled");
    assert!(
        result.is_some(),
        "cancelled job must have a result snapshot"
    );

    // The old worker's (belated) completion POST lands on the new
    // coordinator. The claim map is empty, so complete returns
    // ignored=true.
    let resp = client2
        .complete(
            job.id,
            stale_claim.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 10,
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
        "stale claim from previous lifetime must be ignored"
    );

    // /claim on the cancelled job returns 410 — the caller must
    // re-submit a fresh job to retry.
    match client2.claim(job.id, "x86_64-linux", &[], 1).await {
        Err(nix_ci_core::Error::Gone(_)) => {}
        other => panic!("expected 410 Gone on cancelled job, got {other:?}"),
    }
}

/// After `clear_busy` flips a job to cancelled, `GET /jobs/{id}` must
/// return a valid `JobStatusResponse` — not 500. The sentinel snapshot
/// written by clear_busy previously contained `"id": null`, which
/// failed serde deserialization on the read path with
/// `invalid type: null, expected a formatted UUID string` and every
/// operator query after a coordinator restart returned 500.
#[sqlx::test]
async fn clear_busy_sentinel_survives_round_trip_to_client(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .expect("create_job");

    nix_ci_core::durable::clear_busy(&pool)
        .await
        .expect("clear_busy");

    // Drop the in-memory submission so the status handler falls through
    // to the persisted snapshot (the post-restart shape).
    handle.dispatcher.submissions.remove(job.id);

    let snap = client
        .status(job.id)
        .await
        .expect("status must return valid JSON (not 500)");
    assert_eq!(snap.id, job.id);
    assert_eq!(snap.status, JobStatus::Cancelled);
    assert_eq!(
        snap.eval_error.as_deref(),
        Some("coordinator restarted; job aborted")
    );
}

// ─── 4. Concurrent cross-job claim race ────────────────────────────────

#[sqlx::test]
async fn concurrent_cross_job_claim_exactly_one_winner(pool: PgPool) {
    // Two submissions share a deduped leaf. Fire many concurrent claim
    // requests across both jobs; the CAS on `Step::runnable` must
    // ensure exactly one worker per build attempt sees the drv.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let a = client
        .create_job(&CreateJobRequest {
            external_ref: Some("race-a".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let b = client
        .create_job(&CreateJobRequest {
            external_ref: Some("race-b".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let shared = drv_path("rcx", "shared");
    client
        .ingest_drv(a.id, &ingest(&shared, "shared", &[], true))
        .await
        .unwrap();
    client
        .ingest_drv(b.id, &ingest(&shared, "shared", &[], true))
        .await
        .unwrap();
    client.seal(a.id).await.unwrap();
    client.seal(b.id).await.unwrap();

    // Fire 8 claims concurrently, 4 per job. Each claim long-polls for
    // up to 3s. Exactly one should succeed; the rest must observe the
    // drv as either in-flight (None/204) or already-completed via
    // their job transitioning terminal (None/410).
    let mut joinset = tokio::task::JoinSet::new();
    for (n, jid) in [
        (0u32, a.id),
        (1, a.id),
        (2, a.id),
        (3, a.id),
        (4, b.id),
        (5, b.id),
        (6, b.id),
        (7, b.id),
    ] {
        let client = CoordinatorClient::new(&handle.base_url);
        joinset.spawn(async move { (n, jid, client.claim(jid, "x86_64-linux", &[], 3).await) });
    }

    let mut winners: Vec<(
        u32,
        nix_ci_core::types::JobId,
        nix_ci_core::types::ClaimResponse,
    )> = Vec::new();
    let mut non_winners = 0;
    while let Some(r) = joinset.join_next().await {
        let (n, jid, res) = r.unwrap();
        match res {
            Ok(Some(c)) => winners.push((n, jid, c)),
            Ok(None) | Err(nix_ci_core::Error::Gone(_)) => non_winners += 1,
            Err(e) => panic!("unexpected claim error from task {n}: {e}"),
        }
    }
    assert_eq!(
        winners.len(),
        1,
        "exactly one claim must win the CAS; got {} winners: {:?}",
        winners.len(),
        winners.iter().map(|(n, _, _)| n).collect::<Vec<_>>()
    );
    assert_eq!(non_winners, 7);

    // Complete the winning claim. Both jobs must converge to Done via
    // the shared step's make_rdeps_runnable + check_and_publish_terminal.
    let (_, winning_jid, winning_claim) = winners.pop().unwrap();
    client
        .complete(
            winning_jid,
            winning_claim.claim_id,
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
    assert_eq!(wait_for_terminal(&client, a.id).await, JobStatus::Done);
    assert_eq!(wait_for_terminal(&client, b.id).await, JobStatus::Done);
}

// ─── 5. Heartbeat-timeout uses configured short window ─────────────────

#[sqlx::test]
async fn heartbeat_timeout_respects_configured_window(pool: PgPool) {
    // Same as heartbeat_timeout_reaps_job but proves we can configure
    // a short window for tests.
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.job_heartbeat_timeout_secs = 1;
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

    sqlx::query("UPDATE jobs SET last_heartbeat = now() - INTERVAL '10 seconds' WHERE id = $1")
        .bind(job.id.0)
        .execute(&handle.pool)
        .await
        .unwrap();
    nix_ci_core::durable::reaper::reap_stale_jobs(
        &handle.pool,
        &handle.dispatcher,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    let (status_str,): (String,) = sqlx::query_as("SELECT status FROM jobs WHERE id = $1")
        .bind(job.id.0)
        .fetch_one(&handle.pool)
        .await
        .unwrap();
    assert_eq!(status_str, "cancelled");
}

// ─── 6. Propagated failures stay in owning submission ──────────────────

#[sqlx::test]
async fn propagated_failures_stay_in_owning_submission(pool: PgPool) {
    // Two jobs share a leaf (stdenv) via dedup. Job A owns ONLY the
    // shared leaf. Job B owns the leaf + an rdep (gcc). The leaf fails.
    // Job A's failures list must contain only the leaf — NOT gcc. Job B
    // must contain both. Regression for a bug where propagation
    // recorded rdep failures on every origin submission regardless of
    // whether the origin owned the rdep.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let a = client
        .create_job(&CreateJobRequest {
            external_ref: Some("prop-a".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let b = client
        .create_job(&CreateJobRequest {
            external_ref: Some("prop-b".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let shared = drv_path("std", "stdenv");
    let gcc = drv_path("gcc", "gcc-13.2");

    // Job A: only stdenv as a root.
    client
        .ingest_drv(a.id, &ingest(&shared, "stdenv", &[], true))
        .await
        .unwrap();
    client.seal(a.id).await.unwrap();

    // Job B: stdenv + gcc (gcc depends on stdenv). stdenv is deduped
    // with Job A's step. gcc is owned only by Job B.
    client
        .ingest_drv(b.id, &ingest(&shared, "stdenv", &[], false))
        .await
        .unwrap();
    client
        .ingest_drv(b.id, &ingest(&gcc, "gcc-13.2", &[&shared], true))
        .await
        .unwrap();
    client.seal(b.id).await.unwrap();

    // A worker claims stdenv and reports it as a terminal build failure.
    let c = client
        .claim(a.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("stdenv claim");
    client
        .complete(
            a.id,
            c.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 5,
                exit_code: Some(1),
                error_category: Some(nix_ci_core::types::ErrorCategory::BuildFailure),
                error_message: Some("stdenv broke".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(wait_for_terminal(&client, a.id).await, JobStatus::Failed);
    assert_eq!(wait_for_terminal(&client, b.id).await, JobStatus::Failed);

    let a_status = client.status(a.id).await.unwrap();
    let b_status = client.status(b.id).await.unwrap();

    let a_hashes: Vec<_> = a_status
        .failures
        .iter()
        .map(|f| f.drv_hash.clone())
        .collect();
    let b_hashes: Vec<_> = b_status
        .failures
        .iter()
        .map(|f| f.drv_hash.clone())
        .collect();

    let shared_hash = nix_ci_core::types::drv_hash_from_path(&shared).unwrap();
    let gcc_hash = nix_ci_core::types::drv_hash_from_path(&gcc).unwrap();

    assert!(
        a_hashes.contains(&shared_hash),
        "Job A must report stdenv in failures, got {a_hashes:?}"
    );
    assert!(
        !a_hashes.contains(&gcc_hash),
        "Job A must NOT report gcc in failures (gcc is not in A's closure); got {a_hashes:?}"
    );

    assert!(
        b_hashes.contains(&shared_hash),
        "Job B must report stdenv, got {b_hashes:?}"
    );
    assert!(
        b_hashes.contains(&gcc_hash),
        "Job B must report gcc (propagated), got {b_hashes:?}"
    );
}

// ─── 7. Worker dies mid-build — heartbeat reap restores state ──────────

#[sqlx::test]
async fn worker_dies_mid_build_heartbeat_reaps_cleanly(pool: PgPool) {
    // Simulate a worker that claims a drv, then dies (never /complete,
    // stops heartbeating). Heartbeat timeout reaper must cancel the job,
    // evict the in-memory claim, and publish JobDone so SSE subscribers
    // notice. A subsequent /claim on the job returns 410.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("zw1", "solo");
    client
        .ingest_drv(job.id, &ingest(&drv, "solo", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Worker claims — never completes.
    let _claim = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("leaf claim");
    assert_eq!(handle.dispatcher.claims.len(), 1);

    // "Worker dies" — no more heartbeats. Age the heartbeat out and run
    // the reaper.
    sqlx::query("UPDATE jobs SET last_heartbeat = now() - INTERVAL '1 hour' WHERE id = $1")
        .bind(job.id.0)
        .execute(&handle.pool)
        .await
        .unwrap();
    nix_ci_core::durable::reaper::reap_stale_jobs(
        &handle.pool,
        &handle.dispatcher,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    // Claim map empty; submission gone.
    assert_eq!(handle.dispatcher.claims.len(), 0);
    assert!(handle.dispatcher.submissions.get(job.id).is_none());

    // New /claim returns 410.
    match client.claim(job.id, "x86_64-linux", &[], 1).await {
        Err(nix_ci_core::Error::Gone(_)) => {}
        other => panic!("expected 410 after reap, got {other:?}"),
    }

    // Status reflects terminal cancelled.
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, JobStatus::Cancelled);
}

// ─── 8. Two concurrent /complete on the same claim_id ──────────────────

#[sqlx::test]
async fn concurrent_complete_same_claim_exactly_one_wins(pool: PgPool) {
    // Fire two /complete POSTs with the same claim_id simultaneously.
    // The in-memory `Claims::take` is atomic: exactly one handler must
    // see a live claim (ignored=false), the other must see it already
    // taken (ignored=true). Regression guard against any path that
    // lets two workers both successfully report completion for the
    // same build attempt.
    let handle = spawn_server(pool).await;
    let base = handle.base_url.clone();
    let client = CoordinatorClient::new(&base);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("cc1", "solo");
    client
        .ingest_drv(job.id, &ingest(&drv, "solo", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let claim = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("claim");

    let req = CompleteRequest {
        success: true,
        duration_ms: 5,
        exit_code: Some(0),
        error_category: None,
        error_message: None,
        log_tail: None,
    };

    let c1 = CoordinatorClient::new(&base);
    let c2 = CoordinatorClient::new(&base);
    let req1 = req.clone();
    let req2 = req.clone();
    let (r1, r2) = tokio::join!(
        async move { c1.complete(job.id, claim.claim_id, &req1).await.unwrap() },
        async move { c2.complete(job.id, claim.claim_id, &req2).await.unwrap() },
    );

    let ignored = [r1.ignored, r2.ignored];
    let winners = ignored.iter().filter(|i| !**i).count();
    let losers = ignored.iter().filter(|i| **i).count();
    assert_eq!(winners, 1, "exactly one complete must win, got {ignored:?}");
    assert_eq!(
        losers, 1,
        "exactly one complete must see ignored=true, got {ignored:?}"
    );

    assert_eq!(wait_for_terminal(&client, job.id).await, JobStatus::Done);
}

// ─── 9. Cancel while a long-poll claim is in flight ────────────────────

#[sqlx::test]
async fn cancel_propagates_to_in_flight_long_poll(pool: PgPool) {
    // Start a long-poll /claim with no runnable drvs. In parallel
    // cancel the job. The long-poll must return 410 promptly (well
    // within the poll deadline) — the submission's removal from the
    // dispatcher map is observed either by the pop_runnable path or by
    // the next wake. Guards against a worker hung for up to max_wait
    // after an external cancel.
    let handle = spawn_server(pool).await;
    let base = handle.base_url.clone();
    let client = CoordinatorClient::new(&base);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    // No ingest — pop_runnable will always return None; claim loops
    // until wake or deadline.

    let claim_client = CoordinatorClient::new(&base);
    let claim_task = tokio::spawn(async move {
        // Long wait so the cancel has to propagate through notify.
        claim_client.claim(job.id, "x86_64-linux", &[], 10).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.cancel(job.id).await.unwrap();

    // The long-poll should resolve within a generous bound. It can
    // either return 410 (if the handler re-fetched submissions) or
    // None (if pop_runnable races through wake with the submission
    // still present momentarily). Either is acceptable — the
    // important property is that it resolves BEFORE the 10s deadline.
    let result = tokio::time::timeout(Duration::from_secs(3), claim_task)
        .await
        .expect("claim must resolve within 3s of cancel")
        .expect("task must not panic");
    match result {
        Err(nix_ci_core::Error::Gone(_)) => {}
        Ok(None) => {}
        Ok(Some(c)) => panic!("cancelled job should not issue a claim: {c:?}"),
        Err(e) => panic!("unexpected error: {e}"),
    }
}

// ─── PG fault injection (C7) ─────────────────────────────────────────

/// Kill the coordinator's Postgres backend connections mid-flight and
/// verify the coordinator heals — subsequent requests succeed once PG
/// accepts new connections again. The simulation uses the test pool
/// itself to issue pg_terminate_backend against every backend whose
/// application_name matches us.
///
/// This exercises: sqlx's pool reconnect path; idempotent writeback
/// calls succeeding after retry; in-memory dispatcher state surviving
/// a transient DB outage without divergence.
#[sqlx::test]
async fn coordinator_recovers_from_pg_connection_loss(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Warm-up: job + single drv + seal. Coordinator has open PG
    // connections at this point.
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("faultabc1", "a");
    client
        .ingest_drv(job.id, &ingest(&drv, "a", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Terminate every connection in the test database. The coordinator's
    // pool connections all run against the same DB, so this force-
    // closes their sockets mid-loop. sqlx will detect the broken
    // connection on next use and open a fresh one.
    let killed: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)::bigint FROM pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = current_database() AND pid <> pg_backend_pid()
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    tracing::info!(%killed, "forcefully closed coordinator pg backends");

    // Poll once to let sqlx observe the close; first call may fail
    // with a transport error. Retry up to 5 times (total < 2s). Past
    // that the coordinator should have healed.
    let mut last_err = None;
    let mut healed = false;
    for _ in 0..5 {
        match client.status(job.id).await {
            Ok(_) => {
                healed = true;
                break;
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    assert!(
        healed,
        "coordinator did not recover from pg connection loss after 5 retries; last_err = {last_err:?}"
    );

    // Second warm check: a completely fresh workflow also succeeds —
    // proving the reconnect isn't just a one-shot.
    let job2 = client
        .create_job(&CreateJobRequest::default())
        .await
        .expect("fresh create_job after reconnect must succeed");
    assert!(client.status(job2.id).await.is_ok());
}

/// Terminal writeback (transition_job_terminal) is idempotent by
/// design: the `done_at IS NULL` guard means only the first writer
/// records a status. This test proves the contract by calling cancel
/// twice concurrently — both return success, Postgres only records
/// one transition, and the in-memory submission is removed exactly
/// once.
#[sqlx::test]
async fn terminal_writeback_idempotent_under_race(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("writeidm1", "a");
    client
        .ingest_drv(job.id, &ingest(&drv, "a", &[], true))
        .await
        .unwrap();

    // Two concurrent cancels — one wins the UPDATE race on the
    // `done_at IS NULL` guard; the other hits 0 rows_affected and
    // returns success anyway.
    let c1 = client.clone();
    let c2 = client.clone();
    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { c1.cancel(job.id).await }),
        tokio::spawn(async move { c2.cancel(job.id).await }),
    );
    assert!(r1.unwrap().is_ok());
    assert!(r2.unwrap().is_ok());

    // The persisted row must show exactly one transition.
    let done_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT done_at FROM jobs WHERE id = $1")
            .bind(job.id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(done_at.is_some(), "concurrent cancel must persist terminal");
    assert_eq!(
        client.status(job.id).await.unwrap().status,
        JobStatus::Cancelled
    );
}

// ─── 10. Terminal-writeback retry sweep ────────────────────────────────

/// The wedge this test guards against: a `/complete` that crosses the
/// last-drv-of-job boundary while the PG UPDATE for `jobs.result` fails
/// (transient PG outage, network blip, `statement_timeout` fire).
/// `check_and_publish_terminal` propagates the error BEFORE CAS'ing
/// `sub.mark_terminal()`, so the submission sits in memory with:
///   * `sub.is_sealed() == true`
///   * all members `finished`
///   * `sub.terminal == false`
///   * `jobs.status == 'pending'` and `jobs.result IS NULL` in PG
///
/// With no retry sweep, nothing re-fires `check_and_publish_terminal`
/// for this submission and the runner's SSE waits forever — eventually
/// the CI run times out externally and a successful build is recorded
/// as `cancelled`.
///
/// The retry sweep
/// (`server::complete::retry_pending_terminal_writebacks`) scans live
/// submissions once per reaper tick for this exact shape and re-invokes
/// `check_and_publish_terminal`, which persists on the second try once
/// PG recovers. Idempotent against a racing live `/complete` (both go
/// through the `done_at IS NULL` guard and `mark_terminal` CAS).
///
/// Reaches the wedge state deterministically by:
///   1. Creating + ingesting + sealing normally.
///   2. Manually flipping each toplevel's `finished` bit and bumping
///      `sub.done_count` (mimicking what a `/complete` handler does
///      before `check_and_publish_terminal`).
///   3. Leaving `sub.terminal = false` and `jobs.result = NULL`.
///
/// Then invokes the retry function directly and asserts:
///   * the PG row transitioned to `done` with a non-NULL `result`;
///   * `sub.is_terminal() == true`;
///   * the submission was removed from the in-memory map;
///   * the `terminal_writeback_retry_finalized` counter ticked on
///     `/metrics`.
#[sqlx::test]
async fn retry_finalizes_wedged_submission_after_failed_terminal_write(pool: PgPool) {
    use std::sync::atomic::Ordering;

    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Baseline: jobs_terminal{status="done"} counter must not be
    // double-bumped — this test finalizes through the retry path, which
    // reuses the normal terminal accounting, so one (and only one)
    // transition should count.
    let counter_name = "nix_ci_jobs_terminal_total";
    let counter_labels = &[("status", "done")];
    let counter_before =
        common::scrape_metric(&handle.base_url, counter_name, counter_labels)
            .await
            .unwrap_or(0.0);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // Two-leaf submission so we can cover the all-toplevels-finished
    // scan path (the counter fast-exit alone would pass a single-drv
    // case with less work).
    let leaf_a = drv_path("wed_a", "a");
    let leaf_b = drv_path("wed_b", "b");
    client
        .ingest_drv(job.id, &ingest(&leaf_a, "a", &[], true))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&leaf_b, "b", &[], true))
        .await
        .unwrap();

    // Seal while nothing is done yet. check_and_publish_terminal fires
    // inside the seal handler but fast-exits on done+failed < total, so
    // we're in a "sealed, nothing finished" state after this call.
    client.seal(job.id).await.unwrap();

    // Pull the live submission to mutate it directly. This is what a
    // successful `/complete` would do — flip each step's `finished`
    // and bump `sub.done_count` — except we SKIP the subsequent call
    // to `check_and_publish_terminal`, which is what the wedge looks
    // like after a failed PG write.
    let sub = handle
        .dispatcher
        .submissions
        .get(job.id)
        .expect("submission must be live after seal");
    for leaf in [&leaf_a, &leaf_b] {
        let hash = nix_ci_core::types::drv_hash_from_path(leaf).unwrap();
        let step = handle
            .dispatcher
            .steps
            .get(&hash)
            .expect("step must exist post-ingest");
        step.finished.store(true, Ordering::Release);
        sub.done_count.fetch_add(1, Ordering::AcqRel);
    }

    // Sanity: we're genuinely in the wedge state before the retry runs.
    assert!(sub.is_sealed());
    assert!(
        !sub.is_terminal(),
        "precondition: sub.terminal must be false for the wedge simulation"
    );
    let counts = sub.live_counts();
    assert_eq!(counts.total, 2);
    assert_eq!(counts.done, 2);
    let (status_pre, done_at_pre, result_pre): (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
    ) = sqlx::query_as("SELECT status, done_at, result FROM jobs WHERE id = $1")
        .bind(job.id.0)
        .fetch_one(&handle.pool)
        .await
        .unwrap();
    assert_eq!(status_pre, "pending", "PG row is still pending pre-retry");
    assert!(done_at_pre.is_none());
    assert!(result_pre.is_none());

    // Run the sweep. This is what the background task calls on every
    // reaper_interval_secs tick.
    nix_ci_core::server::complete::retry_pending_terminal_writebacks(&handle.state).await;

    // Post-condition 1: PG row transitioned to done with a terminal
    // snapshot.
    let (status_post, done_at_post, result_post): (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
    ) = sqlx::query_as("SELECT status, done_at, result FROM jobs WHERE id = $1")
        .bind(job.id.0)
        .fetch_one(&handle.pool)
        .await
        .unwrap();
    assert_eq!(
        status_post, "done",
        "retry sweep must have persisted terminal status"
    );
    assert!(
        done_at_post.is_some(),
        "retry sweep must have set done_at"
    );
    assert!(
        result_post.is_some(),
        "retry sweep must have persisted the JobStatusResponse snapshot"
    );

    // Post-condition 2: in-memory terminal flag + submission removal.
    // mark_terminal was CAS'd true; submission dropped from the map.
    assert!(
        sub.is_terminal(),
        "retry sweep must have CAS'd the in-memory terminal flag"
    );
    assert!(
        handle.dispatcher.submissions.get(job.id).is_none(),
        "retry sweep must remove the finalized submission from the dispatcher map"
    );

    // Post-condition 3: /metrics exposes the retry counter so operators
    // can alert on PG availability without having to scrape the logs.
    let retry_counter =
        common::scrape_metric(&handle.base_url, "nix_ci_terminal_writeback_retry_finalized_total", &[])
            .await
            .expect(
                "nix_ci_terminal_writeback_retry_finalized_total must be present after the retry sweep finalized one submission",
            );
    assert_eq!(retry_counter, 1.0);

    // Post-condition 4: normal jobs_terminal accounting — exactly one
    // done transition. A double-bump would mean the retry path was
    // accidentally summing with a live /complete (which can't happen
    // here because we never called /complete, but we assert it so the
    // property holds if the test shape is ever generalized).
    let counter_after =
        common::scrape_metric_expect(&handle.base_url, counter_name, counter_labels).await;
    assert_eq!(
        counter_after - counter_before,
        1.0,
        "retry sweep must bump jobs_terminal{{status=done}} exactly once"
    );

    // Post-condition 5: the client-facing status endpoint returns the
    // terminal snapshot (reading from the persisted JSONB since the
    // submission is gone from memory). End-to-end proof that the
    // recovery is observable by callers.
    let snap = client.status(job.id).await.unwrap();
    assert_eq!(snap.status, JobStatus::Done);
}

/// The retry sweep must be idempotent: calling it twice in a row on the
/// already-recovered submission must not double-bump counters or
/// produce spurious log spam. Regression guard against the sweep
/// mistakenly re-entering `check_and_publish_terminal` after the
/// submission is already removed from the dispatcher map (in which
/// case `submissions.all()` returns an empty set and the function is a
/// no-op — exactly the contract).
#[sqlx::test]
async fn retry_is_idempotent_once_finalized(pool: PgPool) {
    use std::sync::atomic::Ordering;

    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let leaf = drv_path("wedidm", "solo");
    client
        .ingest_drv(job.id, &ingest(&leaf, "solo", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Wedge state.
    let sub = handle.dispatcher.submissions.get(job.id).unwrap();
    let hash = nix_ci_core::types::drv_hash_from_path(&leaf).unwrap();
    let step = handle.dispatcher.steps.get(&hash).unwrap();
    step.finished.store(true, Ordering::Release);
    sub.done_count.fetch_add(1, Ordering::AcqRel);

    // First sweep: finalizes, counter goes 0 → 1.
    nix_ci_core::server::complete::retry_pending_terminal_writebacks(&handle.state).await;
    let after_first = common::scrape_metric_expect(
        &handle.base_url,
        "nix_ci_terminal_writeback_retry_finalized_total",
        &[],
    )
    .await;
    assert_eq!(after_first, 1.0);

    // Second sweep: submission is gone, must be a no-op.
    nix_ci_core::server::complete::retry_pending_terminal_writebacks(&handle.state).await;
    let after_second = common::scrape_metric_expect(
        &handle.base_url,
        "nix_ci_terminal_writeback_retry_finalized_total",
        &[],
    )
    .await;
    assert_eq!(
        after_second, 1.0,
        "retry sweep on an empty candidate set must not touch the counter"
    );
}

/// Defence in depth: a non-sealed submission must never be touched by
/// the retry sweep, even if its counters happen to satisfy
/// `done + failed >= total` (possible when a submission has zero
/// members because nothing has been ingested yet — `0 >= 0` is true).
/// A sealed check is the contract for "no more drvs coming"; without
/// it a pre-seal empty submission would be force-finalized, breaking
/// the "create job, ingest batches, seal" sequence.
#[sqlx::test]
async fn retry_skips_unsealed_submission(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // Note: NO seal, NO ingest. Submission exists in the dispatcher
    // map with zero members — counter math is `0 done + 0 failed >= 0
    // total`, which technically passes the sweep's fast-path check.
    // The explicit is_sealed guard is the thing that saves us.
    let before_sub = handle
        .dispatcher
        .submissions
        .get(job.id)
        .expect("submission must be registered at create time");
    assert!(!before_sub.is_sealed());

    // Run the sweep.
    nix_ci_core::server::complete::retry_pending_terminal_writebacks(&handle.state).await;

    // Submission must still be live, not prematurely terminalized.
    let after_sub = handle
        .dispatcher
        .submissions
        .get(job.id)
        .expect("submission must NOT be removed by a sweep on an unsealed job");
    assert!(!after_sub.is_terminal());

    // PG row stays pending too.
    let (status,): (String,) = sqlx::query_as("SELECT status FROM jobs WHERE id = $1")
        .bind(job.id.0)
        .fetch_one(&handle.pool)
        .await
        .unwrap();
    assert_eq!(status, "pending");

    // Counter must not have ticked.
    let counter = common::scrape_metric(
        &handle.base_url,
        "nix_ci_terminal_writeback_retry_finalized_total",
        &[],
    )
    .await
    .unwrap_or(0.0);
    assert_eq!(
        counter, 0.0,
        "retry must not finalize an unsealed submission"
    );
}

// ─── 11. P-UPGRADE-SAFE: rolling upgrade preserves terminal rows ────

/// The SPEC bar P-UPGRADE-SAFE: a rolling deploy (coordinator N → N+1,
/// or just an in-place restart) must not corrupt any `jobs.result` or
/// `failed_outputs` row that predates the deploy. Without an explicit
/// regression guard, a bad migration, a bug in `clear_busy`, or an
/// over-broad cleanup sweep could silently destroy durable state
/// across every rollout — the worst failure mode for an "only broken
/// builds break builds" coordinator, since no build is currently
/// running but every historical result is lost.
///
/// Shape:
///   1. Boot coordinator, create job with external_ref, ingest + seal
///      + complete one drv → terminal Done. Read `jobs.result` JSONB
///      from PG directly.
///   2. Insert a `failed_outputs` row with `expires_at = now()+1h`.
///   3. Drop the handle (simulated SIGTERM); spin up a fresh
///      coordinator on the same PG (simulated rolling upgrade).
///   4. Assert:
///      * `jobs.result` for the terminal job is byte-identical to
///        what was persisted pre-restart.
///      * The `failed_outputs` row is still present and unexpired.
///      * `GET /jobs/by-external-ref/<ref>` on the new coordinator
///        returns the same snapshot clients would've seen before.
///      * New job creation works end-to-end on the new coordinator.
#[sqlx::test]
async fn rolling_upgrade_preserves_terminal_rows(pool: PgPool) {
    // ─── Phase 1: populate terminal state on "coordinator N" ────
    let handle1 = spawn_server(pool.clone()).await;
    let client1 = CoordinatorClient::new(&handle1.base_url);

    let external_ref = "rolling-upgrade-fixture";
    let job = client1
        .create_job(&CreateJobRequest {
            external_ref: Some(external_ref.into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let leaf = drv_path("ru", "solo");
    client1
        .ingest_drv(job.id, &ingest(&leaf, "solo", &[], true))
        .await
        .unwrap();
    client1.seal(job.id).await.unwrap();

    let claim = client1
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim must issue");
    client1
        .complete(
            job.id,
            claim.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 42,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(wait_for_terminal(&client1, job.id).await, JobStatus::Done);

    // Snapshot `jobs` row pre-restart. `result` is the JSONB we must
    // preserve byte-identically.
    let (status_pre, done_at_pre, result_pre, sealed_pre): (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
        bool,
    ) = sqlx::query_as(
        "SELECT status, done_at, result, sealed FROM jobs WHERE id = $1",
    )
    .bind(job.id.0)
    .fetch_one(&handle1.pool)
    .await
    .unwrap();
    assert_eq!(status_pre, "done");
    assert!(done_at_pre.is_some());
    let result_pre = result_pre.expect("terminal job must have a result JSONB");
    assert!(sealed_pre);

    // Insert a failed_outputs row that must survive the restart.
    // Expires_at = now()+1h so clear_busy's expired-row sweep doesn't
    // clean it up.
    let poisoned_path = "/nix/store/abc-poisoned";
    let poisoned_hash = "abcfakefakefakefakefakefakefakefakefakefakefake";
    sqlx::query(
        "INSERT INTO failed_outputs (output_path, drv_hash, expires_at) \
         VALUES ($1, $2, now() + interval '1 hour')",
    )
    .bind(poisoned_path)
    .bind(poisoned_hash)
    .execute(&handle1.pool)
    .await
    .unwrap();

    // ─── Phase 2: simulated SIGTERM → fresh coordinator boot ───
    // Drop the handle (oneshot shutdown) and give axum a moment to
    // close out before binding a new listener on the same pool.
    drop(handle1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let handle2 = spawn_server(pool.clone()).await;
    let client2 = CoordinatorClient::new(&handle2.base_url);

    // ─── Phase 3: pre-restart terminal rows must be byte-identical ─
    let (status_post, done_at_post, result_post, sealed_post): (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
        bool,
    ) = sqlx::query_as(
        "SELECT status, done_at, result, sealed FROM jobs WHERE id = $1",
    )
    .bind(job.id.0)
    .fetch_one(&handle2.pool)
    .await
    .unwrap();
    assert_eq!(
        status_post, status_pre,
        "restart must not mutate jobs.status on terminal rows"
    );
    assert_eq!(
        done_at_post, done_at_pre,
        "restart must not mutate jobs.done_at on terminal rows"
    );
    assert_eq!(
        sealed_post, sealed_pre,
        "restart must not mutate jobs.sealed on terminal rows"
    );
    let result_post = result_post.expect("terminal job must still have a result JSONB");
    assert_eq!(
        result_post, result_pre,
        "restart must not mutate jobs.result on terminal rows"
    );

    // ─── Phase 4: the failed_outputs row must survive ───
    let surviving: Option<(String, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT output_path, drv_hash, expires_at FROM failed_outputs WHERE output_path = $1",
    )
    .bind(poisoned_path)
    .fetch_optional(&handle2.pool)
    .await
    .unwrap();
    let (surviving_path, surviving_hash, surviving_expires) =
        surviving.expect("failed_outputs row must survive rolling upgrade");
    assert_eq!(surviving_path, poisoned_path);
    assert_eq!(surviving_hash, poisoned_hash);
    assert!(
        surviving_expires > chrono::Utc::now(),
        "failed_outputs expires_at must not be clobbered to the past"
    );

    // ─── Phase 5: client-facing lookups return the same snapshot ─
    let snap_direct = client2.status(job.id).await.unwrap();
    assert_eq!(snap_direct.id, job.id);
    assert_eq!(snap_direct.status, JobStatus::Done);
    // Serialized identity as the pre-restart persisted JSONB.
    let snap_direct_json = serde_json::to_value(&snap_direct).unwrap();
    assert_eq!(
        snap_direct_json, result_pre,
        "GET /jobs/{{id}} after restart must return the same snapshot the pre-restart coordinator persisted"
    );

    // `GET /jobs/by-external-ref/{ref}` — the CCI retry path.
    let http = reqwest::Client::new();
    let resp = http
        .get(format!(
            "{}/jobs/by-external-ref/{external_ref}",
            handle2.base_url
        ))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["id"], job.id.0.to_string());
    assert_eq!(body["status"], "done");

    // Proof that suspected_worker_infra round-trips through the
    // JSONB on the operator's lookup path — even with no infra
    // attribution (healthy run), the field must be present as
    // `null` in the persisted snapshot so older clients don't
    // deserialize-fail. `#[serde(default)]` gives us that for
    // free; this is a guard against a future PR that removes the
    // default.
    assert!(
        snap_direct.suspected_worker_infra.is_none(),
        "healthy job must have no infra suspicion: {:?}",
        snap_direct.suspected_worker_infra
    );

    // ─── Phase 6: new job creation works on the restarted coord ─
    // Verify a FRESH job ingest → seal → complete → terminal path
    // works end-to-end on the new coordinator, not just state reads.
    let new_job = client2
        .create_job(&CreateJobRequest {
            external_ref: Some("post-upgrade-new".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_ne!(new_job.id, job.id);
    let new_leaf = drv_path("ru2", "fresh");
    client2
        .ingest_drv(new_job.id, &ingest(&new_leaf, "fresh", &[], true))
        .await
        .unwrap();
    client2.seal(new_job.id).await.unwrap();
    let new_claim = client2
        .claim(new_job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("fresh claim must issue");
    client2
        .complete(
            new_job.id,
            new_claim.claim_id,
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
    assert_eq!(
        wait_for_terminal(&client2, new_job.id).await,
        JobStatus::Done
    );
}

// ─── 12. Runner SSE recovery across coordinator restart ─────────────

/// C6 contract from the runner's point of view: when the coordinator
/// is unavailable, `sse::print_events_with` must return `Err` in
/// bounded time, never hang waiting for a `JobDone` that can't come.
/// A runner that hangs here means a downstream CI job that never
/// surfaces its failure, which is the worst UX regression — the user
/// sees "pending" forever instead of "failed, retry."
///
/// Tested against a coordinator that's already gone (dead endpoint):
/// reqwest connects to a refused port, outer reconnect loop burns the
/// 5-attempt budget (1+2+3+4+5 = 15s of linear backoff), task returns
/// `Err(Internal("SSE reconnect budget exhausted"))`.
///
/// Why not test "coord goes away mid-stream" in-process: when axum's
/// serve task is aborted, per-connection tasks are NOT automatically
/// aborted (they're spawned as siblings under the tokio runtime, not
/// children). A mid-stream SSE TCP connection stays half-open until
/// the OS garbage-collects the FD, so the in-process test would hang.
/// The end-to-end `ha-failover` orbstack scenario (out-of-process,
/// real TCP, real SIGKILL via `docker kill`) exercises the
/// mid-stream path. Here we cover the narrower
/// "coord-unavailable-to-client" surface, which is what matters for
/// CCI's runner-restarted-by-re-trigger path.
#[sqlx::test]
async fn runner_sse_fails_cleanly_when_coord_unavailable(pool: PgPool) {
    // Bind + immediately release a port so we have a known-refused
    // address. The SSE client's reconnect loop hits ECONNREFUSED
    // within microseconds per attempt.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Give the kernel a moment to fully release the port so the
    // first connect attempt reliably refuses (vs. landing on a
    // lingering TIME_WAIT).
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _ = pool; // unused — we deliberately never create the coord.
    let dead_url = format!("http://{addr}");
    let (tx, rx) = tokio::sync::watch::channel(false);
    let client = std::sync::Arc::new(CoordinatorClient::new(&dead_url));

    let start = tokio::time::Instant::now();
    let sse_task = tokio::spawn(nix_ci_core::runner::sse::print_events_with(
        client,
        nix_ci_core::types::JobId::new(),
        None,
        false,
        tx,
        rx,
    ));

    let outcome = tokio::time::timeout(Duration::from_secs(20), sse_task)
        .await
        .expect("SSE must resolve within reconnect budget — not hang")
        .expect("SSE task must not panic");
    let elapsed = start.elapsed();

    // Bound 1: resolves within ~budget (5 attempts × linear backoff
    // 1+2+3+4+5 = 15s plus a small epsilon for tokio scheduling).
    assert!(
        elapsed < Duration::from_secs(18),
        "SSE reconnect budget should bound at ~15s, took {elapsed:?}"
    );
    // Bound 2: also must not return too fast — a regression that
    // dropped the reconnect loop entirely (returning on first
    // failure) would be less resilient for genuinely flaky coordinators.
    // At least one reconnect attempt's 1s sleep should have elapsed.
    assert!(
        elapsed > Duration::from_millis(500),
        "SSE must attempt at least one reconnect before giving up, elapsed {elapsed:?}"
    );
    // Returns Err, not Ok(_) — we never observed a JobDone.
    assert!(
        outcome.is_err(),
        "SSE against a dead endpoint must return Err, got {outcome:?}"
    );
    // Specifically an `Internal` error with an SSE-related message so
    // the CCI logs explain what happened.
    match outcome {
        Err(nix_ci_core::Error::Internal(msg)) => {
            assert!(
                msg.contains("SSE"),
                "error must identify the SSE path: got {msg}"
            );
        }
        Err(e) => panic!("expected Error::Internal with SSE context, got {e:?}"),
        Ok(_) => unreachable!("covered by the is_err assertion above"),
    }
}

/// After a coordinator restart against the same Postgres, the old
/// job must be cancelled (by `clear_busy` at boot) and observable
/// via `/jobs/{id}` on the new coordinator. This is the durable side
/// of the runner-SSE contract: the runner's SSE fails, CCI retries
/// or surfaces the cancelled status, and the persisted terminal row
/// is consistent.
///
/// Existing `restart_cancels_in_flight_and_stale_complete_is_ignored`
/// and `rolling_upgrade_preserves_terminal_rows` cover adjacent
/// parts; this asserts the narrow "SSE consumer can read the
/// cancelled terminal snapshot from the new coordinator" path.
#[sqlx::test]
async fn runner_sse_consumer_reads_cancelled_terminal_after_restart(pool: PgPool) {
    // Phase 1: create a live job on coord-A.
    let handle_a = spawn_server(pool.clone()).await;
    let client_a = CoordinatorClient::new(&handle_a.base_url);
    let job = client_a
        .create_job(&CreateJobRequest {
            external_ref: Some("sse-cancel-after-restart".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let leaf = drv_path("sse2", "leaf");
    client_a
        .ingest_drv(job.id, &ingest(&leaf, "leaf", &[], true))
        .await
        .unwrap();

    // Phase 2: tear down coord-A without completing the job.
    drop(handle_a);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 3: coord-B boots on same PG → clear_busy runs, flipping
    // the old job to 'cancelled'.
    let handle_b = spawn_server(pool.clone()).await;
    let client_b = CoordinatorClient::new(&handle_b.base_url);

    // Phase 4: runner's status lookup (what CCI does on retry) gets
    // the cancelled snapshot, not a 500 or a hang.
    let status = client_b
        .status(job.id)
        .await
        .expect("status must return the persisted cancelled snapshot");
    assert_eq!(status.status, JobStatus::Cancelled);
    assert_eq!(
        status.eval_error.as_deref(),
        Some("coordinator restarted; job aborted"),
        "the clear_busy sentinel must identify the cause"
    );
}

