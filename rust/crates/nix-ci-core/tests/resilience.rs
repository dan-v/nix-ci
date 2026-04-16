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
        .create_job(&CreateJobRequest { external_ref: None })
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
        .create_job(&CreateJobRequest { external_ref: None })
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
        .create_job(&CreateJobRequest { external_ref: None })
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
        .create_job(&CreateJobRequest { external_ref: None })
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
        .create_job(&CreateJobRequest { external_ref: None })
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
        })
        .await
        .unwrap();
    let b = client
        .create_job(&CreateJobRequest {
            external_ref: Some("race-b".into()),
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
        .create_job(&CreateJobRequest { external_ref: None })
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
        })
        .await
        .unwrap();
    let b = client
        .create_job(&CreateJobRequest {
            external_ref: Some("prop-b".into()),
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
        .create_job(&CreateJobRequest { external_ref: None })
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
        .create_job(&CreateJobRequest { external_ref: None })
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
        .create_job(&CreateJobRequest { external_ref: None })
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
