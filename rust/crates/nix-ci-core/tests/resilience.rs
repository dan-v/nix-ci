//! Resilience tests: cancellation mid-flight, heartbeat reap, crash +
//! stale complete, concurrent cross-job claim race, writeback failure
//! during claim issuance.
//!
//! These cover the real-world failure modes that matter for a
//! drop-in-CI replacement: builds get cancelled, workers die,
//! coordinators restart, and the network is never friendly.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{drv_path, spawn_server, spawn_server_with_cfg};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, IngestDrvRequest, JobStatus,
};
use sqlx::PgPool;

fn ingest(drv: &str, name: &str, deps: &[&str], is_root: bool) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: deps.iter().map(|s| s.to_string()).collect(),
        is_root,
        cache_status: None,
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

    // DB invariants: status=cancelled, done_at set, no drv left at
    // state='building' for this job's closure (cancel released them).
    let (status_str, done_at): (String, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT status, done_at FROM jobs WHERE id = $1")
            .bind(job.id.0)
            .fetch_one(&handle.pool)
            .await
            .unwrap();
    assert_eq!(status_str, "cancelled");
    assert!(done_at.is_some());

    let building_count: (i64,) = sqlx::query_as(
        r#"
        WITH RECURSIVE closure AS (
            SELECT drv_hash FROM job_roots WHERE job_id = $1
            UNION
            SELECT d.dep_hash FROM deps d JOIN closure c ON c.drv_hash = d.drv_hash
        )
        SELECT COUNT(*) FROM derivations
        WHERE state = 'building'
          AND drv_hash IN (SELECT drv_hash FROM closure)
        "#,
    )
    .bind(job.id.0)
    .fetch_one(&handle.pool)
    .await
    .unwrap();
    assert_eq!(
        building_count.0, 0,
        "cancel must release building drvs back to pending"
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
async fn heartbeat_timeout_releases_building_drvs(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    let leaf = drv_path("hb2", "leaf");
    let mid = drv_path("hb3", "mid");
    let root = drv_path("hb4", "root");
    client
        .ingest_drv(job.id, &ingest(&leaf, "leaf", &[], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&mid, "mid", &[&leaf], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&root, "root", &[&mid], true))
        .await
        .unwrap();

    // Claim leaf so it's state='building' in PG.
    let _c = client
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("leaf claim");

    // Age the heartbeat out.
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

    // No drv in this job's closure should remain 'building'. The old
    // reaper query only released roots; the regression would leave
    // non-root drvs (mid + leaf) wedged.
    let building: (i64,) = sqlx::query_as(
        r#"
        WITH RECURSIVE closure AS (
            SELECT drv_hash FROM job_roots WHERE job_id = $1
            UNION
            SELECT d.dep_hash FROM deps d JOIN closure c ON c.drv_hash = d.drv_hash
        )
        SELECT COUNT(*) FROM derivations
        WHERE state = 'building'
          AND drv_hash IN (SELECT drv_hash FROM closure)
        "#,
    )
    .bind(job.id.0)
    .fetch_one(&handle.pool)
    .await
    .unwrap();
    assert_eq!(
        building.0, 0,
        "heartbeat reap must release ALL building drvs in the closure, not just roots"
    );
}

// ─── 3. Coordinator restart + stale complete ───────────────────────────

#[sqlx::test]
async fn restart_with_outstanding_claim_ignores_stale_complete(pool: PgPool) {
    // Simulate a coordinator crash mid-build: issue a claim, tear the
    // server down, spin up a fresh server (which runs rehydrate +
    // clear_busy), then POST /complete with the old claim_id. The
    // new coordinator must not match the claim and must report the
    // completion as ignored.
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

    // "Crash" the coordinator: drop the server handle, which signals
    // its shutdown channel. Then spin up a new server on the same
    // pool — this runs clear_busy + rehydrate, matching what happens
    // on process restart.
    drop(handle);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let handle2 = spawn_server(pool).await;
    let client2 = CoordinatorClient::new(&handle2.base_url);

    // The durable row is back in state='pending' (cleared by
    // clear_busy on restart).
    let (state_str,): (String,) =
        sqlx::query_as("SELECT state FROM derivations WHERE drv_hash = $1")
            .bind(stale_claim.drv_hash.as_str())
            .fetch_one(&handle2.pool)
            .await
            .unwrap();
    assert_eq!(state_str, "pending", "clear_busy must reset building drvs");

    // The old worker's (belated) completion POST lands on the new
    // coordinator. The new coordinator's in-memory claim map is empty
    // for this claim_id, so complete returns ignored=true.
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

    // A fresh claim can now pick up the drv again.
    let fresh = client2
        .claim(job.id, "x86_64-linux", &[], 5)
        .await
        .unwrap()
        .expect("drv must be claimable post-restart");
    assert_ne!(
        fresh.claim_id, stale_claim.claim_id,
        "new claim gets a new id"
    );
    // Complete cleanly; job goes Done.
    client2
        .complete(
            job.id,
            fresh.claim_id,
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
    assert_eq!(wait_for_terminal(&client2, job.id).await, JobStatus::Done);
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
    for (n, jid) in [(0u32, a.id), (1, a.id), (2, a.id), (3, a.id),
                     (4, b.id), (5, b.id), (6, b.id), (7, b.id)]
    {
        let client = CoordinatorClient::new(&handle.base_url);
        joinset.spawn(async move {
            (
                n,
                jid,
                client.claim(jid, "x86_64-linux", &[], 3).await,
            )
        });
    }

    let mut winners: Vec<(u32, nix_ci_core::types::JobId, nix_ci_core::types::ClaimResponse)> =
        Vec::new();
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

// ─── 5. mark_building failure unwinds cleanly ──────────────────────────

#[sqlx::test]
async fn claim_unwinds_when_mark_building_sees_unexpected_state(pool: PgPool) {
    // Force the writeback path's "zero rows affected" branch: manually
    // flip the durable row to 'done' between ingest and claim. The
    // claim handler calls mark_building, which now returns Conflict
    // on zero rows. The unwind must drop the in-memory claim, re-arm
    // runnable, and re-enqueue for the submission.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let drv = drv_path("uwd", "solo");
    client
        .ingest_drv(job.id, &ingest(&drv, "solo", &[], true))
        .await
        .unwrap();

    // Sneak the durable row into state='done' without going through
    // the normal complete path. The in-memory step still thinks it's
    // pending+runnable, so the claim will try mark_building.
    sqlx::query(
        "UPDATE derivations SET state = 'done', completed_at = now() WHERE drv_hash = $1",
    )
    .bind(
        nix_ci_core::types::drv_hash_from_path(&drv)
            .unwrap()
            .as_str(),
    )
    .execute(&handle.pool)
    .await
    .unwrap();

    // /claim should surface the Conflict as a 409.
    match client.claim(job.id, "x86_64-linux", &[], 1).await {
        Err(nix_ci_core::Error::Conflict(_)) => {}
        other => panic!("expected Conflict, got {other:?}"),
    }

    // The in-memory claim was unwound: claims_in_flight is 0.
    let snap = client.admin_snapshot().await.unwrap();
    assert_eq!(snap.active_claims, 0, "unwind must drop the claim");

    // The step was re-armed: runnable flag set back to true.
    let drv_hash = nix_ci_core::types::drv_hash_from_path(&drv).unwrap();
    let step = handle
        .dispatcher
        .steps
        .get(&drv_hash)
        .expect("step still in registry");
    assert!(
        step.runnable.load(Ordering::Acquire),
        "unwind must re-arm runnable"
    );
}

// ─── 6. Heartbeat-timeout uses configured short window ─────────────────

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
