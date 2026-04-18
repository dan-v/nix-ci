//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: claim-deadline and heartbeat-timeout reaper invariants.

mod common;

use std::time::Duration;

use common::{drv_path, ingest};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CreateJobRequest, IngestBatchRequest, IngestDrvRequest};
use sqlx::PgPool;

#[sqlx::test]
async fn reaper_does_not_re_arm_step_finished_via_propagation(pool: PgPool) {
    // Race the reaper would otherwise lose: a step's claim has expired,
    // but before the reaper sets `runnable=true`, propagation marks
    // the step finished (e.g. an upstream sibling's failure cascaded).
    // Post-condition: NO step ends up with both `finished=true` AND
    // `runnable=true` — that would violate the dispatcher invariant
    // and leave an orphan entry in the ready queue.
    use std::sync::atomic::Ordering;

    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = 1;
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
    let drv = drv_path("racefin", "pkg");
    client
        .ingest_drv(job.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");

    // Force claim deadline to the past.
    let claim = handle
        .dispatcher
        .claims
        .take(c.claim_id)
        .expect("claim present");
    *claim.deadline.lock() = tokio::time::Instant::now() - Duration::from_secs(10);
    handle.dispatcher.claims.insert(claim);

    // Mark the step finished BEFORE the reaper runs — simulates
    // propagation from a concurrent failure beating the reaper to
    // the punch.
    let drv_hash = nix_ci_core::types::drv_hash_from_path(&drv).unwrap();
    let step = handle
        .dispatcher
        .steps
        .get(&drv_hash)
        .expect("step present");
    step.previous_failure.store(true, Ordering::Release);
    step.finished.store(true, Ordering::Release);

    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);

    // Invariant: a finished step must NOT also be runnable. Otherwise
    // pop_runnable wastes work and the admin snapshot is inconsistent.
    assert!(
        step.finished.load(Ordering::Acquire),
        "test setup: step should still be finished"
    );
    assert!(
        !step.runnable.load(Ordering::Acquire),
        "reaper must not re-arm a step that became finished concurrently"
    );
}

#[sqlx::test]
async fn reaper_rearms_many_concurrent_expired_claims(pool: PgPool) {
    // Scenario: 50 workers claim 50 drvs, all disappear (no complete).
    // A reaper tick fires when every claim is past its deadline — all
    // 50 steps must be re-armed with runnable=true (attempt++), the
    // claims gauge must return to 0, and no step gets stuck.
    use nix_ci_core::config::ServerConfig;
    const N: usize = 50;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = 1;
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
    let drvs: Vec<IngestDrvRequest> = (0..N)
        .map(|i| {
            ingest(
                &drv_path(&format!("conc{i:03}"), &format!("n{i}")),
                &format!("n{i}"),
                &[],
                true,
            )
        })
        .collect();
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

    // Fire N claims in parallel.
    let mut claim_ids = Vec::with_capacity(N);
    for _ in 0..N {
        let c = client
            .claim(job.id, "x86_64-linux", &[], 3)
            .await
            .unwrap()
            .expect("claim");
        claim_ids.push(c.claim_id);
    }
    assert_eq!(handle.dispatcher.claims.len(), N);

    // Force ALL claim deadlines to the past.
    let past = tokio::time::Instant::now() - Duration::from_secs(10);
    for cid in &claim_ids {
        if let Some(claim) = handle.dispatcher.claims.take(*cid) {
            *claim.deadline.lock() = past;
            handle.dispatcher.claims.insert(claim);
        }
    }

    // One reaper tick reaps every expired claim.
    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);

    // All claims evicted.
    assert_eq!(
        handle.dispatcher.claims.len(),
        0,
        "all expired claims must be evicted in one tick"
    );

    // Every step must be runnable again; workers should be able to
    // claim again. Drain the new claims and confirm we get exactly N.
    let mut second_wave = 0;
    for _ in 0..N {
        if let Ok(Some(_)) = client.claim(job.id, "x86_64-linux", &[], 3).await {
            second_wave += 1;
        }
    }
    assert_eq!(
        second_wave, N,
        "all N steps must be claimable again after reap"
    );
}

#[sqlx::test]
async fn reaper_rearms_non_finished_expired_claim(pool: PgPool) {
    // A claim whose deadline passed must be evicted AND its step
    // re-armed (`runnable=true` + enqueued), so another worker picks
    // it up. If the `!step.finished` guard in reaper is flipped/deleted
    // the reaper would skip the re-arm or re-arm a finished step —
    // breaking either progress or invariants.
    use nix_ci_core::config::ServerConfig;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.claim_deadline_secs = 1; // short deadline for test
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
    let drv = drv_path("rearm", "pkg");
    client
        .ingest_drv(job.id, &ingest(&drv, "pkg", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Worker A claims, then "disappears" (doesn't complete).
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim A");

    // Force the claim's in-memory deadline to expire immediately by
    // dialing it down to now, then invoke reaper.
    let claim = handle
        .dispatcher
        .claims
        .take(c.claim_id)
        .expect("claim present");
    let past = tokio::time::Instant::now() - Duration::from_secs(10);
    *claim.deadline.lock() = past;
    handle.dispatcher.claims.insert(claim);

    nix_ci_core::durable::reaper::reap_expired_claims(&handle.dispatcher);

    // Worker B should now be able to claim the same drv again.
    let c2 = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim B after re-arm");
    assert_eq!(c2.drv_path, drv);
    assert_eq!(c2.attempt, 2, "re-armed claim must increment attempt");
}
