//! Black-box regression guards for the 8 dispatcher invariants.
//!
//! Complement to `tests/invariants.rs` (fast in-memory unit tests on
//! the dispatch state machine) and `tests/property.rs` (randomized
//! state-machine property tests). Those verify the invariants by
//! asserting on the public atomics of `Step` / `Submission` — fast,
//! deterministic, but tied to the specific in-memory representation.
//!
//! This file is the *contract* layer: each invariant is expressed as
//! an observable shape on the HTTP surface or on the scraped
//! `/metrics` endpoint. If a future refactor rearranges the internal
//! fields but preserves the contract, these tests stay green. If the
//! contract itself breaks — a drv silently skipped, a duplicate
//! claim issued, a step that outlives every submission that owned
//! it — these fail.
//!
//! Covered invariants (numbering from `dispatch/mod.rs`):
//! * **3** — Steps registry dedups on drv_hash (observable: the same
//!   drv in two concurrent jobs is only claimed once in total).
//! * **1 / 4** — runnable ⟹ deps-empty; `make_rdeps_runnable` respects
//!   the created barrier (observable: a parent drv can't be claimed
//!   until its leaf completes).
//! * **3 + CAS** — exactly-once claim under cross-submission race
//!   (observable: two parallel workers targeting the same shared drv
//!   get exactly one ClaimResponse and one 204 between them).
//! * **8** — Steps registry holds only `Weak<Step>` (observable: the
//!   `/admin/snapshot` `steps_registry_size` returns to 0 after all
//!   submissions terminate).

mod common;

use std::time::Duration;

use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    ClaimResponse, CompleteRequest, CreateJobRequest, IngestBatchRequest,
};
use sqlx::PgPool;

/// Invariant 3 (Steps dedup): a drv_hash shared across two concurrent
/// jobs maps to a single dispatcher Step. Observable consequence: the
/// drv can only be claimed once *in total* across both jobs. Worker
/// finishes in one submission, the second submission inherits the
/// result via `make_rdeps_runnable` — no re-dispatch.
///
/// Guards against a future change that accidentally dropped dedup and
/// built the same drv twice, which would silently double build-time
/// spend on a giant overlay with heavy cross-job drv sharing.
#[sqlx::test]
async fn cross_job_shared_drv_dedups_at_registry_level(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Two jobs with the same drv (same drv_path ⟹ same drv_hash).
    let shared_drv = drv_path("shared", "libcommon");
    let job_a = client.create_job(&CreateJobRequest::default()).await.unwrap();
    let job_b = client.create_job(&CreateJobRequest::default()).await.unwrap();
    for jid in [job_a.id, job_b.id] {
        client
            .ingest_drv(jid, &ingest(&shared_drv, "libcommon", &[], true))
            .await
            .unwrap();
        client.seal(jid).await.unwrap();
    }

    // Claim from job A — should get the drv.
    let claim_a = client
        .claim(job_a.id, "x86_64-linux", &[], 2)
        .await
        .unwrap()
        .expect("first job must be able to claim its only drv");

    // Claim from job B — should get NOTHING. The step is shared and
    // already runnable=false after A's CAS win. A broken dedup would
    // issue a second claim with a fresh claim_id.
    let claim_b = client.claim(job_b.id, "x86_64-linux", &[], 1).await.unwrap();
    assert!(
        claim_b.is_none(),
        "cross-job dedup broken: job B received an independent claim for the shared drv"
    );

    // Complete A — B must transition to Done via shared finish, no
    // further builds required.
    client
        .complete(
            job_a.id,
            claim_a.claim_id,
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

    // Both jobs reach Done (propagation happens in-memory; allow a
    // moment for the async terminal-publish writeback).
    for jid in [job_a.id, job_b.id] {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let s = client.status(jid).await.unwrap();
            if matches!(s.status, nix_ci_core::types::JobStatus::Done) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "shared drv never propagated to job {jid}; status={:?}",
                    s.status
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// Invariant 1 / 4 (runnable precondition): a parent drv with a
/// dependency must NOT be claimable until the dep has completed
/// successfully. Observable via two consecutive claims.
///
/// Under a broken implementation, the parent would appear in the
/// ready queue before its dep finished — workers would run
/// `nix build` on incomplete closures and the resulting failure would
/// look like the build's fault when it was actually a dispatcher bug.
#[sqlx::test]
async fn parent_not_claimable_until_dep_completes(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client.create_job(&CreateJobRequest::default()).await.unwrap();
    let leaf = drv_path("l", "leaf");
    let parent = drv_path("p", "parent");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![
                    ingest(&leaf, "leaf", &[], false),
                    ingest(&parent, "parent", &[&leaf], true),
                ],
                eval_errors: vec![],
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // First claim MUST be the leaf; parent is still blocked by the dep.
    let first = client
        .claim(job.id, "x86_64-linux", &[], 2)
        .await
        .unwrap()
        .expect("leaf must be claimable after seal");
    assert_eq!(
        first.drv_path, leaf,
        "a parent was offered while its dep was still pending — Invariant 1 broken"
    );

    // A second claim (before completing the leaf) must return 204 /
    // None: parent's dep is still in-flight, nothing else is ready.
    let second = client.claim(job.id, "x86_64-linux", &[], 1).await.unwrap();
    assert!(
        second.is_none(),
        "parent was claimable before its dep finished — got {:?}",
        second.map(|c: ClaimResponse| c.drv_path)
    );

    // Complete leaf → parent becomes runnable → next claim hands it out.
    client
        .complete(
            job.id,
            first.claim_id,
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
    let parent_claim = client
        .claim(job.id, "x86_64-linux", &[], 2)
        .await
        .unwrap()
        .expect("parent must become claimable after leaf completes");
    assert_eq!(parent_claim.drv_path, parent);
}

/// Invariant 3 + CAS (exactly-once under cross-submission race): the
/// same drv is submitted to two jobs; both seal; workers for both
/// jobs race on `/claim` in parallel. The registry's CAS must ensure
/// only one ClaimResponse is issued — the other worker sees 204.
///
/// This is the load-bearing safety property for dedup: a future
/// rewrite that accidentally gave each submission its own
/// step-runnable flag would let the same drv build twice under load.
#[sqlx::test]
async fn cross_submission_claim_cas_exactly_one_winner(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let shared_drv = drv_path("rr", "race");
    let job_a = {
        let c = CoordinatorClient::new(&handle.base_url);
        let j = c.create_job(&CreateJobRequest::default()).await.unwrap();
        c.ingest_drv(j.id, &ingest(&shared_drv, "race", &[], true)).await.unwrap();
        c.seal(j.id).await.unwrap();
        j.id
    };
    let job_b = {
        let c = CoordinatorClient::new(&handle.base_url);
        let j = c.create_job(&CreateJobRequest::default()).await.unwrap();
        c.ingest_drv(j.id, &ingest(&shared_drv, "race", &[], true)).await.unwrap();
        c.seal(j.id).await.unwrap();
        j.id
    };

    // Fire both claims in parallel so the race is real (not
    // first-come-first-served by the test driver).
    let base = handle.base_url.clone();
    let claim_a = tokio::spawn(async move {
        CoordinatorClient::new(&base)
            .claim(job_a, "x86_64-linux", &[], 3)
            .await
    });
    let base = handle.base_url.clone();
    let claim_b = tokio::spawn(async move {
        CoordinatorClient::new(&base)
            .claim(job_b, "x86_64-linux", &[], 3)
            .await
    });
    let (ra, rb) = tokio::join!(claim_a, claim_b);
    let a = ra.unwrap().unwrap();
    let b = rb.unwrap().unwrap();
    let winners = a.is_some() as u32 + b.is_some() as u32;
    assert_eq!(
        winners, 1,
        "exactly one of the racing jobs must win the CAS; got {winners} (a={}, b={})",
        a.is_some(),
        b.is_some()
    );
}

/// Invariant 8 (weak registry): after every submission that referenced
/// a drv terminates, the registry GCs the entry. Observable through
/// the `/metrics` gauge `nix_ci_steps_registry_size`, which is the
/// registry's live-Weak-upgrade count refreshed on every scrape.
///
/// Guards against a refactor that accidentally held a strong
/// `Arc<Step>` in a long-lived place (metrics, event bus, dispatcher
/// tables). In production, such a leak would present as
/// `steps_registry_size` climbing monotonically across redeploys of
/// the same overlay — an OOM fuse that takes hours to trip.
#[sqlx::test]
async fn steps_registry_returns_to_zero_after_all_terminal(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Drive a handful of independent single-drv jobs through Done.
    // Each creates one Step; after terminal, all should GC.
    for i in 0..5 {
        let job = client.create_job(&CreateJobRequest::default()).await.unwrap();
        let d = drv_path(&format!("gc{i:02}"), "solo");
        client.ingest_drv(job.id, &ingest(&d, "solo", &[], true)).await.unwrap();
        client.seal(job.id).await.unwrap();
        let c = client
            .claim(job.id, "x86_64-linux", &[], 2)
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
    }

    // Poll the Prometheus gauge for up to 3s — should drop to 0 as
    // submissions drain and Weak refs fail to upgrade. A non-zero
    // floor means a leak somewhere on the terminal path.
    //
    // Using the scraped wire format (rather than a direct
    // dispatcher.steps.len() read) is deliberate: this file is the
    // contract-surface guard. The gauge is what Prometheus actually
    // serves, and a future representation change that breaks the
    // observable contract is exactly what this test should catch.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let gauge = common::scrape_metric_expect(
            &handle.base_url,
            "nix_ci_steps_registry_size",
            &[],
        )
        .await;
        let snap = client.admin_snapshot().await.unwrap();
        if gauge == 0.0 && snap.submissions == 0 {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "registry leak: steps_registry_size={gauge} after 3s quiesce; \
                 submissions still in memory={}",
                snap.submissions
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
