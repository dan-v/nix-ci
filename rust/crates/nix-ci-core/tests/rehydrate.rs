//! Crash-recovery tests: drive a coordinator to a known state, tear
//! down the dispatcher, rehydrate from the same Postgres database,
//! and verify the rebuilt state matches what the pre-crash coordinator
//! would have behaved like.
//!
//! These exercise the startup path that a real coordinator restart
//! would take: `clear_busy` → `rehydrate` → serve.

mod common;

use std::time::Duration;

use common::{drv_path, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::dispatch::Dispatcher;
use nix_ci_core::durable::rehydrate;
use nix_ci_core::observability::metrics::Metrics;
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
        cache_status: None,
    }
}

/// Simulate a coordinator restart: drop the old dispatcher, build a
/// fresh one, re-run `clear_busy` + `rehydrate` against the same
/// Postgres.
async fn restart_dispatcher(pool: &PgPool) -> Dispatcher {
    rehydrate::clear_busy(pool).await.expect("clear_busy");
    let dispatcher = Dispatcher::new(Metrics::new());
    rehydrate::rehydrate(pool, &dispatcher)
        .await
        .expect("rehydrate");
    dispatcher
}

#[sqlx::test]
async fn rehydrate_pending_chain_still_dispatches(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    let a = drv_path("aaa", "a");
    let b = drv_path("bbb", "b");
    let c = drv_path("ccc", "c");
    client
        .ingest_drv(job.id, &ingest(&a, "a", &[], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&b, "b", &[&a], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&c, "c", &[&b], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Simulate coordinator crash — drop the dispatcher, but leave PG
    // intact. Since the test's server holds its own dispatcher via
    // AppState, we verify directly via the `rehydrate` entrypoint.
    let rebuilt = restart_dispatcher(&pool).await;

    // The rebuilt dispatcher should contain all 3 drvs + 1 submission,
    // with a armed runnable and b/c waiting on their deps.
    assert_eq!(rebuilt.submissions.len(), 1);
    let live = rebuilt.steps.live();
    assert_eq!(live.len(), 3);

    let find = |needle: &str| {
        live.iter()
            .find(|s| s.drv_path().contains(needle))
            .unwrap()
            .clone()
    };
    let a_step = find("a.drv");
    let b_step = find("b.drv");
    let c_step = find("c.drv");

    assert!(
        a_step.runnable.load(std::sync::atomic::Ordering::Acquire),
        "leaf should be armed after rehydrate"
    );
    assert!(
        !b_step.runnable.load(std::sync::atomic::Ordering::Acquire),
        "b still has dep a"
    );
    assert!(
        !c_step.runnable.load(std::sync::atomic::Ordering::Acquire),
        "c still has dep b"
    );
    assert_eq!(b_step.state.read().deps.len(), 1);
    assert_eq!(c_step.state.read().deps.len(), 1);
}

#[sqlx::test]
async fn rehydrate_resets_in_flight_to_pending(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();

    let solo = drv_path("zzz", "solo");
    client
        .ingest_drv(job.id, &ingest(&solo, "solo", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Claim the step but deliberately don't complete. This simulates
    // a worker that started a build and then the coordinator crashed.
    let claim = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    // Verify pre-crash state: drv is building in PG.
    let pre: (String,) = sqlx::query_as("SELECT state FROM derivations WHERE drv_hash = $1")
        .bind(claim.drv_hash.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(pre.0, "building");

    // Restart coordinator.
    drop(handle); // tear down first server
    let rebuilt = restart_dispatcher(&pool).await;

    // After rehydrate, the drv should be back to pending + runnable.
    let post: (String, Option<sqlx::types::Uuid>) =
        sqlx::query_as("SELECT state, assigned_claim_id FROM derivations WHERE drv_hash = $1")
            .bind(claim.drv_hash.as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(post.0, "pending");
    assert!(post.1.is_none(), "claim_id should be cleared");

    // In-memory: the rebuilt dispatcher should have the step as runnable.
    let live = rebuilt.steps.live();
    let step = live
        .iter()
        .find(|s| s.drv_path() == solo)
        .expect("step present");
    assert!(
        step.runnable.load(std::sync::atomic::Ordering::Acquire),
        "rehydrated step should be armed"
    );
    assert!(!step.finished.load(std::sync::atomic::Ordering::Acquire));
}

#[sqlx::test]
async fn rehydrate_preserves_done_drvs(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job = client
        .create_job(&CreateJobRequest { external_ref: None })
        .await
        .unwrap();
    let a = drv_path("aaa", "a");
    let b = drv_path("bbb", "b");
    client
        .ingest_drv(job.id, &ingest(&a, "a", &[], false))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&b, "b", &[&a], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Complete a, leaving b pending.
    let claim_a = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim a");
    client
        .complete(
            job.id,
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

    // Let rdep propagation settle before restart.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Restart.
    drop(handle);
    let rebuilt = restart_dispatcher(&pool).await;

    // Rehydrate loads only (a) non-terminal drvs and (b) roots of
    // live jobs. Done intermediate drvs (here: `a`) are intentionally
    // NOT reconstructed — their outputs are substitutable from the
    // Nix store. The contract is: whatever rehydrate loads must be
    // consistent (roots reachable), and runnable drvs must be armed.
    let live = rebuilt.steps.live();
    let b_step = live
        .iter()
        .find(|s| s.drv_path() == b)
        .expect("b present — it's the live root");
    assert!(
        b_step.runnable.load(std::sync::atomic::Ordering::Acquire),
        "b should be armed: its dep a was already done and so not \
         attached; b is treated as a leaf of the rehydrated graph."
    );
    // Confirm a is NOT in the live set — it's been GC'd from the
    // registry because no submission member nor any dep references it.
    assert!(
        live.iter().all(|s| s.drv_path() != a),
        "finished intermediate drv should not be reconstructed"
    );
}

#[sqlx::test]
async fn rehydrate_restores_cross_job_dedup(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);

    let job_a = client
        .create_job(&CreateJobRequest {
            external_ref: Some("a".into()),
        })
        .await
        .unwrap();
    let job_b = client
        .create_job(&CreateJobRequest {
            external_ref: Some("b".into()),
        })
        .await
        .unwrap();
    let shared = drv_path("shh", "shared");
    client
        .ingest_drv(job_a.id, &ingest(&shared, "shared", &[], true))
        .await
        .unwrap();
    client
        .ingest_drv(job_b.id, &ingest(&shared, "shared", &[], true))
        .await
        .unwrap();
    client.seal(job_a.id).await.unwrap();
    client.seal(job_b.id).await.unwrap();

    drop(handle);
    let rebuilt = restart_dispatcher(&pool).await;

    assert_eq!(rebuilt.submissions.len(), 2);
    let live = rebuilt.steps.live();
    assert_eq!(live.len(), 1, "cross-job dedup must survive rehydrate");
    let step = &live[0];

    // Both submissions should hold weak refs to the shared step.
    let n_subs = step
        .state
        .read()
        .submissions
        .iter()
        .filter(|w| w.strong_count() > 0)
        .count();
    assert_eq!(n_subs, 2, "shared step must re-link to both submissions");
}

#[sqlx::test]
async fn rehydrate_full_server_round_trip(pool: PgPool) {
    // Full HTTP roundtrip: create + ingest + complete via HTTP, then
    // spin up a second `spawn_server` against the same PG and verify
    // the new server sees the completed state.
    {
        let handle = spawn_server(pool.clone()).await;
        let client = CoordinatorClient::new(&handle.base_url);
        let job = client
            .create_job(&CreateJobRequest { external_ref: None })
            .await
            .unwrap();
        let drv = drv_path("rrr", "r");
        client
            .ingest_drv(job.id, &ingest(&drv, "r", &[], true))
            .await
            .unwrap();
        client.seal(job.id).await.unwrap();
        let claim = client
            .claim(job.id, "x86_64-linux", &[], 3)
            .await
            .unwrap()
            .expect("claim");
        client
            .complete(
                job.id,
                claim.claim_id,
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
        // Wait for terminal transition to persist.
        for _ in 0..20 {
            let s = client.status(job.id).await.unwrap();
            if s.status.is_terminal() {
                assert_eq!(s.status, JobStatus::Done);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(handle);
    }
    // Second server against the same DB. The job is terminal in PG;
    // rehydrate only rebuilds non-terminal jobs, so no in-memory
    // submission exists.
    let handle2 = spawn_server(pool).await;
    let rows: Vec<(String,)> = sqlx::query_as("SELECT id::text FROM jobs WHERE status = 'done'")
        .fetch_all(&handle2.pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // Keep server alive until end of test.
    let _keepalive = handle2;
}
