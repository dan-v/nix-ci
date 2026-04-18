//! End-to-end tests for the per-worker auto-quarantine circuit breaker.
//!
//! **Scope**: auto-quarantine is a FLEET-mode-only lever. The per-job
//! claim endpoint (`/jobs/{id}/claim`) deliberately skips the
//! quarantine check because the worker IS the CI run's only
//! claimant — quarantining it mid-run would hang the job waiting for
//! a worker that won't claim. Fleet mode (`/claim`) is where one
//! sick host's failures otherwise spread across unrelated jobs, so
//! that's where containment matters. See the `per_job_mode_*` tests
//! at the bottom of this file for the deliberate-non-containment
//! contract.
//!
//! Contract verified by this file:
//!
//! * A fleet worker whose reported failures cross `threshold` in
//!   `window` is auto-fenced for `cooldown` — its subsequent
//!   **fleet** claim calls return 204. Manual fence is unchanged.
//! * Other fleet workers (fresh `worker_id`) are unaffected.
//! * Per-job claims by the same (now-quarantined) worker continue
//!   to succeed — the CI run still gets its worker.
//! * The feature is opt-in: defaults leave auto-quarantine off.

mod common;

use common::{drv_path, ingest, spawn_server_with_cfg};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, ErrorCategory, IngestBatchRequest,
};
use sqlx::PgPool;

async fn seed_job_with_drv(
    client: &CoordinatorClient,
    tag: &str,
) -> nix_ci_core::types::JobId {
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let root = drv_path(tag, "pkg");
    let batch = IngestBatchRequest {
        drvs: vec![ingest(&root, "pkg", &[], true)],
        eval_errors: Vec::new(),
    };
    client.ingest_batch(job.id, &batch).await.unwrap();
    client.seal(job.id).await.unwrap();
    job.id
}

async fn fail_claim(
    client: &CoordinatorClient,
    job_id: nix_ci_core::types::JobId,
    claim_id: nix_ci_core::types::ClaimId,
    category: ErrorCategory,
) {
    client
        .complete(
            job_id,
            claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 100,
                exit_code: Some(1),
                error_category: Some(category),
                error_message: Some("simulated".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();
}

#[sqlx::test]
async fn fleet_worker_is_quarantined_on_next_fleet_claim(pool: PgPool) {
    // Threshold=2 → on the 2nd reported failure, the worker is
    // auto-quarantined; the 3rd fleet claim must return 204.
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.worker_quarantine_failure_threshold = Some(2);
        cfg.worker_quarantine_window_secs = 600;
        cfg.worker_quarantine_cooldown_secs = 3600;
        cfg.flaky_retry_backoff_step_ms = 0;
        cfg.max_attempts = 5;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Three distinct jobs so the fleet has work to hand out.
    let j1 = seed_job_with_drv(&client, "alpha").await;
    let j2 = seed_job_with_drv(&client, "beta").await;
    let _j3 = seed_job_with_drv(&client, "gamma").await;

    // Sick worker takes two fleet claims and fails both.
    for _ in 0..2 {
        let c = client
            .claim_any_as_worker("x86_64-linux", &[], 5, Some("sick"))
            .await
            .unwrap()
            .expect("fleet claim succeeds");
        // Before quarantine: these can be from any job, including
        // j1 or j2; we don't care which.
        fail_claim(&client, c.job_id, c.claim_id, ErrorCategory::Transient).await;
    }

    // Third fleet claim — the sick worker is now auto-quarantined
    // and MUST see 204. (Short wait so the assertion is quick.)
    let sick_third = client
        .claim_any_as_worker("x86_64-linux", &[], 2, Some("sick"))
        .await
        .unwrap();
    assert!(
        sick_third.is_none(),
        "quarantined fleet worker must see 204, got {sick_third:?}"
    );

    // A different fleet worker ID picks up the next drv without issue.
    let fresh = client
        .claim_any_as_worker("x86_64-linux", &[], 5, Some("healthy"))
        .await
        .unwrap();
    assert!(
        fresh.is_some(),
        "healthy fleet worker must still receive a claim"
    );
    // Clean up for the drop-based shutdown.
    if let Some(c) = fresh {
        fail_claim(&client, c.job_id, c.claim_id, ErrorCategory::Transient).await;
    }
    let _ = (j1, j2);
}

#[sqlx::test]
async fn per_job_mode_is_not_quarantined(pool: PgPool) {
    // The deliberate non-containment contract: per-job workers are
    // tied to a single CI run. Even if they've tripped the
    // fleet-wide quarantine threshold through some prior fleet work,
    // the per-job claim endpoint MUST still hand them drvs — the
    // user's CI run is supposed to surface its own failures, not
    // hang waiting for a quarantined-but-exclusive worker.
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.worker_quarantine_failure_threshold = Some(2);
        cfg.worker_quarantine_window_secs = 600;
        cfg.worker_quarantine_cooldown_secs = 3600;
        cfg.flaky_retry_backoff_step_ms = 0;
        cfg.max_attempts = 5;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Seed enough drvs that the sick worker can trip quarantine via
    // fleet claims AND still have work waiting for the per-job
    // follow-up.
    let j1 = seed_job_with_drv(&client, "alpha").await;
    let j2 = seed_job_with_drv(&client, "beta").await;
    let j3 = seed_job_with_drv(&client, "gamma").await;

    // Fleet-mode failures → trip quarantine.
    for _ in 0..2 {
        let c = client
            .claim_any_as_worker("x86_64-linux", &[], 5, Some("sick"))
            .await
            .unwrap()
            .expect("fleet claim");
        fail_claim(&client, c.job_id, c.claim_id, ErrorCategory::Transient).await;
    }

    // Sanity: fleet quarantine is active.
    let fleet_denied = client
        .claim_any_as_worker("x86_64-linux", &[], 2, Some("sick"))
        .await
        .unwrap();
    assert!(fleet_denied.is_none(), "fleet path must see 204 post-quarantine");

    // The per-job path for the SAME worker must still succeed.
    // Try each of j1/j2/j3 — the retry-armed drvs make the specific
    // job timing-dependent, but at least one job should have a
    // runnable drv for the sick worker.
    let mut served_somewhere = false;
    for j in [j1, j2, j3] {
        if let Some(c) = client
            .claim_as_worker(j, "x86_64-linux", &[], 2, Some("sick"))
            .await
            .unwrap()
        {
            served_somewhere = true;
            // Cleanup to let the handle drop cleanly.
            fail_claim(&client, j, c.claim_id, ErrorCategory::Transient).await;
            break;
        }
    }
    assert!(
        served_somewhere,
        "per-job claim must succeed for quarantined worker on at least one job"
    );
}

#[sqlx::test]
async fn quarantine_disabled_by_default(pool: PgPool) {
    // Production defaults: threshold unset → auto-quarantine off.
    // Even a worker that fails every fleet drv in sight keeps
    // getting new claims. Feature is strictly opt-in.
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.flaky_retry_backoff_step_ms = 0;
        cfg.max_attempts = 10;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);

    let j1 = seed_job_with_drv(&client, "alpha").await;
    let j2 = seed_job_with_drv(&client, "beta").await;

    // Three fleet failures by the same worker, no auto-fence.
    for _ in 0..3 {
        let c = client
            .claim_any_as_worker("x86_64-linux", &[], 5, Some("badguy"))
            .await
            .unwrap()
            .expect("claim should succeed in default config");
        fail_claim(&client, c.job_id, c.claim_id, ErrorCategory::Transient).await;
    }

    // Fourth attempt: without quarantine enabled, the same sick
    // worker can still claim.
    let c = client
        .claim_any_as_worker("x86_64-linux", &[], 2, Some("badguy"))
        .await
        .unwrap();
    assert!(
        c.is_some(),
        "auto-quarantine must be off by default; got 204 = {c:?}"
    );
    // Cleanup.
    if let Some(c) = c {
        fail_claim(&client, c.job_id, c.claim_id, ErrorCategory::Transient).await;
    }
    let _ = (j1, j2);
}

#[sqlx::test]
async fn buildfailure_also_counts_toward_fleet_quarantine(pool: PgPool) {
    // Any reported failure (Transient OR BuildFailure) counts.
    // The circuit breaker doesn't look at category — a sick
    // worker mis-reporting legitimate drvs as `BuildFailure` is
    // exactly the nightmare we're hardening against, so that
    // path must not be a quarantine blind spot.
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.worker_quarantine_failure_threshold = Some(2);
        cfg.worker_quarantine_window_secs = 600;
        cfg.worker_quarantine_cooldown_secs = 3600;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let _j1 = seed_job_with_drv(&client, "alpha").await;
    let _j2 = seed_job_with_drv(&client, "beta").await;
    for _ in 0..2 {
        let c = client
            .claim_any_as_worker("x86_64-linux", &[], 5, Some("sick"))
            .await
            .unwrap()
            .expect("claim");
        fail_claim(&client, c.job_id, c.claim_id, ErrorCategory::BuildFailure).await;
    }
    let after = client
        .claim_any_as_worker("x86_64-linux", &[], 2, Some("sick"))
        .await
        .unwrap();
    assert!(after.is_none(), "BuildFailure must count toward quarantine");
}

#[sqlx::test]
async fn full_fleet_story_sick_worker_contained(pool: PgPool) {
    // End-to-end story: a mixed fleet where one worker's
    // environment is broken. Without auto-quarantine the sick
    // worker contaminates drvs across unrelated jobs (each drv
    // eats a retry before a healthy worker gets it). With
    // auto-quarantine at threshold=3 it's contained after 3
    // failures, and healthy fleet workers carry the load.
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.worker_quarantine_failure_threshold = Some(3);
        cfg.worker_quarantine_window_secs = 600;
        cfg.worker_quarantine_cooldown_secs = 3600;
        cfg.flaky_retry_backoff_step_ms = 0;
        cfg.max_attempts = 5;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);

    // 5 jobs, 1 drv each.
    let mut jobs = Vec::new();
    for tag in ["alpha", "beta", "gamma", "delta", "epsilon"] {
        jobs.push(seed_job_with_drv(&client, tag).await);
    }

    // Sick worker fleet-claims + fails 3 drvs → trips quarantine.
    for _ in 0..3 {
        let c = client
            .claim_any_as_worker("x86_64-linux", &[], 5, Some("sick"))
            .await
            .unwrap()
            .expect("fleet claim before quarantine");
        fail_claim(&client, c.job_id, c.claim_id, ErrorCategory::Transient).await;
    }

    // Sick worker's next fleet claim must be 204.
    let sick_next = client
        .claim_any_as_worker("x86_64-linux", &[], 2, Some("sick"))
        .await
        .unwrap();
    assert!(
        sick_next.is_none(),
        "sick worker must be quarantined post-threshold"
    );

    // Healthy workers complete all remaining drvs.
    for idx in 0..jobs.len() {
        let worker = format!("healthy-{idx}");
        let claim = client
            .claim_any_as_worker("x86_64-linux", &[], 5, Some(&worker))
            .await
            .unwrap();
        // After all sick-worker-armed retries and the healthy-worker
        // pickups, at some point the fleet runs dry — accept 204 as
        // terminal for the loop.
        let Some(claim) = claim else {
            continue;
        };
        client
            .complete(
                claim.job_id,
                claim.claim_id,
                &CompleteRequest {
                    success: true,
                    duration_ms: 50,
                    exit_code: Some(0),
                    error_category: None,
                    error_message: None,
                    log_tail: None,
                },
            )
            .await
            .unwrap();
    }

    // Quarantine metric must have incremented.
    let body = reqwest::get(format!("{}/metrics", handle.base_url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let parsed: f64 = body
        .lines()
        .find_map(|l| l.strip_prefix("nix_ci_worker_auto_quarantined_total "))
        .and_then(|v| v.parse().ok())
        .expect("counter must be emitted after increment");
    assert!(parsed >= 1.0, "worker_auto_quarantined_total must be >= 1");
}
