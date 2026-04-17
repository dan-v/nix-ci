//! H15.D regression: hard-to-get-right API contracts that the audit
//! flagged as untested. These are adversarial / racy shapes a
//! production coordinator will hit and that would silently do the
//! wrong thing if the implementation drifted.

use std::time::Duration;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CompleteRequest, CreateJobRequest, IngestBatchRequest, IngestDrvRequest,
};
use sqlx::PgPool;

mod common;

fn drv_path(hash: &str, name: &str) -> String {
    format!("/nix/store/{hash}-{name}.drv")
}

fn ingest(drv: &str, name: &str, is_root: bool) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root,
        attr: None,
    }
}

/// Cross-job claim attempt: worker claims a drv belonging to job A
/// but POSTs /complete against job B's path. The server must
/// reject — otherwise a buggy client (or a hostile one with a valid
/// token but wrong claim_id tracking) could complete work for an
/// unrelated job and corrupt that job's state.
///
/// Implementation: server/complete.rs rejects when claim.job_id !=
/// path.job_id with a 400. This test is the only regression guard
/// for that check.
#[sqlx::test]
async fn complete_rejects_cross_job_claim_id(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    let job_a = client
        .create_job(&CreateJobRequest {
            external_ref: Some("a".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let job_b = client
        .create_job(&CreateJobRequest {
            external_ref: Some("b".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    // Each job gets its own drv so both are claimable.
    let drv_a = drv_path("aaa", "a-drv");
    client
        .ingest_batch(
            job_a.id,
            &IngestBatchRequest {
                drvs: vec![ingest(&drv_a, "a-drv", true)],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    client
        .ingest_batch(
            job_b.id,
            &IngestBatchRequest {
                drvs: vec![ingest(&drv_path("bbb", "b-drv"), "b-drv", true)],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();

    // Worker claims drv A under job A.
    let c_a = client
        .claim(job_a.id, "x86_64-linux", &[], 2)
        .await
        .unwrap()
        .expect("claim A");

    // Attempt to complete the A-claim via job B's path. Must be
    // rejected — the server's job-id binding is the integrity
    // boundary that prevents work from being misattributed.
    let resp = client
        .complete(
            job_b.id,
            c_a.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: 1,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await;
    match resp {
        Err(nix_ci_core::Error::BadRequest(msg)) => {
            assert!(
                msg.contains("claim_id") || msg.contains("job"),
                "error must name the contract violation; got: {msg}"
            );
        }
        other => panic!("expected 400 BadRequest for cross-job complete; got: {other:?}"),
    }

    // And the original claim is still alive on job A — the failed
    // cross-post must NOT have taken the claim. Complete it legit
    // and verify job A reaches Done cleanly.
    let resp_ok = client
        .complete(
            job_a.id,
            c_a.claim_id,
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
    assert!(
        !resp_ok.ignored,
        "legit complete on correct job must NOT be ignored (the rejected cross-job attempt must not have eaten the claim)"
    );
}

/// Concurrent seal: two callers POST /seal for the same job
/// simultaneously. Seal is idempotent — both should return a
/// successful snapshot, neither should 4xx. The dispatcher should
/// not double-transition, metrics should increment exactly once.
#[sqlx::test]
async fn concurrent_seal_is_idempotent(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = std::sync::Arc::new(CoordinatorClient::new(handle.base_url.clone()));

    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    // Empty ingest so the seal immediately terminalizes (no drvs =
    // no toplevels = Done). This exercises the seal + terminalize
    // path back-to-back under concurrency.
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: Vec::new(),
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..4 {
        let c = client.clone();
        let id = job.id;
        handles.push(tokio::spawn(async move { c.seal(id).await }));
    }

    let mut ok_count = 0;
    let mut err_count = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => ok_count += 1,
            Err(_) => err_count += 1,
        }
    }
    // All four callers must succeed — seal is idempotent; a racing
    // seal is not an error. If this fails as "only one succeeded"
    // the dispatcher is using a one-shot flag without idempotent
    // return semantics, which would break retry-aware submitters.
    assert_eq!(
        ok_count, 4,
        "4 concurrent seals: all must succeed; got {ok_count} ok / {err_count} err"
    );

    // Exactly one terminal-transition counter tick on /metrics,
    // not four. Scraped through the observable surface so any
    // regression in how metrics are EXPOSED (label order, name
    // suffixing, missing registration) also trips the test.
    let counter = common::scrape_metric_expect(
        &handle.base_url,
        "nix_ci_jobs_terminal_total",
        &[("status", "done")],
    )
    .await;
    assert_eq!(
        counter, 1.0,
        "exactly one jobs_terminal{{status=done}} tick on /metrics; got {counter}"
    );
}

/// /complete against a nonexistent job returns 404 or treats as
/// stale-claim. Either is acceptable; what must NOT happen is a
/// 500 / panic / silent success. A malformed worker requesting
/// complete for a reaped job is a routine scenario.
#[sqlx::test]
async fn complete_on_nonexistent_job_is_bounded(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());

    let fake_job = nix_ci_core::types::JobId::new();
    let fake_claim = nix_ci_core::types::ClaimId::new();
    let resp = client
        .complete(
            fake_job,
            fake_claim,
            &CompleteRequest {
                success: true,
                duration_ms: 1,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await;
    // Success with ignored:true is the expected path (claim not
    // in the map → treat as stale). A NotFound is also acceptable.
    // Anything else (esp. 500) is a regression.
    match resp {
        Ok(r) => assert!(
            r.ignored,
            "complete with unknown claim_id must surface ignored:true, not success"
        ),
        Err(nix_ci_core::Error::NotFound(_)) => {}
        Err(e) => panic!("unexpected error shape for unknown job+claim: {e}"),
    }
}

/// /complete retry with the exact same claim_id is safe —
/// idempotent. Worker retries on network error; server must not
/// double-process. Already tested in http_e2e; this variant
/// additionally verifies the complete counter increments exactly
/// once even on 3+ retries.
#[sqlx::test]
async fn complete_retry_is_idempotent_even_at_metric_level(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("ret", "retry");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![ingest(&drv, "retry", true)],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 2)
        .await
        .unwrap()
        .expect("claim");

    // Baseline counter from /metrics (not present == 0 — counters
    // only emit once incremented).
    let counter_before = common::scrape_metric(
        &handle.base_url,
        "nix_ci_builds_completed_total",
        &[("outcome", "success")],
    )
    .await
    .unwrap_or(0.0);

    for i in 0..3 {
        let resp = client
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
        if i == 0 {
            assert!(!resp.ignored, "first complete must be accepted");
        } else {
            assert!(
                resp.ignored,
                "retries of an already-completed claim must return ignored:true"
            );
        }
    }

    let counter_after = common::scrape_metric_expect(
        &handle.base_url,
        "nix_ci_builds_completed_total",
        &[("outcome", "success")],
    )
    .await;
    assert_eq!(
        counter_after - counter_before,
        1.0,
        "3 complete retries must produce exactly 1 success increment on /metrics"
    );
}

/// A long-poll /claim that times out returns 204 with bounded
/// latency, not 500 / panic. The wait=0 case especially exercises
/// the "no wait, just check once" edge that the deadline math
/// could mishandle.
#[sqlx::test]
async fn zero_wait_claim_returns_204_immediately(pool: PgPool) {
    let handle = common::spawn_server(pool).await;
    let client = CoordinatorClient::new(handle.base_url.clone());
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let r = client
        .claim(job.id, "x86_64-linux", &[], 0)
        .await
        .unwrap();
    assert!(r.is_none(), "empty job must return None on zero-wait claim");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "zero-wait claim must short-circuit; took {elapsed:?}"
    );
}
