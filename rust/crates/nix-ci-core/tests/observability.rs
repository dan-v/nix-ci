//! Operator-facing observability: GET /jobs, by-external-ref,
//! snapshot.recent_failures, and the SSE event enrichments
//! (Progress.in_flight/propagated/retries/sealed,
//! DrvFailed.used_by_attrs).
//!
//! These cover the "I see N failures in metrics — which jobs and what
//! broke?" path end-to-end, plus the runner-output state-machine
//! prerequisites.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{drv_path, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    AdminSnapshot, CompleteRequest, CreateJobRequest, ErrorCategory, IngestBatchRequest,
    IngestDrvRequest, JobStatus,
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
        attr: None,
    }
}

fn ingest_root_attr(drv: &str, name: &str, attr: &str) -> IngestDrvRequest {
    let mut r = ingest(drv, name, &[], true);
    r.attr = Some(attr.to_string());
    r
}

async fn drive_to_failure(client: &CoordinatorClient, job_id: nix_ci_core::types::JobId) {
    let c = client
        .claim(job_id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    client
        .complete(
            job_id,
            c.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 1,
                exit_code: Some(1),
                error_category: Some(ErrorCategory::BuildFailure),
                error_message: Some("nope".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();
    for _ in 0..40 {
        if client.status(job_id).await.unwrap().status.is_terminal() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ─── GET /jobs?status=failed&since=...&cursor=... ────────────────────

#[sqlx::test]
async fn list_jobs_returns_failures_newest_first(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);

    // Create + fail three jobs in order.
    let mut ids = Vec::new();
    for i in 0..3 {
        let job = client
            .create_job(&CreateJobRequest {
                external_ref: Some(format!("ext-{i}")),
                ..Default::default()
            })
            .await
            .unwrap();
        let drv = drv_path(&format!("lf{i}"), &format!("p{i}"));
        client
            .ingest_drv(job.id, &ingest(&drv, &format!("p{i}"), &[], true))
            .await
            .unwrap();
        client.seal(job.id).await.unwrap();
        drive_to_failure(&client, job.id).await;
        ids.push(job.id);
        // Tiny gap so done_at differs deterministically.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let resp = client.list_jobs("failed", None, None, 50).await.unwrap();
    assert_eq!(resp.jobs.len(), 3);
    // Newest first → reverse order from creation.
    assert_eq!(resp.jobs[0].id, ids[2]);
    assert_eq!(resp.jobs[1].id, ids[1]);
    assert_eq!(resp.jobs[2].id, ids[0]);
    // Each summary has the originating drv name.
    for j in &resp.jobs {
        assert_eq!(j.originating_failures_total, 1);
        assert!(!j.originating_failures.is_empty());
        assert_eq!(j.status, JobStatus::Failed);
    }
}

#[sqlx::test]
async fn list_jobs_paginates_by_cursor(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    for i in 0..5 {
        let job = client
            .create_job(&CreateJobRequest {
                external_ref: Some(format!("p{i}")),
                ..Default::default()
            })
            .await
            .unwrap();
        client
            .ingest_drv(
                job.id,
                &ingest(
                    &drv_path(&format!("c{i}"), &format!("d{i}")),
                    "x",
                    &[],
                    true,
                ),
            )
            .await
            .unwrap();
        client.seal(job.id).await.unwrap();
        drive_to_failure(&client, job.id).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let page1 = client.list_jobs("failed", None, None, 2).await.unwrap();
    assert_eq!(page1.jobs.len(), 2);
    let cursor = page1.next_cursor.expect("cursor present mid-pagination");
    let page2 = client
        .list_jobs("failed", None, Some(cursor), 2)
        .await
        .unwrap();
    assert_eq!(page2.jobs.len(), 2);
    let cursor2 = page2.next_cursor.expect("cursor still present");
    let page3 = client
        .list_jobs("failed", None, Some(cursor2), 2)
        .await
        .unwrap();
    assert_eq!(page3.jobs.len(), 1, "5 total, last page has the remainder");
    assert!(page3.next_cursor.is_none(), "no cursor on last page");

    let all_ids: Vec<_> = page1
        .jobs
        .iter()
        .chain(page2.jobs.iter())
        .chain(page3.jobs.iter())
        .map(|j| j.id)
        .collect();
    let dedup: std::collections::HashSet<_> = all_ids.iter().cloned().collect();
    assert_eq!(dedup.len(), 5, "no overlap across pages");
}

// ─── GET /jobs/by-external-ref/{ref} ─────────────────────────────────

#[sqlx::test]
async fn show_job_resolves_uuid_and_external_ref(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("two-paths".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&drv_path("two", "x"), "x", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();
    drive_to_failure(&client, job.id).await;

    let by_uuid = client.show_job(&job.id.0.to_string()).await.unwrap();
    let by_ref = client.show_job("two-paths").await.unwrap();
    assert_eq!(by_uuid.id, by_ref.id);
}

// ─── /admin/snapshot.recent_failures ─────────────────────────────────

#[sqlx::test]
async fn snapshot_recent_failures_lists_top_5_newest(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    for i in 0..7 {
        let job = client
            .create_job(&CreateJobRequest {
                external_ref: Some(format!("rec-{i}")),
                ..Default::default()
            })
            .await
            .unwrap();
        client
            .ingest_drv(
                job.id,
                &ingest(&drv_path(&format!("rc{i}"), "x"), "x", &[], true),
            )
            .await
            .unwrap();
        client.seal(job.id).await.unwrap();
        drive_to_failure(&client, job.id).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let snap: AdminSnapshot = reqwest::get(format!("{}/admin/snapshot", handle.base_url))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(snap.recent_failures.len(), 5, "snapshot caps at 5");
    // Newest first → ext-refs rec-6, rec-5, rec-4, rec-3, rec-2
    let refs: Vec<_> = snap
        .recent_failures
        .iter()
        .map(|j| j.external_ref.as_deref().unwrap())
        .collect();
    assert_eq!(refs, vec!["rec-6", "rec-5", "rec-4", "rec-3", "rec-2"]);
}

// ─── DrvFailed.used_by_attrs (rdep walk + attribution) ───────────────

#[sqlx::test]
async fn drv_failed_event_carries_used_by_attrs(pool: PgPool) {
    use eventsource_stream::Eventsource;
    use futures::StreamExt;

    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // Graph: leaf ← mid ← root. Ingest mid + leaf as non-root deps;
    // root is the toplevel WITH an attr name.
    let leaf = drv_path("uba-l", "leaf");
    let mid = drv_path("uba-m", "mid");
    let root = drv_path("uba-r", "root");
    client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![
                    ingest(&leaf, "leaf", &[], false),
                    ingest(&mid, "mid", &[&leaf], false),
                    {
                        let mut r = ingest(&root, "root", &[&mid], true);
                        r.attr = Some("packages.x86_64-linux.helloApp".into());
                        r
                    },
                ],
                eval_errors: Vec::new(),
            },
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Subscribe for SSE before the failure.
    let events_url = format!("{}/jobs/{}/events", handle.base_url, job.id);
    let sse_task = tokio::spawn(async move {
        let resp = reqwest::Client::new()
            .get(&events_url)
            .header("accept", "text/event-stream")
            .send()
            .await
            .unwrap();
        let mut stream = resp.bytes_stream().eventsource();
        while let Some(ev) = stream.next().await {
            let ev = ev.unwrap();
            if ev.event == "drv_failed" {
                return ev.data;
            }
        }
        panic!("no drv_failed event");
    });

    // Fail the leaf — propagation should mark mid + root failed.
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(c.drv_path, leaf);
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: false,
                duration_ms: 1,
                exit_code: Some(1),
                error_category: Some(ErrorCategory::BuildFailure),
                error_message: Some("boom".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();

    let payload = tokio::time::timeout(Duration::from_secs(5), sse_task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        payload.contains("\"used_by_attrs\":[\"packages.x86_64-linux.helloApp\"]"),
        "drv_failed must report attr attribution; got: {payload}"
    );
}

// ─── Progress event enrichments (in_flight, propagated, retries, sealed) ──

#[sqlx::test]
async fn progress_event_carries_enrichments(pool: PgPool) {
    // Progress events are spliced into the SSE response stream by
    // events.rs (not published to the broadcast channel) — open a
    // real HTTP SSE connection to observe them.
    use eventsource_stream::Eventsource;
    use futures::StreamExt;
    use nix_ci_core::config::ServerConfig;

    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.progress_tick_secs = 1; // tighter than default 10s for test speed
    })
    .await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .ingest_drv(
            job.id,
            &ingest_root_attr(&drv_path("prg", "x"), "x", "attrs.x"),
        )
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Issue a claim so the Progress event reports it as in_flight.
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");

    let events_url = format!("{}/jobs/{}/events", handle.base_url, job.id);
    let payload = tokio::time::timeout(Duration::from_secs(5), async {
        let resp = reqwest::Client::new()
            .get(&events_url)
            .header("accept", "text/event-stream")
            .send()
            .await
            .unwrap();
        let mut stream = resp.bytes_stream().eventsource();
        while let Some(ev) = stream.next().await {
            let ev = ev.unwrap();
            if ev.event == "progress" {
                return ev.data;
            }
        }
        panic!("SSE closed without progress");
    })
    .await
    .expect("Progress within 5s");

    // Spot-check: in_flight has the one claim; sealed=true.
    assert!(
        payload.contains("\"sealed\":true"),
        "progress payload missing sealed flag: {payload}"
    );
    assert!(
        payload.contains("\"in_flight\""),
        "progress payload missing in_flight: {payload}"
    );
    // Should mention the drv_name "x" inside in_flight.
    assert!(
        payload.contains("\"drv_name\":\"x\""),
        "progress payload missing in-flight drv_name: {payload}"
    );

    // Cleanly complete.
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

// ─── Regression: fleet mode still drains across multiple jobs ────────
//
// The new field additions on JobEvent + IngestDrvRequest must not have
// broken the fleet end-to-end. Re-run a minimized version of the
// fleet drain test inline so a one-line check failure jumps out here
// rather than confusingly in the unrelated fleet.rs file.

// ─── Sanity: list endpoint returns empty when no failures ────────────

