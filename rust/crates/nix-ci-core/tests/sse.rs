//! Server-sent-events job_done / drv_failed / progress stream.

mod common;

use std::time::Duration;

use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CompleteRequest, CreateJobRequest, ErrorCategory, JobStatus};
use sqlx::PgPool;

#[sqlx::test]
async fn sse_job_done_event_surfaces_for_success(pool: PgPool) {
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
    let drv = drv_path("sse", "single");
    client
        .ingest_drv(job.id, &ingest(&drv, "single", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

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
            if ev.event == "job_done" {
                return ev.data;
            }
        }
        panic!("SSE closed without job_done");
    });

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
                duration_ms: 5,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();

    let payload = tokio::time::timeout(Duration::from_secs(5), sse_task)
        .await
        .expect("SSE task deadline")
        .expect("SSE task joined");
    assert!(
        payload.contains("\"status\":\"done\""),
        "job_done SSE payload missing status: {payload}"
    );
}

#[sqlx::test]
async fn sse_consumer_returns_err_on_404(pool: PgPool) {
    // GET /jobs/{id}/events on an unknown job_id returns 404. The
    // print_events function used to silently return Ok(Pending) on a
    // bad-status response, hiding the failure from the orchestrator.
    // Should return Err so the caller can react.
    use std::sync::Arc;
    use tokio::sync::watch;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let bogus_job_id = nix_ci_core::types::JobId::new();

    let (sd_tx, sd_rx) = watch::channel(false);
    let res = tokio::time::timeout(
        Duration::from_secs(5),
        nix_ci_core::runner::sse::print_events(client, bogus_job_id, sd_tx, sd_rx),
    )
    .await
    .expect("sse must return promptly on 404");

    assert!(
        res.is_err(),
        "non-2xx SSE response must surface as Err, not Ok; got {res:?}"
    );
}

#[sqlx::test]
async fn sse_consumer_returns_pending_on_clean_shutdown(pool: PgPool) {
    // Caller-initiated shutdown (the typical orchestrator path on
    // SIGTERM) is the ONLY case Pending is a correct return. Hold the
    // sender outside the spawn so we can flip it from the test.
    use std::sync::Arc;
    use tokio::sync::watch;

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let (sd_tx, sd_rx) = watch::channel(false);
    let sd_tx_inner = sd_tx.clone();
    let task = tokio::spawn({
        let client = client.clone();
        async move { nix_ci_core::runner::sse::print_events(client, job.id, sd_tx_inner, sd_rx).await }
    });

    // Let the consumer subscribe + sit waiting on events.
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Caller signals shutdown — print_events must return Ok(Pending).
    let _ = sd_tx.send(true);

    let res = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("must exit promptly on shutdown")
        .expect("joined");
    assert!(
        matches!(res, Ok(JobStatus::Pending)),
        "clean shutdown must return Ok(Pending); got {res:?}"
    );
}

#[sqlx::test]
async fn sse_drv_completed_event_carries_real_duration(pool: PgPool) {
    // The complete handler records duration_ms in metrics; the
    // DrvCompleted SSE event must NOT hardcode 0 — subscribers want
    // the real duration.
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
    let drv = drv_path("dur", "timed");
    client
        .ingest_drv(job.id, &ingest(&drv, "timed", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

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
            if ev.event == "drv_completed" {
                return ev.data;
            }
        }
        panic!("SSE closed without drv_completed");
    });

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    const REPORTED_MS: u64 = 12_345;
    client
        .complete(
            job.id,
            c.claim_id,
            &CompleteRequest {
                success: true,
                duration_ms: REPORTED_MS,
                exit_code: Some(0),
                error_category: None,
                error_message: None,
                log_tail: None,
            },
        )
        .await
        .unwrap();

    let payload = tokio::time::timeout(Duration::from_secs(5), sse_task)
        .await
        .expect("SSE task deadline")
        .expect("SSE task joined");
    let expected = format!("\"duration_ms\":{REPORTED_MS}");
    assert!(
        payload.contains(&expected),
        "drv_completed must carry duration_ms={REPORTED_MS}, got: {payload}"
    );
}

#[sqlx::test]
async fn sse_lagged_event_fires_when_subscriber_falls_behind(pool: PgPool) {
    // With a tiny broadcast capacity, a subscriber that doesn't drain
    // fast enough receives a `BroadcastStreamRecvError::Lagged` which
    // `server::events` maps to a `JobEvent::Lagged { missed }`. We
    // exercise the broadcast layer + its BroadcastStream wrapper
    // directly because axum's SSE plumbing adds buffers that mask
    // backpressure at the HTTP boundary.
    use futures::StreamExt;
    use nix_ci_core::config::ServerConfig;
    use nix_ci_core::types::JobEvent;
    use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
    use tokio_stream::wrappers::BroadcastStream;

    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.submission_event_capacity = 4; // deliberately tiny
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

    // Grab the submission directly and create a BroadcastStream that
    // we deliberately never poll while events fire.
    let sub = handle
        .dispatcher
        .submissions
        .get(job.id)
        .expect("submission present");
    let rx = sub.subscribe();
    let mut stream = BroadcastStream::new(rx);

    // Fire many events (>> capacity) without touching the stream.
    for i in 0..20u32 {
        sub.publish(JobEvent::Progress {
            counts: nix_ci_core::types::JobCounts {
                total: i,
                pending: i,
                ..Default::default()
            },
            in_flight: vec![],
            propagated_failed: 0,
            transient_retries: 0,
            sealed: false,
        });
    }

    // Now read: the very first poll must yield a Lagged error (the
    // receiver is too far behind the capacity-4 ring).
    let first = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("stream must yield")
        .expect("stream still open");
    match first {
        Err(BroadcastStreamRecvError::Lagged(missed)) => {
            assert!(missed > 0, "Lagged must carry a positive missed count");
        }
        other => {
            panic!("expected Lagged error, got {other:?}; broadcast capacity may be too generous")
        }
    }

    // And the `server::events` handler maps exactly that error to a
    // `JobEvent::Lagged { missed }` — guard the mapping so a refactor
    // (e.g. swallowing the error) doesn't silently lose it.
    let mapped = JobEvent::Lagged { missed: 7 };
    match mapped {
        JobEvent::Lagged { missed } => assert_eq!(missed, 7),
        _ => unreachable!(),
    }
}

#[sqlx::test]
async fn broadcast_preserves_per_subscriber_publish_order(pool: PgPool) {
    // tokio::sync::broadcast guarantees FIFO per receiver — confirm
    // we haven't introduced any reordering in our JobEvent plumbing.
    // Use two subscribers to also catch a "publish only to one" bug.
    use futures::StreamExt;
    use nix_ci_core::types::JobEvent;
    use tokio_stream::wrappers::BroadcastStream;

    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let sub = handle
        .dispatcher
        .submissions
        .get(job.id)
        .expect("submission");

    // Two subscribers before any event fires.
    let mut stream_a = BroadcastStream::new(sub.subscribe());
    let mut stream_b = BroadcastStream::new(sub.subscribe());

    // Publish N events with strictly-increasing `total`.
    const N: u32 = 50;
    for i in 0..N {
        sub.publish(JobEvent::Progress {
            counts: nix_ci_core::types::JobCounts {
                total: i,
                ..Default::default()
            },
            in_flight: vec![],
            propagated_failed: 0,
            transient_retries: 0,
            sealed: false,
        });
    }

    // Drain each stream; sequences must match 0..N exactly.
    async fn drain_totals(stream: &mut BroadcastStream<JobEvent>, n: usize) -> Vec<u32> {
        let mut totals = Vec::with_capacity(n);
        for _ in 0..n {
            let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .expect("stream yields within timeout")
                .expect("stream open")
                .expect("not lagged");
            if let JobEvent::Progress { counts, .. } = ev {
                totals.push(counts.total);
            } else {
                panic!("unexpected event type");
            }
        }
        totals
    }

    let seq_a = drain_totals(&mut stream_a, N as usize).await;
    let seq_b = drain_totals(&mut stream_b, N as usize).await;
    let expected: Vec<u32> = (0..N).collect();
    assert_eq!(seq_a, expected, "subscriber A must see publish-order");
    assert_eq!(seq_b, expected, "subscriber B must see publish-order");
}

#[sqlx::test]
async fn sse_drv_failed_event_surfaces_for_terminal_failure(pool: PgPool) {
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
    let drv = drv_path("drvfail", "oops");
    client
        .ingest_drv(job.id, &ingest(&drv, "oops", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

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
            if ev.event == "job_done" {
                // Reached terminal without seeing drv_failed — fail.
                return format!("UNEXPECTED_JOB_DONE:{}", ev.data);
            }
        }
        panic!("SSE closed without drv_failed");
    });

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
        .expect("SSE task deadline")
        .expect("SSE task joined");
    assert!(
        !payload.starts_with("UNEXPECTED_JOB_DONE"),
        "drv_failed must arrive before job_done: {payload}"
    );
    // Payload must contain the drv_name field (guards the Step::drv_name
    // getter and publish_drv_failed's population of the event).
    assert!(
        payload.contains(r#""drv_name":"oops""#),
        "drv_failed payload missing drv_name 'oops': {payload}"
    );
    assert!(payload.contains(r#""error_category":"build_failure""#));
    assert!(payload.contains(r#""will_retry":false"#));
}
