//! Tests for the new observability surface: build log archive, claims
//! listing, per-endpoint histograms, per-job size measurement.

mod common;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use common::{drv_path, spawn_server, spawn_server_with_cfg};
use nix_ci_core::client::{BuildLogUploadMeta, CoordinatorClient};
use nix_ci_core::durable::logs::{LogStore, PgLogStore};
use nix_ci_core::types::{
    ClaimId, CreateJobRequest, DrvHash, IngestBatchRequest, IngestDrvRequest,
};
use sqlx::PgPool;

fn ingest(drv: &str, name: &str, deps: &[&str], is_root: bool) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv.to_string(),
        drv_name: name.to_string(),
        system: "x86_64-linux".into(),
        required_features: Vec::new(),
        input_drvs: deps.iter().map(|s| s.to_string()).collect(),
        is_root,
        attr: None,
    }
}

fn gz(s: &str) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(s.as_bytes()).unwrap();
    e.finish().unwrap()
}

#[sqlx::test]
async fn upload_then_fetch_log_roundtrips(pool: PgPool) {
    let h = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&h.base_url);

    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("logs-roundtrip".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let drv = drv_path("aaa", "demo");
    let drv_hash = DrvHash::new("aaa-demo.drv".to_string());
    client
        .ingest_drv(job.id, &ingest(&drv, "demo", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Need a real ClaimId — drive a single claim/complete so the claim
    // existed (we then archive against that claim_id).
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");

    let payload = "===STDERR===\nerror: builder failed with exit code 1\n[final line]";
    client
        .upload_log(
            job.id,
            c.claim_id,
            BuildLogUploadMeta {
                drv_hash: &drv_hash,
                attempt: c.attempt,
                original_size: payload.len() as u32,
                truncated: false,
                success: false,
                exit_code: Some(1),
                started_at: Utc::now() - chrono::Duration::seconds(5),
                ended_at: Utc::now(),
            },
            gz(payload),
        )
        .await
        .unwrap();

    // Fetch round-trip — coordinator decompresses on the way out.
    let body = client.fetch_log(job.id, c.claim_id).await.unwrap();
    assert_eq!(
        body, payload,
        "fetch_log must return the exact bytes uploaded"
    );

    // Listing endpoint sees the attempt.
    let listing = client.list_drv_logs(job.id, &drv_hash).await.unwrap();
    assert_eq!(listing.attempts.len(), 1);
    let a = &listing.attempts[0];
    assert_eq!(a.claim_id, c.claim_id);
    assert_eq!(a.original_size, payload.len() as u32);
    // Gzip headers add ~20 bytes — for tiny payloads the *compressed*
    // size can exceed the raw size. Just assert we have a non-zero
    // blob and we haven't ballooned absurdly.
    assert!(a.stored_size > 0 && a.stored_size < (payload.len() as u32 + 64));
    assert!(!a.truncated);
    assert!(!a.success);
    assert_eq!(a.exit_code, Some(1));
}

#[sqlx::test]
async fn fetch_log_404_when_missing(pool: PgPool) {
    let h = spawn_server(pool).await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let bogus = ClaimId::new();
    let err = client.fetch_log(job.id, bogus).await.unwrap_err();
    assert!(
        matches!(err, nix_ci_core::Error::NotFound(_)),
        "missing log must surface as NotFound, got {err:?}"
    );
}

#[sqlx::test]
async fn upload_overwrites_on_same_claim_id(pool: PgPool) {
    // Idempotent retry: a worker that retried the upload after a
    // network blip should NOT 23505. Second upload wins (newer bytes).
    let h = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("bbb", "demo");
    let drv_hash = DrvHash::new("bbb-demo.drv".to_string());
    client
        .ingest_drv(job.id, &ingest(&drv, "demo", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .unwrap();

    let make_meta = |size: usize| BuildLogUploadMeta {
        drv_hash: &drv_hash,
        attempt: c.attempt,
        original_size: size as u32,
        truncated: false,
        success: false,
        exit_code: Some(1),
        started_at: Utc::now(),
        ended_at: Utc::now(),
    };
    client
        .upload_log(job.id, c.claim_id, make_meta(5), gz("first"))
        .await
        .unwrap();
    client
        .upload_log(job.id, c.claim_id, make_meta(6), gz("second"))
        .await
        .unwrap();

    let body = client.fetch_log(job.id, c.claim_id).await.unwrap();
    assert_eq!(body, "second", "second upload must win on same claim_id");
    let listing = client.list_drv_logs(job.id, &drv_hash).await.unwrap();
    assert_eq!(
        listing.attempts.len(),
        1,
        "upsert must keep one row per claim_id"
    );
}

#[sqlx::test]
async fn upload_rejects_oversized_body(pool: PgPool) {
    let h = spawn_server(pool).await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("ccc", "demo");
    let drv_hash = DrvHash::new("ccc-demo.drv".to_string());
    client
        .ingest_drv(job.id, &ingest(&drv, "demo", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();
    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .unwrap();

    // 16 MiB of zero bytes — well past the 8 MiB cap. Server should
    // 400.
    let huge: Vec<u8> = vec![0u8; 16 * 1024 * 1024];
    let err = client
        .upload_log(
            job.id,
            c.claim_id,
            BuildLogUploadMeta {
                drv_hash: &drv_hash,
                attempt: c.attempt,
                original_size: huge.len() as u32,
                truncated: false,
                success: false,
                exit_code: Some(1),
                started_at: Utc::now(),
                ended_at: Utc::now(),
            },
            huge,
        )
        .await
        .unwrap_err();
    let s = format!("{err:?}");
    assert!(
        s.contains("400") || s.contains("BadRequest") || s.contains("exceed"),
        "oversized upload should 400, got {err:?}"
    );
}

#[sqlx::test]
async fn list_drv_logs_orders_attempts_newest_first(pool: PgPool) {
    // Two attempts on the same drv, different claim_ids — listing
    // must surface them ordered by attempt DESC (newest first).
    let store = PgLogStore::new(pool.clone());
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let job_id = nix_ci_core::types::JobId::new();
    let drv_hash = DrvHash::new("zzz-multi.drv".to_string());
    let cid_old = ClaimId::new();
    let cid_new = ClaimId::new();
    use nix_ci_core::durable::logs::LogPutRequest;
    store
        .put(
            LogPutRequest {
                job_id,
                claim_id: cid_old,
                drv_hash: drv_hash.clone(),
                attempt: 1,
                success: false,
                exit_code: Some(1),
                started_at: Utc::now() - chrono::Duration::seconds(10),
                ended_at: Utc::now() - chrono::Duration::seconds(5),
                original_size: 4,
                truncated: false,
            },
            gz("old1"),
        )
        .await
        .unwrap();
    store
        .put(
            LogPutRequest {
                job_id,
                claim_id: cid_new,
                drv_hash: drv_hash.clone(),
                attempt: 2,
                success: false,
                exit_code: Some(1),
                started_at: Utc::now(),
                ended_at: Utc::now(),
                original_size: 4,
                truncated: false,
            },
            gz("new2"),
        )
        .await
        .unwrap();

    let attempts = store.list_attempts(job_id, &drv_hash).await.unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].attempt, 2, "newest attempt first");
    assert_eq!(attempts[1].attempt, 1);
}

#[sqlx::test]
async fn prune_older_than_drops_old_rows(pool: PgPool) {
    // Cleanup loop's contract: anything older than the cutoff is gone;
    // anything newer survives.
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let store = PgLogStore::new(pool.clone());
    use nix_ci_core::durable::logs::LogPutRequest;
    let drv_hash = DrvHash::new("ddd-prune.drv".to_string());
    let job_id = nix_ci_core::types::JobId::new();
    let old_cid = ClaimId::new();
    let new_cid = ClaimId::new();
    store
        .put(
            LogPutRequest {
                job_id,
                claim_id: old_cid,
                drv_hash: drv_hash.clone(),
                attempt: 1,
                success: false,
                exit_code: None,
                started_at: Utc::now(),
                ended_at: Utc::now(),
                original_size: 4,
                truncated: false,
            },
            gz("old"),
        )
        .await
        .unwrap();
    // Backdate one row so the cutoff catches it.
    sqlx::query("UPDATE build_logs SET stored_at = now() - interval '30 days' WHERE claim_id = $1")
        .bind(old_cid.0)
        .execute(&pool)
        .await
        .unwrap();
    store
        .put(
            LogPutRequest {
                job_id,
                claim_id: new_cid,
                drv_hash: drv_hash.clone(),
                attempt: 2,
                success: false,
                exit_code: None,
                started_at: Utc::now(),
                ended_at: Utc::now(),
                original_size: 4,
                truncated: false,
            },
            gz("new"),
        )
        .await
        .unwrap();

    // Cutoff: 14 days back — drops the 30-day-old row, keeps the fresh one.
    let cutoff = Utc::now() - chrono::Duration::days(14);
    let removed = store.prune_older_than(cutoff).await.unwrap();
    assert_eq!(removed, 1, "exactly the old row should be pruned");
    assert!(store.fetch_gz(job_id, old_cid).await.unwrap().is_none());
    assert!(store.fetch_gz(job_id, new_cid).await.unwrap().is_some());
}

#[sqlx::test]
async fn list_claims_returns_active_claims_with_worker_id(pool: PgPool) {
    // Drive a claim with an explicit worker_id and verify GET /claims
    // surfaces it. Don't complete — we want to see it as in-flight.
    let h = spawn_server(pool).await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("claims-listing".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("xxx", "claimsworker");
    client
        .ingest_drv(job.id, &ingest(&drv, "claimsworker", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let claim = client
        .claim_as_worker(job.id, "x86_64-linux", &[], 3, Some("host42-pid12345-aabb"))
        .await
        .unwrap()
        .expect("claim");

    let resp = client.list_claims().await.unwrap();
    assert_eq!(resp.claims.len(), 1, "exactly one in-flight claim");
    let c = &resp.claims[0];
    assert_eq!(c.claim_id, claim.claim_id);
    assert_eq!(c.job_id, job.id);
    assert_eq!(c.worker_id.as_deref(), Some("host42-pid12345-aabb"));
    assert!(
        c.elapsed_ms < 60_000,
        "elapsed must be a small number of ms"
    );
    // deadline is in the future (claim_deadline_secs default 2h).
    assert!(c.deadline > Utc::now());
}

#[sqlx::test]
async fn list_claims_sorts_longest_running_first(pool: PgPool) {
    // Two claims, second issued strictly later → first must rank first
    // in the response (longest-running == first issued).
    let h = spawn_server(pool).await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv1 = drv_path("yyy1", "first");
    let drv2 = drv_path("yyy2", "second");
    client
        .ingest_drv(job.id, &ingest(&drv1, "first", &[], true))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&drv2, "second", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let _claim_a = client
        .claim_as_worker(job.id, "x86_64-linux", &[], 3, Some("worker-a"))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    let _claim_b = client
        .claim_as_worker(job.id, "x86_64-linux", &[], 3, Some("worker-b"))
        .await
        .unwrap()
        .unwrap();

    let resp = client.list_claims().await.unwrap();
    assert_eq!(resp.claims.len(), 2);
    assert!(
        resp.claims[0].elapsed_ms >= resp.claims[1].elapsed_ms,
        "expected longest-first ordering, got {:?}",
        resp.claims
    );
    assert_eq!(resp.claims[0].worker_id.as_deref(), Some("worker-a"));
}

#[sqlx::test]
async fn http_metrics_record_per_endpoint_latency(pool: PgPool) {
    // Drive a couple of distinct endpoints, then scrape /metrics and
    // confirm we see distinct route labels for each. We don't assert
    // on exact bucket counts — just that the histogram emitted
    // observations for the routes we hit and NOT for excluded long-poll
    // routes (modulo a deliberate /claim hit which should be excluded).
    let h = spawn_server(pool).await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("met", "metricsdrv");
    client
        .ingest_drv(job.id, &ingest(&drv, "metricsdrv", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Long-poll: should NOT appear in the histogram.
    let _ = client.claim(job.id, "x86_64-linux", &[], 1).await;

    let metrics_url = format!("{}/metrics", h.base_url);
    let body = reqwest::get(&metrics_url)
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // The histograms are emitted per route under
    // `nix_ci_http_request_duration_seconds_bucket{...}`. Don't pin
    // the exact label format, just look for the route strings.
    assert!(
        body.contains("nix_ci_http_request_duration_seconds"),
        "histogram series missing"
    );
    assert!(
        body.contains("/jobs/{id}/seal") || body.contains(r#""/jobs/{id}/seal""#),
        "expected POST /jobs/{{id}}/seal route in histogram, body=\n{body}"
    );
    // Long-poll route excluded — we deliberately did NOT add /claim
    // to the histogram. (Don't fail the assertion if the literal text
    // appears in some other unrelated metric line — be conservative.)
    let claim_route_lines: Vec<&str> = body
        .lines()
        .filter(|l| l.starts_with("nix_ci_http_request_duration_seconds_bucket"))
        .filter(|l| l.contains("route=\"/jobs/{id}/claim\""))
        .collect();
    assert!(
        claim_route_lines.is_empty(),
        "long-poll /jobs/{{id}}/claim must NOT be observed in the latency histogram, found: {claim_route_lines:?}"
    );
}

#[sqlx::test]
async fn drvs_per_job_histogram_records_at_terminal(pool: PgPool) {
    // Drive a job to terminal via cancel and confirm the per-job size
    // histogram emitted at least one observation. Three drvs ingested
    // → first bucket above 1 should pick up.
    let h = spawn_server(pool).await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("size-hist".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let batch = IngestBatchRequest {
        drvs: vec![
            ingest(&drv_path("s1", "a"), "a", &[], true),
            ingest(&drv_path("s2", "b"), "b", &[], true),
            ingest(&drv_path("s3", "c"), "c", &[], true),
        ],
    eval_errors: Vec::new(),
        };
    client.ingest_batch(job.id, &batch).await.unwrap();
    client.cancel(job.id).await.unwrap();

    let metrics_url = format!("{}/metrics", h.base_url);
    let body = reqwest::get(&metrics_url)
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("nix_ci_drvs_per_job_count "),
        "drvs_per_job histogram missing: \n{body}"
    );
    // The _count line should be ≥ 1 — a job actually terminated.
    let count_line = body
        .lines()
        .find(|l| l.starts_with("nix_ci_drvs_per_job_count "))
        .unwrap();
    let n: u64 = count_line
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap();
    assert!(n >= 1, "expected ≥1 observation, got {n}");
}

#[sqlx::test]
async fn submission_warn_fires_once_when_threshold_crossed(pool: PgPool) {
    // Lower the threshold to 5 so the test is cheap; ingest 6 drvs in
    // two batches and verify the metric counter increments by exactly
    // 1 (CAS makes the warn one-shot per submission).
    let h = spawn_server_with_cfg(pool, |cfg| {
        cfg.submission_warn_threshold = 5;
    })
    .await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    // First batch of 3 — under threshold.
    let b1 = IngestBatchRequest {
        drvs: (0..3)
            .map(|i| ingest(&drv_path(&format!("w{i}"), "x"), "x", &[], true))
            .collect(),
        eval_errors: Vec::new(),
    };
    client.ingest_batch(job.id, &b1).await.unwrap();
    // Second batch of 3 — pushes total to 6 (crosses 5).
    let b2 = IngestBatchRequest {
        drvs: (3..6)
            .map(|i| ingest(&drv_path(&format!("w{i}"), "x"), "x", &[], true))
            .collect(),
        eval_errors: Vec::new(),
    };
    client.ingest_batch(job.id, &b2).await.unwrap();
    // Third batch of 3 — already warned; counter must NOT bump again.
    let b3 = IngestBatchRequest {
        drvs: (6..9)
            .map(|i| ingest(&drv_path(&format!("w{i}"), "x"), "x", &[], true))
            .collect(),
        eval_errors: Vec::new(),
    };
    client.ingest_batch(job.id, &b3).await.unwrap();

    let metrics_url = format!("{}/metrics", h.base_url);
    let body = reqwest::get(&metrics_url)
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let count_line = body
        .lines()
        .find(|l| l.starts_with("nix_ci_submission_warn_total "))
        .expect("submission_warn_total missing");
    let n: u64 = count_line
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(n, 1, "warn must fire exactly once per submission, got {n}");
}

#[sqlx::test]
async fn cleanup_sweep_emits_log_metrics(pool: PgPool) {
    // After a sweep, build_logs_bytes_total / rows_total gauges should
    // be populated (Postgres returns sane numbers even on empty table).
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let store: Arc<dyn LogStore> = Arc::new(PgLogStore::new(pool.clone()));
    let metrics = nix_ci_core::observability::metrics::Metrics::new();
    nix_ci_core::durable::cleanup::sweep(&pool, 7, 14, store.as_ref(), &metrics)
        .await
        .unwrap();
    let body = metrics.render();
    assert!(body.contains("nix_ci_build_logs_bytes_total"));
    assert!(body.contains("nix_ci_build_logs_rows_total"));
}
