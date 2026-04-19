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
    // FK build_logs_job_fk requires a real jobs row.
    nix_ci_core::durable::writeback::upsert_job(&pool, job_id, None)
        .await
        .unwrap();
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
    // FK build_logs_job_fk requires a real jobs row.
    nix_ci_core::durable::writeback::upsert_job(&pool, job_id, None)
        .await
        .unwrap();
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
    nix_ci_core::durable::cleanup::sweep(&pool, 7, 14, None, store.as_ref(), &metrics)
        .await
        .unwrap();
    let body = metrics.render();
    assert!(body.contains("nix_ci_build_logs_bytes_total"));
    assert!(body.contains("nix_ci_build_logs_rows_total"));
}

// ─── Byte-ceiling prune ───────────────────────────────────────────

/// Helper: insert a build_logs row with a blob of the given size and a
/// specific stored_at so ordering is deterministic. The `content` field
/// in the gz column isn't required to be real gzip — `prune_to_byte_ceiling`
/// only measures `octet_length(log_gz)`.
async fn insert_log_row_with_blob(
    pool: &PgPool,
    job_id: nix_ci_core::types::JobId,
    claim_id: ClaimId,
    drv_hash: &DrvHash,
    attempt: i32,
    stored_at: chrono::DateTime<Utc>,
    blob_bytes: usize,
) {
    let blob: Vec<u8> = vec![b'x'; blob_bytes];
    sqlx::query(
        r#"
        INSERT INTO build_logs (
            claim_id, job_id, drv_hash, attempt, success, exit_code,
            started_at, ended_at, original_size, truncated, log_gz, stored_at
        )
        VALUES ($1, $2, $3, $4, FALSE, 1, now(), now(), $5, FALSE, $6, $7)
        "#,
    )
    .bind(claim_id.0)
    .bind(job_id.0)
    .bind(drv_hash.as_str())
    .bind(attempt)
    .bind(blob_bytes as i32)
    .bind(blob)
    .bind(stored_at)
    .execute(pool)
    .await
    .unwrap();
}

/// Table under cap: prune_to_byte_ceiling is a no-op and returns 0.
/// Guards against a bug where the function always runs the expensive
/// DELETE even on healthy systems.
#[sqlx::test]
async fn prune_to_byte_ceiling_under_cap_is_noop(pool: PgPool) {
    let h = spawn_server(pool.clone()).await;
    let store = PgLogStore::new(pool.clone());
    let job = nix_ci_core::client::CoordinatorClient::new(&h.base_url)
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let drv_hash = DrvHash::new("test".to_string());
    insert_log_row_with_blob(
        &pool,
        job.id,
        ClaimId::new(),
        &drv_hash,
        1,
        Utc::now(),
        1024,
    )
    .await;

    // Cap comfortably larger than the single 1 KiB row.
    let pruned = store.prune_to_byte_ceiling(100 * 1024).await.unwrap();
    assert_eq!(pruned, 0, "cap not exceeded, nothing must be pruned");
    // Row still there.
    let rows: (i64,) = sqlx::query_as("SELECT count(*) FROM build_logs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows.0, 1);
}

/// Table over cap: oldest rows pruned until under. Prune is measured
/// on `octet_length(log_gz)`, not `pg_total_relation_size`, so the cap
/// can be asserted tightly without waiting for autovacuum.
#[sqlx::test]
async fn prune_to_byte_ceiling_over_cap_drops_oldest(pool: PgPool) {
    let h = spawn_server(pool.clone()).await;
    let store = PgLogStore::new(pool.clone());
    let job = nix_ci_core::client::CoordinatorClient::new(&h.base_url)
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv_hash = DrvHash::new("test".to_string());

    // Three rows of 2 KiB each, stored_at spaced 1h apart. Total 6 KiB.
    let base = Utc::now() - chrono::Duration::hours(3);
    let c1 = ClaimId::new();
    let c2 = ClaimId::new();
    let c3 = ClaimId::new();
    for (cid, offset) in [(c1, 0), (c2, 1), (c3, 2)] {
        insert_log_row_with_blob(
            &pool,
            job.id,
            cid,
            &drv_hash,
            1,
            base + chrono::Duration::hours(offset),
            2048,
        )
        .await;
    }

    // Cap at 3 KiB — only the newest row's 2 KiB fits; the other two
    // must be pruned.
    let pruned = store.prune_to_byte_ceiling(3 * 1024).await.unwrap();
    assert_eq!(pruned, 2, "two oldest rows must be pruned");

    // Surviving row is the newest (c3).
    let surviving: Vec<(sqlx::types::Uuid,)> =
        sqlx::query_as("SELECT claim_id FROM build_logs ORDER BY stored_at DESC")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(surviving.len(), 1);
    assert_eq!(surviving[0].0, c3.0, "newest row must survive");

    // Sanity: post-prune byte sum is under the cap.
    let sum: Option<(Option<i64>,)> =
        sqlx::query_as("SELECT COALESCE(sum(octet_length(log_gz)), 0) FROM build_logs")
            .fetch_optional(&pool)
            .await
            .unwrap();
    let total: i64 = sum.and_then(|(v,)| v).unwrap_or(0);
    assert!(
        total <= 3 * 1024,
        "post-prune sum {total} must be <= cap 3072"
    );
}

/// End-to-end through cleanup::sweep with max_build_logs_bytes set:
/// the cap kicks in after time-based retention runs, the counter
/// metric ticks for pruned rows, and the surviving set is the
/// newest-first ordering. Guards against a future refactor that
/// forgets to wire max_build_logs_bytes into the sweep call.
#[sqlx::test]
async fn cleanup_sweep_enforces_max_build_logs_bytes(pool: PgPool) {
    let h = spawn_server(pool.clone()).await;
    let store: Arc<dyn LogStore> = Arc::new(PgLogStore::new(pool.clone()));
    let metrics = nix_ci_core::observability::metrics::Metrics::new();

    let job = nix_ci_core::client::CoordinatorClient::new(&h.base_url)
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv_hash = DrvHash::new("test".to_string());

    // Five rows of 1 KiB, within the time retention window.
    let base = Utc::now() - chrono::Duration::hours(1);
    let mut ids = Vec::new();
    for i in 0..5 {
        let cid = ClaimId::new();
        ids.push(cid);
        insert_log_row_with_blob(
            &pool,
            job.id,
            cid,
            &drv_hash,
            1,
            base + chrono::Duration::minutes(i as i64),
            1024,
        )
        .await;
    }

    // Cap at 2 KiB — 3 rows must be pruned.
    nix_ci_core::durable::cleanup::sweep(
        &pool,
        7,
        14,
        Some(2 * 1024),
        store.as_ref(),
        &metrics,
    )
    .await
    .unwrap();

    // Exactly 2 newest rows remain.
    let rows: (i64,) = sqlx::query_as("SELECT count(*) FROM build_logs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows.0, 2, "cap should prune 3 of 5 rows");

    // Counter exposes how many we pruned — operators alert on this
    // as "some job emitted fatter logs than expected."
    let rendered = metrics.render();
    assert!(
        rendered.contains("nix_ci_build_logs_byte_ceiling_prunes_total 3"),
        "byte-ceiling prune counter must reflect the 3 pruned rows; got:\n{rendered}"
    );

    // Sweep with NO cap must not touch surviving rows (positive case:
    // None disables the guard).
    nix_ci_core::durable::cleanup::sweep(&pool, 7, 14, None, store.as_ref(), &metrics)
        .await
        .unwrap();
    let rows_after_noop: (i64,) = sqlx::query_as("SELECT count(*) FROM build_logs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows_after_noop.0, 2, "no cap = no pruning");
}

// ─── Per-job log-bytes accumulator + WARN threshold ──────────────

/// Drive one log upload and assert the submission's
/// `log_bytes_accumulated` grew by the upload size. This is the
/// in-memory counter the histogram reads at terminal time and the
/// per-job WARN fires against.
#[sqlx::test]
async fn upload_log_grows_per_submission_byte_accumulator(pool: PgPool) {
    let h = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("logs-accum".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let drv = drv_path("accum", "d");
    let drv_hash = DrvHash::new("accum-d.drv".to_string());
    client
        .ingest_drv(job.id, &ingest(&drv, "d", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");

    // Baseline accumulator is 0.
    let sub = h.dispatcher.submissions.get(job.id).expect("live sub");
    assert_eq!(
        sub.log_bytes_accumulated
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );

    let payload = "x".repeat(8 * 1024); // 8 KiB raw
    let gz_bytes = gz(&payload);
    let uploaded = gz_bytes.len() as u64;
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
                started_at: Utc::now() - chrono::Duration::seconds(1),
                ended_at: Utc::now(),
            },
            gz_bytes,
        )
        .await
        .unwrap();

    let accumulated = sub
        .log_bytes_accumulated
        .load(std::sync::atomic::Ordering::Acquire);
    assert_eq!(
        accumulated, uploaded,
        "accumulator must match the gzipped upload body size"
    );
}

/// Crossing `build_log_bytes_per_job_warn` mid-flight must emit the
/// one-shot WARN metric tick. Subsequent uploads on the same
/// already-over-threshold job must NOT tick again (CAS guard).
#[sqlx::test]
async fn log_bytes_warn_fires_once_per_submission(pool: PgPool) {
    // Threshold must be smaller than the wire-bytes of the gzipped
    // upload. `"a".repeat(8 * 1024)` compresses to ~50 bytes (RLE);
    // set a 32-byte threshold so the first upload definitely crosses
    // regardless of the specific gzip output size.
    let h = spawn_server_with_cfg(pool.clone(), |cfg| {
        cfg.build_log_bytes_per_job_warn = Some(32);
    })
    .await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv_a = drv_path("warn_a", "a");
    let drv_b = drv_path("warn_b", "b");
    let dh_a = DrvHash::new("warn_a-a.drv".to_string());
    let dh_b = DrvHash::new("warn_b-b.drv".to_string());
    client
        .ingest_drv(job.id, &ingest(&drv_a, "a", &[], true))
        .await
        .unwrap();
    client
        .ingest_drv(job.id, &ingest(&drv_b, "b", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    // Upload 1: crosses the threshold, should tick the counter.
    let c1 = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim 1");
    let payload_1 = "a".repeat(8 * 1024);
    client
        .upload_log(
            job.id,
            c1.claim_id,
            BuildLogUploadMeta {
                drv_hash: &dh_a,
                attempt: c1.attempt,
                original_size: payload_1.len() as u32,
                truncated: false,
                success: false,
                exit_code: Some(1),
                started_at: Utc::now() - chrono::Duration::seconds(5),
                ended_at: Utc::now(),
            },
            gz(&payload_1),
        )
        .await
        .unwrap();
    // Complete the claim so its drv terminalizes (won't block further claim).
    client
        .complete(
            job.id,
            c1.claim_id,
            &nix_ci_core::types::CompleteRequest {
                success: false,
                duration_ms: 1,
                exit_code: Some(1),
                error_category: Some(nix_ci_core::types::ErrorCategory::BuildFailure),
                error_message: Some("a failed".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();

    let warn_count_after_first = common::scrape_metric(
        &h.base_url,
        "nix_ci_submission_log_bytes_warn_total",
        &[],
    )
    .await
    .unwrap_or(0.0);
    assert_eq!(
        warn_count_after_first, 1.0,
        "first over-threshold upload must tick the warn counter exactly once"
    );

    // Upload 2 on the SAME job: already over threshold, CAS guard
    // must keep the counter at 1.
    let c2 = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim 2");
    let payload_2 = "b".repeat(8 * 1024);
    client
        .upload_log(
            job.id,
            c2.claim_id,
            BuildLogUploadMeta {
                drv_hash: &dh_b,
                attempt: c2.attempt,
                original_size: payload_2.len() as u32,
                truncated: false,
                success: false,
                exit_code: Some(1),
                started_at: Utc::now() - chrono::Duration::seconds(5),
                ended_at: Utc::now(),
            },
            gz(&payload_2),
        )
        .await
        .unwrap();

    let warn_count_after_second = common::scrape_metric_expect(
        &h.base_url,
        "nix_ci_submission_log_bytes_warn_total",
        &[],
    )
    .await;
    assert_eq!(
        warn_count_after_second, 1.0,
        "a second upload on the same over-threshold job must not re-fire the warn"
    );
}

/// At terminal time, the per-submission accumulator is folded into the
/// `build_log_bytes_per_job` histogram. End-to-end through the
/// catastrophic-fail path: one upload, fail the drv, read the histogram.
#[sqlx::test]
async fn build_log_bytes_per_job_histogram_observed_at_terminal(pool: PgPool) {
    let h = spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&h.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();
    let drv = drv_path("hist", "d");
    let drv_hash = DrvHash::new("hist-d.drv".to_string());
    client
        .ingest_drv(job.id, &ingest(&drv, "d", &[], true))
        .await
        .unwrap();
    client.seal(job.id).await.unwrap();

    let c = client
        .claim(job.id, "x86_64-linux", &[], 3)
        .await
        .unwrap()
        .expect("claim");
    let payload = "h".repeat(16 * 1024); // 16 KiB raw
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
            gz(&payload),
        )
        .await
        .unwrap();

    // Now fail the drv — terminal path fires, histogram gets one obs.
    client
        .complete(
            job.id,
            c.claim_id,
            &nix_ci_core::types::CompleteRequest {
                success: false,
                duration_ms: 1,
                exit_code: Some(1),
                error_category: Some(nix_ci_core::types::ErrorCategory::BuildFailure),
                error_message: Some("boom".into()),
                log_tail: None,
            },
        )
        .await
        .unwrap();
    // Wait up to 1s for terminal.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let s = client.status(job.id).await.unwrap();
        if s.status.is_terminal() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("job never terminalized");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Histogram _count must be >= 1 (one observation recorded).
    let count = common::scrape_metric(
        &h.base_url,
        "nix_ci_build_log_bytes_per_job_count",
        &[],
    )
    .await
    .unwrap_or(0.0);
    assert!(
        count >= 1.0,
        "build_log_bytes_per_job must have at least one observation after terminal, got {count}"
    );
}
