//! `/admin/refute` integration tests.
//!
//! The operator story: a sick worker mis-labels a drv as
//! `BuildFailure`, which persists to `failed_outputs` for the full
//! TTL (default 1h). Every subsequent job referencing the same
//! output path short-circuits — "drv is known bad" — and inherits
//! the failure. Refuting clears the false positive so the next
//! legitimate run can rebuild.
//!
//! Happy path:
//!   1. Worker reports BuildFailure → writeback inserts into cache.
//!   2. Operator identifies the false positive.
//!   3. `POST /admin/refute { drv_hash: ..., output_paths: [...] }`
//!      → cache entry gone.
//!   4. Next job's ingest sees a cache miss and builds normally.

mod common;

use common::spawn_server;
use nix_ci_core::durable::writeback;
use nix_ci_core::types::DrvHash;
use sqlx::PgPool;

async fn post_json(
    url: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let v: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    (status, v)
}

#[sqlx::test]
async fn refute_by_output_path_deletes_entry(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;

    // Seed: insert a failed output directly via the durable layer.
    let drv_hash = DrvHash::new("drvhash-1");
    let output = "/nix/store/abc-hello".to_string();
    writeback::insert_failed_outputs(
        &pool,
        &drv_hash,
        std::slice::from_ref(&output),
        3600,
        None,
    )
    .await
    .unwrap();

    // Sanity: it's present.
    let hits = writeback::failed_output_hits(&pool, &[output.as_str()]).await;
    assert!(hits.contains(output.as_str()), "cache must contain the seeded output");

    // Refute by output_path.
    let url = format!("{}/admin/refute", handle.base_url);
    let (status, body) = post_json(
        &url,
        serde_json::json!({
            "output_paths": [output.as_str()],
        }),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "refute failed: {body}");
    assert_eq!(body["rows_affected"].as_u64(), Some(1));

    // Verify: entry is gone.
    let hits2 = writeback::failed_output_hits(&pool, &[output.as_str()]).await;
    assert!(!hits2.contains(output.as_str()), "cache entry must be gone after refute");
}

#[sqlx::test]
async fn refute_by_drv_hash_removes_all_output_paths(pool: PgPool) {
    let handle = spawn_server(pool.clone()).await;

    // One drv with two outputs. Insert both with the same drv_hash.
    let drv_hash = DrvHash::new("drvhash-multi");
    let outs = vec![
        "/nix/store/p1-out".to_string(),
        "/nix/store/p2-dev".to_string(),
    ];
    writeback::insert_failed_outputs(&pool, &drv_hash, &outs, 3600, None)
        .await
        .unwrap();

    let url = format!("{}/admin/refute", handle.base_url);
    let (status, body) = post_json(
        &url,
        serde_json::json!({
            "drv_hash": "drvhash-multi",
        }),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["rows_affected"].as_u64(), Some(2));

    let refs: Vec<&str> = outs.iter().map(|s| s.as_str()).collect();
    let hits = writeback::failed_output_hits(&pool, &refs).await;
    assert!(hits.is_empty(), "all outputs for drv_hash must be refuted");
}

#[sqlx::test]
async fn refute_with_no_target_is_bad_request(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let url = format!("{}/admin/refute", handle.base_url);
    let (status, body) = post_json(&url, serde_json::json!({})).await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("at least one of"),
        "error message must point operator at missing fields: {body}"
    );
}

#[sqlx::test]
async fn refute_for_unknown_entry_is_idempotent(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let url = format!("{}/admin/refute", handle.base_url);
    // Refute a drv_hash that was never cached: 200 OK with 0 rows.
    // This is the shape operators get when they retry a refute or
    // when the TTL already expired between observation and action.
    let (status, body) = post_json(
        &url,
        serde_json::json!({
            "drv_hash": "never-cached",
            "output_paths": ["/nix/store/never-failed"],
        }),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["rows_affected"].as_u64(), Some(0));
}

#[sqlx::test]
async fn refute_by_worker_id_bulk_deletes_that_workers_entries(pool: PgPool) {
    // Operator story: host `sick-host-a` returned a burst of false
    // BuildFailures that poisoned the cache. Rather than refute each
    // drv_hash / output_path individually, operator refutes by
    // worker_id → every row that worker owns is cleared in one call.
    // Rows from other workers MUST survive.
    let handle = spawn_server(pool.clone()).await;

    let drv_a = DrvHash::new("a-drv");
    let drv_b = DrvHash::new("b-drv");
    let drv_c = DrvHash::new("c-drv");

    // Two rows from the suspected-sick worker, one from a healthy one,
    // one with no worker attribution (legacy / pre-migration shape).
    writeback::insert_failed_outputs(
        &pool,
        &drv_a,
        &["/nix/store/a-out".to_string()],
        3600,
        Some("sick-host-a"),
    )
    .await
    .unwrap();
    writeback::insert_failed_outputs(
        &pool,
        &drv_b,
        &["/nix/store/b-out".to_string()],
        3600,
        Some("sick-host-a"),
    )
    .await
    .unwrap();
    writeback::insert_failed_outputs(
        &pool,
        &drv_c,
        &["/nix/store/c-out".to_string()],
        3600,
        Some("healthy-host-z"),
    )
    .await
    .unwrap();
    writeback::insert_failed_outputs(
        &pool,
        &DrvHash::new("legacy-drv"),
        &["/nix/store/legacy-out".to_string()],
        3600,
        None,
    )
    .await
    .unwrap();

    // Refute by worker_id=sick-host-a.
    let url = format!("{}/admin/refute", handle.base_url);
    let (status, body) = post_json(
        &url,
        serde_json::json!({
            "worker_id": "sick-host-a",
        }),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "body: {body}");
    assert_eq!(
        body["rows_affected"].as_u64(),
        Some(2),
        "exactly two sick-host-a rows must be deleted"
    );

    // The sick worker's entries are gone; the other two survive.
    let all: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT output_path, worker_id FROM failed_outputs ORDER BY output_path")
            .fetch_all(&pool)
            .await
            .unwrap();
    let paths: Vec<&str> = all.iter().map(|(p, _)| p.as_str()).collect();
    assert_eq!(
        paths,
        vec!["/nix/store/c-out", "/nix/store/legacy-out"],
        "only the non-sick rows must remain: got {all:?}"
    );
    // Legacy (NULL worker_id) row survives — refute by worker_id must
    // NOT touch unattributed rows.
    let legacy = all
        .iter()
        .find(|(p, _)| p == "/nix/store/legacy-out")
        .expect("legacy row must survive");
    assert!(
        legacy.1.is_none(),
        "legacy row's worker_id stays NULL: {legacy:?}"
    );
}

/// insert_failed_outputs must actually persist the worker_id column
/// for new rows. Guard against a future refactor dropping the bind.
#[sqlx::test]
async fn insert_failed_outputs_persists_worker_id_column(pool: PgPool) {
    let _handle = spawn_server(pool.clone()).await;
    let drv = DrvHash::new("persist-worker-id");
    let path = "/nix/store/persist-worker-out".to_string();
    writeback::insert_failed_outputs(
        &pool,
        &drv,
        std::slice::from_ref(&path),
        3600,
        Some("worker-xyz"),
    )
    .await
    .unwrap();

    let row: (Option<String>,) = sqlx::query_as(
        "SELECT worker_id FROM failed_outputs WHERE output_path = $1",
    )
    .bind(&path)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0.as_deref(), Some("worker-xyz"));
}

#[sqlx::test]
async fn refute_rejects_oversized_batch(pool: PgPool) {
    // Defense-in-depth: a malformed /admin/refute body with 10k
    // `output_paths` must be rejected cleanly, not slow-pathed
    // into a DB query that holds a pool connection for seconds.
    let handle = spawn_server(pool).await;
    let url = format!("{}/admin/refute", handle.base_url);
    let paths: Vec<String> = (0..10_000)
        .map(|i| format!("/nix/store/over-{i}-p"))
        .collect();
    let (status, body) = post_json(
        &url,
        serde_json::json!({
            "output_paths": paths,
        }),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("max_input_drvs_per_drv"),
        "error must cite the bound used: {body}"
    );
}
