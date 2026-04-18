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
    writeback::insert_failed_outputs(&pool, &drv_hash, std::slice::from_ref(&output), 3600)
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
    writeback::insert_failed_outputs(&pool, &drv_hash, &outs, 3600)
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
