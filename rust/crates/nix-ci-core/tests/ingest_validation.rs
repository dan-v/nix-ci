//! Auto-split from edge_cases.rs per H15.B audit decomposition.
//! Topic: ingest input validation (empty/malformed fields, length boundaries).

mod common;


use common::{drv_path, ingest, spawn_server};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{
    CreateJobRequest, IngestBatchRequest, IngestDrvRequest,
};
use sqlx::PgPool;


#[sqlx::test]
async fn ingest_rejects_empty_drv_path(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let bad = IngestDrvRequest {
        drv_path: "".into(),
        drv_name: "x".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        attr: None,
    };
    let err = client.ingest_drv(job.id, &bad).await.unwrap_err();
    match err {
        nix_ci_core::Error::BadRequest(_) => {}
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

#[sqlx::test]
async fn ingest_rejects_malformed_drv_path(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    // No trailing .drv, no hyphen — drv_hash_from_path should reject.
    let bad = IngestDrvRequest {
        drv_path: "/nix/store/nohyphen".into(),
        drv_name: "x".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        attr: None,
    };
    let err = client.ingest_drv(job.id, &bad).await.unwrap_err();
    match err {
        nix_ci_core::Error::BadRequest(_) => {}
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

#[sqlx::test]
async fn ingest_batch_partial_validation_counts_errors(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let good = drv_path("good", "pkg");
    let bad_empty = IngestDrvRequest {
        drv_path: "".into(),
        drv_name: "x".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: false,
        attr: None,
    };
    let bad_nohyphen = IngestDrvRequest {
        drv_path: "/nix/store/nohyphen.drv".into(),
        drv_name: "x".into(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: false,
        attr: None,
    };
    let batch = IngestBatchRequest {
        drvs: vec![ingest(&good, "pkg", &[], true), bad_empty, bad_nohyphen],
    eval_errors: Vec::new(),
        };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.new_drvs, 1);
    assert_eq!(resp.errored, 2);
}

#[sqlx::test]
async fn ingest_length_boundaries_accept_at_max_reject_above(pool: PgPool) {
    // At exactly `max_drv_path_bytes` the drv must be accepted; at
    // max+1 rejected. Guards the `> max` (vs `>= max`) comparison.
    use nix_ci_core::config::ServerConfig;
    // Shrink caps to keep the test fast and the boundary arithmetic
    // obvious.
    const PATH_CAP: usize = 80;
    const NAME_CAP: usize = 20;
    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        cfg.max_drv_path_bytes = PATH_CAP;
        cfg.max_drv_name_bytes = NAME_CAP;
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

    // Build a drv_path of exactly PATH_CAP bytes, name of exactly NAME_CAP.
    let name_at_cap: String = "n".repeat(NAME_CAP);
    // Prefix "/nix/store/" + hash "-" + name_at_cap + ".drv".
    let prefix = "/nix/store/";
    let suffix = format!("-{name_at_cap}.drv");
    // Pad the hash region so total == PATH_CAP.
    let hash_len = PATH_CAP - prefix.len() - suffix.len();
    assert!(hash_len > 0);
    let hash = "a".repeat(hash_len);
    let path_at_cap = format!("{prefix}{hash}{suffix}");
    assert_eq!(path_at_cap.len(), PATH_CAP);

    let at_cap = IngestDrvRequest {
        drv_path: path_at_cap.clone(),
        drv_name: name_at_cap.clone(),
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        attr: None,
    };
    let resp = client
        .ingest_batch(job.id, &IngestBatchRequest { drvs: vec![at_cap], eval_errors: Vec::new() })
        .await
        .unwrap();
    assert_eq!(resp.errored, 0, "drv at exactly cap must be accepted");
    assert_eq!(resp.new_drvs, 1);

    // Now drv_path of cap+1: rejected.
    let over_cap = IngestDrvRequest {
        drv_path: format!("{path_at_cap}x"),
        drv_name: name_at_cap,
        system: "x86_64-linux".into(),
        required_features: vec![],
        input_drvs: vec![],
        is_root: true,
        attr: None,
    };
    let resp = client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![over_cap],
            eval_errors: Vec::new(),
        },
        )
        .await
        .unwrap();
    assert_eq!(resp.errored, 1, "drv at cap+1 must be rejected");
}

#[sqlx::test]
async fn ingest_batch_counts_wire_dep_errors(pool: PgPool) {
    // A drv with a syntactically bad `input_drv` path reaches `wire_dep`
    // which returns Err and increments the errored counter. Guards the
    // `errored += 1` in the dep-wiring branch.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let parent = drv_path("wdpar", "pkg");
    let good_parent = ingest(&parent, "pkg", &["/nix/store/nohyphen.drv"], true);
    let resp = client
        .ingest_batch(
            job.id,
            &IngestBatchRequest {
                drvs: vec![good_parent],
            eval_errors: Vec::new(),
        },
        )
        .await
        .unwrap();
    // Parent itself is valid (1 new drv) but its one dep is bad → 1 errored.
    assert_eq!(resp.new_drvs, 1);
    assert_eq!(
        resp.errored, 1,
        "wire_dep must count syntactically-bad input_drv paths"
    );
}

#[sqlx::test]
async fn ingest_batch_rejects_empty_drv_name(pool: PgPool) {
    // drv_path="" is caught by drv_hash_from_path later; but an empty
    // drv_name or empty system is only rejected by the early validation
    // block. A mutant that weakens the `||` chain would let either
    // through.
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut bad = ingest(&drv_path("ok", "ignored"), "ignored", &[], true);
    bad.drv_name = "".into();
    let batch = IngestBatchRequest { drvs: vec![bad], eval_errors: Vec::new() };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.errored, 1);
    assert_eq!(resp.new_drvs, 0);
}

#[sqlx::test]
async fn ingest_batch_rejects_empty_system(pool: PgPool) {
    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut bad = ingest(&drv_path("ok", "ignored"), "ignored", &[], true);
    bad.system = "".into();
    let batch = IngestBatchRequest { drvs: vec![bad], eval_errors: Vec::new() };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.errored, 1);
    assert_eq!(resp.new_drvs, 0);
}
