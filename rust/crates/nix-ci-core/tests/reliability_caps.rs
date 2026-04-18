//! Reliability / input-hardening tests. Validates that the caps added
//! to guard against malicious or buggy submitters behave correctly:
//!
//! * Per-drv `input_drvs` cap: over-limit drvs are counted in
//!   `errored` and skipped; the rest of the batch proceeds.
//! * Per-drv `required_features` cap: same shape.
//! * Per-batch `eval_errors` cap: hard 413 rejection (the whole
//!   batch, because silent truncation of user-facing errors would
//!   mislead operators).
//! * A batch full of legitimate drvs at the soft boundaries still
//!   succeeds — the caps must not narrow the real use-case.
//!
//! These sit alongside `ingest_validation.rs`; we keep them separate
//! because the surface is specifically the "poison-input containment"
//! line of defense rather than the correctness baseline.

mod common;

use common::{drv_path, ingest, spawn_server_with_cfg};
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CreateJobRequest, EvalError, IngestBatchRequest, IngestDrvRequest};
use sqlx::PgPool;

fn mk_drv(
    path_tag: &str,
    input_drvs: Vec<String>,
    required_features: Vec<String>,
    is_root: bool,
) -> IngestDrvRequest {
    IngestDrvRequest {
        drv_path: drv_path(path_tag, "pkg"),
        drv_name: "pkg".into(),
        system: "x86_64-linux".into(),
        required_features,
        input_drvs,
        is_root,
        attr: None,
    }
}

#[sqlx::test]
async fn per_drv_input_drvs_cap_skips_over_limit_drv(pool: PgPool) {
    // Tight cap for the test so we don't have to synthesize thousands
    // of fake dep paths. The production cap (4096) sits well above
    // any real nixpkgs eval, so testing the *mechanism* at cap=2 is
    // equivalent to testing at cap=4096 for the same mechanism.
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.max_input_drvs_per_drv = 2;
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

    // Two plausible dep paths; enough to put a drv *over* cap=2 when
    // it lists three inputs.
    let deps_over: Vec<String> = (0..3).map(|i| drv_path(&format!("d{i}"), "dep")).collect();
    let over = mk_drv("boom", deps_over, Vec::new(), true);
    let fine = mk_drv("fine", Vec::new(), Vec::new(), true);

    let batch = IngestBatchRequest {
        drvs: vec![over, fine],
        eval_errors: Vec::new(),
    };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    // The "fine" drv must still be admitted; only the over-cap one
    // counts as errored.
    assert_eq!(resp.new_drvs, 1, "fine drv must land");
    assert_eq!(resp.errored, 1, "over-cap drv must be errored");
    assert_eq!(resp.dedup_skipped, 0);
}

#[sqlx::test]
async fn per_drv_input_drvs_at_exactly_cap_is_accepted(pool: PgPool) {
    // Boundary: cap=2, drv with exactly 2 inputs must be accepted.
    // Off-by-one in the guard (`>` vs `>=`) would fail this test
    // without failing the "over-cap" case above — both are needed.
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.max_input_drvs_per_drv = 2;
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

    let deps_at_cap: Vec<String> = (0..2).map(|i| drv_path(&format!("d{i}"), "dep")).collect();
    let at_cap = mk_drv("good", deps_at_cap, Vec::new(), true);
    let batch = IngestBatchRequest {
        drvs: vec![at_cap],
        eval_errors: Vec::new(),
    };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.errored, 0, "exactly-at-cap drv must be accepted");
    assert_eq!(resp.new_drvs, 1);
}

#[sqlx::test]
async fn per_drv_features_cap_skips_over_limit_drv(pool: PgPool) {
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.max_required_features_per_drv = 2;
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

    let features_over = vec!["kvm".into(), "big-parallel".into(), "benchmark".into()];
    let over = mk_drv("featboom", Vec::new(), features_over, true);
    let fine = mk_drv("featfine", Vec::new(), vec!["kvm".into()], true);
    let batch = IngestBatchRequest {
        drvs: vec![over, fine],
        eval_errors: Vec::new(),
    };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.errored, 1);
    assert_eq!(resp.new_drvs, 1);
}

#[sqlx::test]
async fn eval_errors_over_batch_cap_rejects_with_413(pool: PgPool) {
    // eval_errors is different from the per-drv caps: silently
    // dropping errors is *worse* than rejecting the whole batch,
    // because the user-facing summary would be wrong. We test the
    // reject (not drop) semantics explicitly.
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.max_eval_errors_per_batch = 3;
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

    let eval_errors = (0..4)
        .map(|i| EvalError {
            attr: format!("pkg{i}"),
            error: "evaluation failed".into(),
        })
        .collect();
    let batch = IngestBatchRequest {
        drvs: Vec::new(),
        eval_errors,
    };
    let err = client.ingest_batch(job.id, &batch).await.unwrap_err();
    match err {
        nix_ci_core::Error::PayloadTooLarge(msg) => {
            assert!(msg.contains("eval_errors"), "message should name the cap: {msg}");
        }
        other => panic!("expected PayloadTooLarge, got {other:?}"),
    }
}

#[sqlx::test]
async fn eval_errors_at_exactly_cap_is_accepted(pool: PgPool) {
    let handle = spawn_server_with_cfg(pool, |cfg| {
        cfg.max_eval_errors_per_batch = 3;
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

    let eval_errors: Vec<EvalError> = (0..3)
        .map(|i| EvalError {
            attr: format!("pkg{i}"),
            error: "evaluation failed".into(),
        })
        .collect();
    let batch = IngestBatchRequest {
        drvs: vec![ingest(&drv_path("root", "pkg"), "pkg", &[], true)],
        eval_errors,
    };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    assert_eq!(resp.new_drvs, 1);
    assert_eq!(resp.errored, 0);
}

#[sqlx::test]
async fn production_defaults_accept_realistic_drvs(pool: PgPool) {
    // Canary: a drv at the 95th-percentile of real nixpkgs shape
    // (~200 direct deps, 2-3 features) must succeed with the
    // production defaults. This guards against picking caps that
    // look "generous" but actually reject real traffic.
    let handle = spawn_server_with_cfg(pool, |_| {}).await; // defaults
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let deps: Vec<String> = (0..200)
        .map(|i| drv_path(&format!("dep{i}"), "d"))
        .collect();
    let realistic = IngestDrvRequest {
        drv_path: drv_path("root", "big"),
        drv_name: "big".into(),
        system: "x86_64-linux".into(),
        required_features: vec!["big-parallel".into(), "kvm".into(), "benchmark".into()],
        input_drvs: deps,
        is_root: true,
        attr: None,
    };
    let batch = IngestBatchRequest {
        drvs: vec![realistic],
        eval_errors: Vec::new(),
    };
    let resp = client.ingest_batch(job.id, &batch).await.unwrap();
    // `new_drvs` counts drvs explicitly listed in `req.drvs` (one
    // root here); the 200 deps are wire_dep'd into the graph as
    // placeholder Steps but aren't counted as submitted drvs.
    assert_eq!(resp.errored, 0);
    assert_eq!(resp.new_drvs, 1);
}
