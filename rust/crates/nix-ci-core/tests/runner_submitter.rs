//! Integration test for `runner::submitter` — drives a real coordinator
//! with real .drv files on disk and a synthetic eval_rx stream (no
//! nix-eval-jobs subprocess). Exercises `drv_walk` + batch-ingest
//! against the same dispatcher the server uses in production.

mod common;

use std::io::Write;
use std::sync::Arc;

use common::spawn_server;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::runner::eval_jobs::EvalLine;
use nix_ci_core::runner::submitter;
use nix_ci_core::types::{CreateJobRequest, JobStatus};
use sqlx::PgPool;
use tokio::sync::{mpsc, watch};

/// Write a minimal ATerm-format .drv file to `dir/<name>.drv` and
/// return the full path. `input_drvs` is a list of absolute paths to
/// prior .drv files this derivation depends on.
fn write_drv(dir: &std::path::Path, name: &str, input_drvs: &[&str]) -> std::path::PathBuf {
    // `drv_hash_from_path` on the server side requires a `-` in the
    // basename, so we prefix a synthetic hash.
    let path = dir.join(format!("hash-{name}.drv"));
    let out_store = format!("/nix/store/OUT-{name}");
    let outputs = format!(r#"[("out","{out_store}","","")]"#);
    let inputs_str = input_drvs
        .iter()
        .map(|p| format!(r#"("{p}",["out"])"#))
        .collect::<Vec<_>>()
        .join(",");
    let input_drvs_field = format!("[{inputs_str}]");
    let body = format!(
        r#"Derive({outputs},{input_drvs_field},["/nix/store/BUILDER"],"x86_64-linux","/bin/sh",[],[("name","{name}"),("out","{out_store}"),("system","x86_64-linux")])"#,
    );
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(body.as_bytes()).unwrap();
    path
}

#[sqlx::test]
async fn submitter_ingests_drv_closure_into_coordinator(pool: PgPool) {
    // Tempdir with: leaf.drv, mid.drv(→leaf), root.drv(→mid).
    let dir = tempfile::tempdir().unwrap();
    let leaf = write_drv(dir.path(), "leaf", &[]);
    let mid = write_drv(dir.path(), "mid", &[leaf.to_str().unwrap()]);
    let root = write_drv(dir.path(), "root", &[mid.to_str().unwrap()]);

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    // One EvalLine pointing at root; neededBuilds covers all three so
    // the walker's filter keeps everything.
    let line = EvalLine {
        drv_path: Some(root.to_string_lossy().into_owned()),
        attr: Some("root".into()),
        name: Some("root".into()),
        error: None,
        cache_status: Some(serde_json::Value::String("notBuilt".into())),
        is_cached: None,
        needed_builds: vec![
            root.to_string_lossy().into_owned(),
            mid.to_string_lossy().into_owned(),
            leaf.to_string_lossy().into_owned(),
        ],
    };

    let (tx, rx) = mpsc::channel::<EvalLine>(4);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    tx.send(line).await.unwrap();
    drop(tx); // Close the channel so submitter exits after draining.

    let stats = submitter::run(client.clone(), job.id, rx, shutdown_rx)
        .await
        .unwrap();

    // All three drvs should have been ingested on the first (and only)
    // batch: 3 new drvs, no dedup, no errors.
    assert_eq!(stats.new_drvs, 3, "all three drvs must be new");
    assert_eq!(stats.dedup_skipped, 0);
    assert_eq!(stats.errors, 0);
    assert_eq!(stats.eval_errors, 0);
    assert_eq!(stats.cached_skipped, 0);

    // Verify coordinator snapshot matches.
    let snap: nix_ci_core::types::AdminSnapshot =
        reqwest::get(format!("{}/admin/snapshot", handle.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(snap.steps_total, 3);
}

#[sqlx::test]
async fn submitter_skips_attr_with_eval_error(pool: PgPool) {
    // A line carrying an `error` field must NOT attempt to ingest —
    // it's a per-attribute eval error that doesn't abort the run.
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let err_line = EvalLine {
        drv_path: None,
        attr: Some("broken".into()),
        name: None,
        error: Some("syntax error".into()),
        cache_status: None,
        is_cached: None,
        needed_builds: vec![],
    };

    let (tx, rx) = mpsc::channel::<EvalLine>(4);
    let (_stx, srx) = watch::channel(false);
    tx.send(err_line).await.unwrap();
    drop(tx);

    let stats = submitter::run(client.clone(), job.id, rx, srx)
        .await
        .unwrap();
    assert_eq!(stats.eval_errors, 1);
    assert_eq!(stats.new_drvs, 0);
    assert_eq!(stats.errors, 0);

    let snap: nix_ci_core::types::AdminSnapshot =
        reqwest::get(format!("{}/admin/snapshot", handle.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(snap.steps_total, 0, "eval error must not create any steps");
}

#[sqlx::test]
async fn submitter_skips_fully_cached_attr(pool: PgPool) {
    // cacheStatus="cached" means the attr's whole closure is
    // substitutable — nothing to build, nothing to ingest.
    let dir = tempfile::tempdir().unwrap();
    let root = write_drv(dir.path(), "cached-root", &[]);

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let line = EvalLine {
        drv_path: Some(root.to_string_lossy().into_owned()),
        attr: Some("cached".into()),
        name: Some("cached".into()),
        error: None,
        cache_status: Some(serde_json::Value::String("cached".into())),
        is_cached: None,
        needed_builds: vec![],
    };

    let (tx, rx) = mpsc::channel::<EvalLine>(4);
    let (_stx, srx) = watch::channel(false);
    tx.send(line).await.unwrap();
    drop(tx);

    let stats = submitter::run(client.clone(), job.id, rx, srx)
        .await
        .unwrap();
    assert_eq!(stats.cached_skipped, 1);
    assert_eq!(stats.new_drvs, 0);
    let snap: nix_ci_core::types::AdminSnapshot =
        reqwest::get(format!("{}/admin/snapshot", handle.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(snap.steps_total, 0);
}

#[sqlx::test]
async fn submitter_dedups_across_overlapping_closures(pool: PgPool) {
    // Two attrs, both depending on the same leaf. The leaf must be
    // POSTed exactly once (first EvalLine owns the first insert,
    // second sees it in the `submitted` set and drops it).
    let dir = tempfile::tempdir().unwrap();
    let shared = write_drv(dir.path(), "shared-stdenv", &[]);
    let a = write_drv(dir.path(), "attr-a", &[shared.to_str().unwrap()]);
    let b = write_drv(dir.path(), "attr-b", &[shared.to_str().unwrap()]);

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let mk_line = |path: &std::path::Path, name: &str, needs: Vec<String>| EvalLine {
        drv_path: Some(path.to_string_lossy().into_owned()),
        attr: Some(name.into()),
        name: Some(name.into()),
        error: None,
        cache_status: Some(serde_json::Value::String("notBuilt".into())),
        is_cached: None,
        needed_builds: needs,
    };

    let (tx, rx) = mpsc::channel::<EvalLine>(4);
    let (_stx, srx) = watch::channel(false);
    tx.send(mk_line(
        &a,
        "a",
        vec![
            a.to_string_lossy().into_owned(),
            shared.to_string_lossy().into_owned(),
        ],
    ))
    .await
    .unwrap();
    tx.send(mk_line(
        &b,
        "b",
        vec![
            b.to_string_lossy().into_owned(),
            shared.to_string_lossy().into_owned(),
        ],
    ))
    .await
    .unwrap();
    drop(tx);

    let stats = submitter::run(client.clone(), job.id, rx, srx)
        .await
        .unwrap();
    // 3 unique drvs across the two lines; the second line must NOT
    // re-POST `shared`.
    assert_eq!(stats.new_drvs, 3);
    let snap: nix_ci_core::types::AdminSnapshot =
        reqwest::get(format!("{}/admin/snapshot", handle.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(snap.steps_total, 3);
    // And seal → all three run fine with no workers, stay Pending.
    let status = client.status(job.id).await.unwrap();
    assert!(matches!(status.status, JobStatus::Pending));
    let _ = dir; // keep alive
}

#[sqlx::test]
async fn submitter_records_server_dedup_skipped_across_runs(pool: PgPool) {
    // The submitter's own `submitted` set filters dupes within ONE
    // run; but across separate runs against the same coordinator,
    // dupes are reported by the SERVER as `resp.dedup_skipped` and
    // accumulated into `stats.dedup_skipped`. Guards the
    // `stats.dedup_skipped += resp.dedup_skipped` accumulator path.
    let dir = tempfile::tempdir().unwrap();
    let leaf = write_drv(dir.path(), "shared-leaf", &[]);

    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let line = EvalLine {
        drv_path: Some(leaf.to_string_lossy().into_owned()),
        attr: Some("a".into()),
        name: Some("a".into()),
        error: None,
        cache_status: Some(serde_json::Value::String("notBuilt".into())),
        is_cached: None,
        needed_builds: vec![leaf.to_string_lossy().into_owned()],
    };

    // Run 1: introduces the drv.
    let (tx, rx) = mpsc::channel::<EvalLine>(4);
    let (_stx, srx) = watch::channel(false);
    tx.send(line.clone()).await.unwrap();
    drop(tx);
    let s1 = submitter::run(client.clone(), job.id, rx, srx)
        .await
        .unwrap();
    assert_eq!(s1.new_drvs, 1);
    assert_eq!(s1.dedup_skipped, 0);

    // Run 2: same drv → server replies dedup_skipped=1 → stats reflects.
    let (tx, rx) = mpsc::channel::<EvalLine>(4);
    let (_stx, srx) = watch::channel(false);
    tx.send(line).await.unwrap();
    drop(tx);
    let s2 = submitter::run(client.clone(), job.id, rx, srx)
        .await
        .unwrap();
    assert_eq!(
        s2.new_drvs, 0,
        "second run reports zero new — server already had it"
    );
    assert_eq!(
        s2.dedup_skipped, 1,
        "submitter must accumulate server-reported dedup_skipped"
    );
}

#[sqlx::test]
async fn submitter_records_server_errored_in_stats(pool: PgPool) {
    // When the server rejects a drv inside a batch (here: drv_name
    // exceeds `max_drv_name_bytes`), `resp.errored > 0` and the
    // submitter must accumulate that into `stats.errors`. Guards the
    // `stats.errors += resp.errored` line.
    use nix_ci_core::config::ServerConfig;
    let dir = tempfile::tempdir().unwrap();
    let drv = write_drv(dir.path(), "longname-pkg", &[]);

    let handle = common::spawn_server_with_cfg(pool, |cfg: &mut ServerConfig| {
        // Cap drv_name bytes below what the parser-extracted name will
        // be ("longname-pkg" = 12 bytes), so the server rejects.
        cfg.max_drv_name_bytes = 4;
    })
    .await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let line = EvalLine {
        drv_path: Some(drv.to_string_lossy().into_owned()),
        attr: Some("longname".into()),
        name: Some("longname".into()),
        error: None,
        cache_status: Some(serde_json::Value::String("notBuilt".into())),
        is_cached: None,
        needed_builds: vec![drv.to_string_lossy().into_owned()],
    };

    let (tx, rx) = mpsc::channel::<EvalLine>(4);
    let (_stx, srx) = watch::channel(false);
    tx.send(line).await.unwrap();
    drop(tx);
    let stats = submitter::run(client.clone(), job.id, rx, srx)
        .await
        .unwrap();
    assert_eq!(stats.new_drvs, 0);
    assert_eq!(
        stats.errors, 1,
        "server rejection must surface as stats.errors"
    );
}

#[sqlx::test]
async fn submitter_fails_fast_on_unreadable_root(pool: PgPool) {
    // Regression guard: a closure walk that can't read the root .drv
    // MUST fail the whole submission, not silently succeed with zero
    // ingested drvs. The "giant nixpkgs overlay" contract is that
    // nix-ci is never the reason a build doesn't happen — if we can't
    // honor the submission, the caller has to see that clearly. The
    // prior behavior (warn + skip) would seal the job as Done with no
    // drvs, which looks identical to "everything was cached" and is
    // unrecoverable without log archaeology.
    let handle = spawn_server(pool).await;
    let client = Arc::new(CoordinatorClient::new(&handle.base_url));
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let line = EvalLine {
        drv_path: Some("/nonexistent/nope-xyz.drv".into()),
        attr: Some("ghost".into()),
        name: Some("ghost".into()),
        error: None,
        cache_status: Some(serde_json::Value::String("notBuilt".into())),
        is_cached: None,
        needed_builds: vec!["/nonexistent/nope-xyz.drv".into()],
    };
    let (tx, rx) = mpsc::channel::<EvalLine>(4);
    let (_stx, srx) = watch::channel(false);
    tx.send(line).await.unwrap();
    drop(tx);

    let result = submitter::run(client.clone(), job.id, rx, srx).await;
    let err = match result {
        Ok(_) => panic!("walk failure must propagate as a submitter error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("walk") || msg.contains(".drv") || msg.contains("nope-xyz"),
        "error must name the walk cause: {msg}"
    );
}
