//! Direct tests for `durable::writeback` helpers. The integration
//! suite covers these transitively via the HTTP surface; these tests
//! pin each function's contract at the source so a behavioral drift
//! (e.g., `upsert_job` suddenly double-inserting) surfaces right here
//! instead of three levels away.

use std::collections::HashSet;

use chrono::{Duration as ChronoDuration, Utc};
use nix_ci_core::durable::writeback::{
    failed_output_hits, find_job_by_external_ref, heartbeat_job, insert_failed_outputs,
    list_jobs_by_status, lookup_job_by_external_ref, lookup_job_by_id, persist_terminal_snapshot,
    seal_job, transition_job_terminal, upsert_job,
};
use nix_ci_core::types::{DrvHash, JobCounts, JobId, JobStatus, JobStatusResponse};
use sqlx::PgPool;

async fn migrate(pool: &PgPool) {
    sqlx::migrate!("./migrations").run(pool).await.unwrap();
}

// ─── upsert_job ───────────────────────────────────────────────────────

#[sqlx::test]
async fn upsert_job_is_idempotent_on_duplicate_id(pool: PgPool) {
    migrate(&pool).await;
    let id = JobId::new();
    upsert_job(&pool, id, Some("ref-A")).await.unwrap();
    // Second call with *different* external_ref must not double-insert
    // or clobber; the ON CONFLICT DO NOTHING means the first write wins.
    upsert_job(&pool, id, Some("ref-B")).await.unwrap();

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE id = $1")
        .bind(id.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "ON CONFLICT DO NOTHING must not double-insert");

    let (stored_ref,): (Option<String>,) =
        sqlx::query_as("SELECT external_ref FROM jobs WHERE id = $1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        stored_ref.as_deref(),
        Some("ref-A"),
        "first write wins; second must not clobber"
    );
}

// ─── find_job_by_external_ref ─────────────────────────────────────────

#[sqlx::test]
async fn find_job_by_external_ref_returns_none_when_missing(pool: PgPool) {
    migrate(&pool).await;
    let got = find_job_by_external_ref(&pool, "no-such-ref")
        .await
        .unwrap();
    assert!(got.is_none());
}

#[sqlx::test]
async fn find_job_by_external_ref_returns_id_after_upsert(pool: PgPool) {
    migrate(&pool).await;
    let id = JobId::new();
    upsert_job(&pool, id, Some("pr-42")).await.unwrap();
    let got = find_job_by_external_ref(&pool, "pr-42")
        .await
        .unwrap()
        .expect("ref exists");
    assert_eq!(got, id, "retrieved id must match the upserted one");
}

// ─── heartbeat_job ────────────────────────────────────────────────────

#[sqlx::test]
async fn heartbeat_job_returns_true_for_pending_and_advances_timestamp(pool: PgPool) {
    migrate(&pool).await;
    let id = JobId::new();
    upsert_job(&pool, id, None).await.unwrap();

    // Capture the initial heartbeat, sleep past the timestamp resolution
    // boundary, then heartbeat again. The second timestamp must be
    // strictly greater — otherwise the reaper's "last_heartbeat < now() -
    // interval" filter couldn't distinguish a hung job from a live one.
    let (t0,): (chrono::DateTime<chrono::Utc>,) =
        sqlx::query_as("SELECT last_heartbeat FROM jobs WHERE id = $1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let ok = heartbeat_job(&pool, id).await.unwrap();
    assert!(ok, "heartbeat on a pending job must return true");

    let (t1,): (chrono::DateTime<chrono::Utc>,) =
        sqlx::query_as("SELECT last_heartbeat FROM jobs WHERE id = $1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        t1 > t0,
        "heartbeat must advance last_heartbeat (got {t0} → {t1})"
    );
}

#[sqlx::test]
async fn heartbeat_job_returns_false_for_terminal_job(pool: PgPool) {
    migrate(&pool).await;
    let id = JobId::new();
    upsert_job(&pool, id, None).await.unwrap();
    // Drive the job to terminal.
    let snap = serde_json::json!({ "status": "done" });
    transition_job_terminal(&pool, id, "done", &snap)
        .await
        .unwrap();
    let ok = heartbeat_job(&pool, id).await.unwrap();
    assert!(
        !ok,
        "heartbeat on a terminal job must return false — the worker uses this to stop its loop"
    );
}

#[sqlx::test]
async fn heartbeat_job_returns_false_for_missing_job(pool: PgPool) {
    migrate(&pool).await;
    let ghost = JobId::new();
    let ok = heartbeat_job(&pool, ghost).await.unwrap();
    assert!(!ok, "heartbeat on a nonexistent job must return false");
}

// ─── seal_job ─────────────────────────────────────────────────────────

#[sqlx::test]
async fn seal_job_is_idempotent_on_repeat(pool: PgPool) {
    migrate(&pool).await;
    let id = JobId::new();
    upsert_job(&pool, id, None).await.unwrap();
    assert!(
        seal_job(&pool, id).await.unwrap(),
        "first seal returns true"
    );
    assert!(
        seal_job(&pool, id).await.unwrap(),
        "second seal still returns true — the UPDATE still matches the row"
    );
    let (sealed,): (bool,) = sqlx::query_as("SELECT sealed FROM jobs WHERE id = $1")
        .bind(id.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(sealed);
}

#[sqlx::test]
async fn seal_job_returns_false_for_missing(pool: PgPool) {
    migrate(&pool).await;
    let ghost = JobId::new();
    assert!(
        !seal_job(&pool, ghost).await.unwrap(),
        "seal of a nonexistent row must return false so the caller 404s"
    );
}

// ─── persist_terminal_snapshot ────────────────────────────────────────

fn snapshot(id: JobId, status: JobStatus) -> JobStatusResponse {
    JobStatusResponse {
        id,
        status,
        sealed: true,
        counts: JobCounts::default(),
        failures: vec![],
        eval_error: None,
        eval_errors: vec![],
        suspected_worker_infra: None,
    }
}

#[sqlx::test]
async fn persist_terminal_snapshot_writes_status_and_result_in_one_pass(pool: PgPool) {
    migrate(&pool).await;
    let id = JobId::new();
    upsert_job(&pool, id, None).await.unwrap();
    let snap = snapshot(id, JobStatus::Done);
    let transitioned = persist_terminal_snapshot(&pool, id, &snap).await.unwrap();
    assert!(transitioned);

    // The row must be both `done` AND have a matching result JSONB
    // payload so post-terminal GET /jobs/{id} returns the right shape.
    let (status, done_at, result): (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
    ) = sqlx::query_as("SELECT status, done_at, result FROM jobs WHERE id = $1")
        .bind(id.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "done");
    assert!(
        done_at.is_some(),
        "done_at must be populated for terminal jobs"
    );
    let r = result.expect("result must be set");
    assert_eq!(r["status"], "done");
}

#[sqlx::test]
async fn persist_terminal_snapshot_second_call_is_noop(pool: PgPool) {
    migrate(&pool).await;
    let id = JobId::new();
    upsert_job(&pool, id, None).await.unwrap();
    let first = snapshot(id, JobStatus::Done);
    assert!(persist_terminal_snapshot(&pool, id, &first).await.unwrap());

    // Attempt to overwrite with a *different* terminal — the WHERE
    // done_at IS NULL guard must block it. The first write wins.
    let second = snapshot(id, JobStatus::Failed);
    let transitioned = persist_terminal_snapshot(&pool, id, &second).await.unwrap();
    assert!(
        !transitioned,
        "second persist_terminal_snapshot must be a no-op"
    );

    let (status,): (String,) = sqlx::query_as("SELECT status FROM jobs WHERE id = $1")
        .bind(id.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "done", "original terminal state must be preserved");
}

// ─── insert_failed_outputs ────────────────────────────────────────────

#[sqlx::test]
async fn insert_failed_outputs_empty_slice_is_noop(pool: PgPool) {
    // The early-return path: no rows inserted, no DB errors. Easy to
    // miss if the branch gets refactored away.
    migrate(&pool).await;
    let drv = DrvHash::new("abc-empty.drv");
    insert_failed_outputs(&pool, &drv, &[], 60, None).await.unwrap();
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM failed_outputs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test]
async fn insert_failed_outputs_on_conflict_is_noop(pool: PgPool) {
    migrate(&pool).await;
    let drv = DrvHash::new("abc123-failing.drv");
    let paths = vec![
        "/nix/store/out-a-fail".to_string(),
        "/nix/store/out-b-fail".to_string(),
    ];
    insert_failed_outputs(&pool, &drv, &paths, 3600, None)
        .await
        .unwrap();
    // Re-insert the same paths: ON CONFLICT DO NOTHING must keep the
    // row count at 2. Without the guard, the second insert would
    // either error or duplicate the row.
    insert_failed_outputs(&pool, &drv, &paths, 3600, None)
        .await
        .unwrap();
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM failed_outputs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

// ─── failed_output_hits ───────────────────────────────────────────────

#[sqlx::test]
async fn failed_output_hits_empty_input_is_empty(pool: PgPool) {
    migrate(&pool).await;
    let got = failed_output_hits(&pool, &[]).await;
    assert!(
        got.is_empty(),
        "zero inputs must produce zero hits without a DB trip"
    );
}

#[sqlx::test]
async fn failed_output_hits_returns_only_unexpired_matches(pool: PgPool) {
    migrate(&pool).await;
    let drv = DrvHash::new("hits-test-hash.drv");
    let live_path = "/nix/store/LIVE-out-hits-test".to_string();
    let stale_path = "/nix/store/STALE-out-hits-test".to_string();

    // Insert a live row (long TTL) and a manually-expired row via
    // direct SQL — the helper doesn't expose an expire-override path
    // so we dip into raw SQL for the expired case.
    insert_failed_outputs(&pool, &drv, std::slice::from_ref(&live_path), 3600, None)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO failed_outputs (output_path, drv_hash, expires_at) VALUES ($1, $2, now() - interval '1 hour')",
    )
    .bind(&stale_path)
    .bind(drv.as_str())
    .execute(&pool)
    .await
    .unwrap();

    let got = failed_output_hits(
        &pool,
        &[
            live_path.as_str(),
            stale_path.as_str(),
            "/nix/store/missing-unique",
        ],
    )
    .await;
    let want: HashSet<String> = [live_path].into_iter().collect();
    assert_eq!(
        got, want,
        "must return exactly the unexpired hit, excluding stale + missing"
    );
}

// ─── lookup_job_by_{external_ref,id} ──────────────────────────────────

#[sqlx::test]
async fn lookup_job_by_id_returns_none_for_missing(pool: PgPool) {
    migrate(&pool).await;
    let ghost = JobId::new();
    assert!(lookup_job_by_id(&pool, ghost).await.unwrap().is_none());
}

#[sqlx::test]
async fn lookup_job_by_id_returns_row_after_terminal(pool: PgPool) {
    migrate(&pool).await;
    let id = JobId::new();
    upsert_job(&pool, id, None).await.unwrap();
    let snap = snapshot(id, JobStatus::Done);
    persist_terminal_snapshot(&pool, id, &snap).await.unwrap();
    let got = lookup_job_by_id(&pool, id).await.unwrap().expect("row");
    assert_eq!(got.status, "done");
    assert!(got.done_at.is_some());
    assert!(got.result.is_some(), "terminal row must carry result JSONB");
}

#[sqlx::test]
async fn lookup_job_by_external_ref_returns_row_fields(pool: PgPool) {
    // There's a UNIQUE index on external_ref, so at most one row can
    // match. Verify the lookup populates every field the caller needs
    // (id, status, done_at, result JSONB) — drift on any would quietly
    // break `GET /jobs/by-external-ref/{ref}` and `nix-ci show`.
    migrate(&pool).await;
    let id = JobId::new();
    upsert_job(&pool, id, Some("some-ref")).await.unwrap();

    // Pre-terminal: done_at/result should be None; status is `pending`.
    let got = lookup_job_by_external_ref(&pool, "some-ref")
        .await
        .unwrap()
        .expect("row");
    assert_eq!(got.id, id);
    assert_eq!(got.status, "pending");
    assert!(got.done_at.is_none());
    assert!(got.result.is_none());

    // Drive to terminal; the same lookup now carries the JSONB result.
    persist_terminal_snapshot(&pool, id, &snapshot(id, JobStatus::Done))
        .await
        .unwrap();
    let got = lookup_job_by_external_ref(&pool, "some-ref")
        .await
        .unwrap()
        .expect("still there");
    assert_eq!(got.status, "done");
    assert!(got.done_at.is_some());
    assert_eq!(
        got.result.unwrap()["status"],
        "done",
        "terminal snapshot must round-trip through the JSONB column"
    );
}

#[sqlx::test]
async fn lookup_job_by_external_ref_returns_none_for_missing(pool: PgPool) {
    migrate(&pool).await;
    assert!(lookup_job_by_external_ref(&pool, "nope")
        .await
        .unwrap()
        .is_none());
}

// ─── list_jobs_by_status ──────────────────────────────────────────────

#[sqlx::test]
async fn list_jobs_by_status_filters_to_requested_status_only(pool: PgPool) {
    migrate(&pool).await;
    let done_id = JobId::new();
    let failed_id = JobId::new();
    let pending_id = JobId::new();
    for (id, s) in [
        (done_id, JobStatus::Done),
        (failed_id, JobStatus::Failed),
        (pending_id, JobStatus::Done), // will remain pending below
    ] {
        upsert_job(&pool, id, None).await.unwrap();
        if id != pending_id {
            let snap = snapshot(id, s);
            persist_terminal_snapshot(&pool, id, &snap).await.unwrap();
        }
    }

    let rows = list_jobs_by_status(&pool, "done", None, 10).await.unwrap();
    let ids: Vec<JobId> = rows.iter().map(|r| r.id).collect();
    assert!(ids.contains(&done_id), "done list must include done_id");
    assert!(
        !ids.contains(&failed_id),
        "done list must exclude failed_id"
    );
    assert!(
        !ids.contains(&pending_id),
        "list_jobs_by_status must exclude non-terminal rows (done_at IS NOT NULL guard)"
    );
}

#[sqlx::test]
async fn list_jobs_by_status_cursor_excludes_rows_at_or_after_cursor(pool: PgPool) {
    migrate(&pool).await;
    let first_id = JobId::new();
    upsert_job(&pool, first_id, None).await.unwrap();
    persist_terminal_snapshot(&pool, first_id, &snapshot(first_id, JobStatus::Done))
        .await
        .unwrap();
    // Cursor ≫ now — list must be empty.
    let future = Utc::now() + ChronoDuration::hours(24);
    // Use `None` -> pick up the row to extract its done_at, then
    // re-query with a cursor equal to that done_at. The helper uses
    // a strict `<` comparison, so the row at exactly that timestamp
    // must be excluded (otherwise a client paginating would see the
    // same row twice).
    let initial = list_jobs_by_status(&pool, "done", None, 10).await.unwrap();
    let row = initial
        .iter()
        .find(|r| r.id == first_id)
        .expect("inserted row");
    let ts = row.done_at.unwrap();
    let next = list_jobs_by_status(&pool, "done", Some(ts), 10)
        .await
        .unwrap();
    assert!(
        next.iter().all(|r| r.id != first_id),
        "strict `<` comparison means cursor must exclude the row at its own timestamp"
    );

    // And a cursor in the future still hides *nothing* but future rows;
    // here that means the inserted row must reappear.
    let all_past = list_jobs_by_status(&pool, "done", Some(future), 10)
        .await
        .unwrap();
    assert!(
        all_past.iter().any(|r| r.id == first_id),
        "cursor > ts must include row at ts"
    );
}
