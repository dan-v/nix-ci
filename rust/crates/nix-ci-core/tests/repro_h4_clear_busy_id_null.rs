//! Regression test for H4: `durable::clear_busy` previously wrote a
//! sentinel with `"id": null` into `jobs.result`. On the next
//! `GET /jobs/{id}`, the server deserialized the blob into
//! `JobStatusResponse` whose `id: JobId(Uuid)` rejects JSON null with
//! `invalid type: null, expected a formatted UUID string` — every job
//! cancelled on coordinator restart returned 500.
//!
//! The fix: build the sentinel in-SQL via `jsonb_build_object('id',
//! id::text, ...)` so the per-row UUID is populated as a string.
//!
//! This test exercises the end-to-end flow: create a job, run
//! `clear_busy` to force it into the sentinel-cancelled state, then
//! GET the status endpoint and verify it returns a valid
//! JobStatusResponse (not 500).

mod common;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CreateJobRequest, JobStatus};
use sqlx::PgPool;

#[sqlx::test]
async fn clear_busy_sentinel_survives_round_trip_to_client(pool: PgPool) {
    let handle = common::spawn_server(pool.clone()).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .expect("create_job must succeed");

    // Simulate a coordinator restart by running clear_busy directly
    // on the pool. This flips the job's status to `cancelled` with
    // the sentinel `result` JSONB.
    nix_ci_core::durable::clear_busy(&pool)
        .await
        .expect("clear_busy must succeed");

    // The in-memory submission still exists at this point, so
    // `jobs::status` returns the live response (not the persisted
    // one). Drop it to force the DB-read path — which is the
    // real-world shape: after a restart the in-memory dispatcher
    // starts empty and status polls land on the persisted snapshot.
    handle.dispatcher.submissions.remove(job.id);

    // GET /jobs/{id} must return a valid JobStatusResponse — NOT 500.
    // Before the fix, the status handler's
    // `serde_json::from_value::<JobStatusResponse>(result)` call
    // failed because `"id": null` can't deserialize into `JobId(Uuid)`.
    let snap = client
        .status(job.id)
        .await
        .expect("status must return valid JSON (not 500)");
    assert_eq!(snap.id, job.id, "returned id must match the original job");
    assert_eq!(snap.status, JobStatus::Cancelled);
    assert_eq!(
        snap.eval_error.as_deref(),
        Some("coordinator restarted; job aborted")
    );
}
