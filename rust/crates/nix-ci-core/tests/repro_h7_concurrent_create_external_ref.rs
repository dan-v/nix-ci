//! Reproducer for H7 (adversarial): two concurrent `POST /jobs` with
//! the same `external_ref` can lose the idempotency race — both find
//! no existing row, both call `upsert_job`, one succeeds and the
//! other hits the `jobs_external_ref_uniq` unique constraint and
//! returns a 500 instead of the idempotent job id.
//!
//! Users of `nix-ci show <ext-ref>` / CCI integrations that retry a
//! lost POST /jobs response expect idempotency on `external_ref`.
//! The race here is tiny (both lookups + both inserts interleaved)
//! but real — especially when a client implements its own retry loop.

mod common;

use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::CreateJobRequest;
use sqlx::PgPool;

#[sqlx::test]
async fn concurrent_create_with_same_external_ref_is_idempotent(pool: PgPool) {
    let handle = common::spawn_server(pool).await;

    // Fire two create_job calls in parallel with the same ref. The
    // contract is: both succeed and return the SAME id.
    let c1 = CoordinatorClient::new(&handle.base_url);
    let c2 = CoordinatorClient::new(&handle.base_url);
    let req = CreateJobRequest {
        external_ref: Some("concurrent-race-ref".into()),
        ..Default::default()
    };
    let (r1, r2) = tokio::join!(c1.create_job(&req), c2.create_job(&req));

    // Both must succeed. Under the current code, one of the two can
    // return an Internal (500) error from the unique-constraint
    // violation on `external_ref`.
    match (&r1, &r2) {
        (Ok(a), Ok(b)) => {
            assert_eq!(a.id, b.id, "same external_ref must resolve to same id");
        }
        other => panic!(
            "concurrent POST /jobs with same external_ref must be idempotent; got {other:?}"
        ),
    }
}
