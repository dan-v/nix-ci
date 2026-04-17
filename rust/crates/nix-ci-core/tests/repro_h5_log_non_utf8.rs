//! Regression test for H5: the worker deliberately captures raw
//! stderr bytes (comment in worker.rs: "avoiding the lossy conversion
//! preserves binary diagnostic output"). Before the fix, the server's
//! `fetch_decompressed()` called `read_to_string()` which REJECTS
//! non-UTF-8 bytes → 500 on `GET /jobs/{id}/claims/{cid}/log`.
//!
//! The fix: decompress into `Vec<u8>` and convert via
//! `String::from_utf8_lossy`, so non-UTF-8 bytes surface as U+FFFD
//! replacement characters instead of making the log unretrievable.

use std::io::Write;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use nix_ci_core::durable::logs::{fetch_decompressed, LogPutRequest, LogRow, LogStore};
use nix_ci_core::error::Result;
use nix_ci_core::types::{ClaimId, DrvHash, JobId};

/// In-memory LogStore that always returns a pre-seeded gzipped blob.
struct SeededStore {
    gz: Vec<u8>,
}

#[async_trait]
impl LogStore for SeededStore {
    async fn put(&self, _meta: LogPutRequest, _gz: Vec<u8>) -> Result<()> {
        unreachable!("not exercised by this test")
    }

    async fn fetch_gz(&self, _job_id: JobId, _claim_id: ClaimId) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.gz.clone()))
    }

    async fn list_attempts(&self, _job_id: JobId, _drv_hash: &DrvHash) -> Result<Vec<LogRow>> {
        Ok(Vec::new())
    }

    async fn prune_older_than(&self, _older_than: DateTime<Utc>) -> Result<u64> {
        Ok(0)
    }

    async fn total_bytes(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn row_count(&self) -> Result<u64> {
        Ok(0)
    }
}

#[tokio::test]
async fn fetch_decompressed_does_not_fail_on_non_utf8_payload() {
    // Gzip a payload containing bytes that are NEVER valid UTF-8 on
    // their own. Realistic analogs: a core-dump fragment mixed into
    // stderr, non-UTF-8 locale output, binary artifacts accidentally
    // printed. Before the fix these would make the whole log
    // unretrievable via `GET /jobs/{id}/claims/{cid}/log`.
    let raw: Vec<u8> = vec![
        b'e', b'r', b'r', b':', b' ', 0xFF, 0xFE, 0xFD, b'\n',
    ];
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(&raw).unwrap();
    let gz = enc.finish().unwrap();

    let store = SeededStore { gz };
    let out = fetch_decompressed(&store, JobId::new(), ClaimId::new())
        .await
        .expect("fetch_decompressed must succeed on non-UTF-8 payload")
        .expect("store returned Some(..)");

    // Every run of valid bytes from the original is preserved; the
    // invalid bytes become U+FFFD (each replaced independently).
    assert!(out.starts_with("err: "), "prefix must be preserved");
    assert!(out.ends_with('\n'), "trailing newline must be preserved");
    // U+FFFD is the Unicode replacement character.
    assert!(
        out.contains('\u{FFFD}'),
        "invalid bytes must be replaced with U+FFFD, got: {out:?}"
    );
}
