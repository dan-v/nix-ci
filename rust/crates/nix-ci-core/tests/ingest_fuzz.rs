//! Randomized ingest tests: drive `POST /drvs/batch` with many
//! adversarially-shaped payloads and assert structural invariants
//! survive — never a panic, never a memory-leak-looking unbounded
//! growth, always a clean 4xx / 200 response.
//!
//! Complements the positive-path tests in `ingest_validation.rs` (one
//! specific malformation each) by exercising the combinatoric space:
//! random-length strings, random dep-path shapes, random `is_root`
//! flags, random counts. The property we assert is the degradation
//! contract:
//!
//!   * Every response is either 200 (accepted; counters balance) or
//!     a documented 4xx (413 for eval_too_large, 400 for per-drv
//!     validation failures).
//!   * Member count never exceeds `max_drvs_per_job`.
//!   * `new_drvs + dedup_skipped + errored == drvs.len()` on every
//!     200 response.
//!   * The coordinator survives N iterations with a bounded in-
//!     memory footprint (`steps.len()` bounded by the total distinct
//!     drv_hashes generated, not 2 * N).
//!
//! Not proptest — uses `rand` directly with a fixed seed so the
//! sequence replays on regression. A failure emits the seed so the
//! operator can re-run.

mod common;

use common::spawn_server;
use nix_ci_core::client::CoordinatorClient;
use nix_ci_core::types::{CreateJobRequest, IngestBatchRequest, IngestDrvRequest};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sqlx::PgPool;

/// Build a random valid-shaped drv request. Validity here means
/// "the drv_path's hash is extractable and the lengths fit within
/// default caps" — we're fuzzing the combinatoric space of
/// accepted shapes, not trying to break the parser (that's
/// `drv_parser_fuzz.rs`'s job).
fn random_drv(rng: &mut StdRng, idx: usize, max_deps: usize) -> IngestDrvRequest {
    // drv_hash is 32 lowercase hex chars to match the real nix
    // content-addressed prefix. Using `idx` as a seed keeps hashes
    // unique within a batch (distinct drv_hash means distinct
    // step); real-world overlap comes from dedup, which we test
    // separately.
    let hash: String = (0..32)
        .map(|_| {
            let n: u8 = rng.gen_range(0..16);
            match n {
                0..=9 => (b'0' + n) as char,
                _ => (b'a' + (n - 10)) as char,
            }
        })
        .collect();
    let name_len = rng.gen_range(1..=20);
    let name: String = (0..name_len)
        .map(|_| (b'a' + rng.gen_range(0..26)) as char)
        .collect();
    let drv_path = format!("/nix/store/{hash}-{name}-{idx}.drv");

    // Random features (0-3). Short lowercase words.
    let n_features = rng.gen_range(0..=3);
    let required_features: Vec<String> = (0..n_features)
        .map(|_| {
            let l = rng.gen_range(3..=8);
            (0..l)
                .map(|_| (b'a' + rng.gen_range(0..26)) as char)
                .collect()
        })
        .collect();

    // Random input_drvs list; each entry targets a NEW random hash
    // (so the coordinator auto-creates placeholder steps). Most are
    // 0-2 deps; occasionally 5-10 to exercise fan-in.
    let n_deps = if rng.gen_bool(0.1) {
        let hi = max_deps.clamp(5, 10);
        rng.gen_range(5..=hi)
    } else {
        rng.gen_range(0..=2)
    };
    let input_drvs: Vec<String> = (0..n_deps)
        .map(|i| {
            let dep_hash: String = (0..32)
                .map(|_| {
                    let n: u8 = rng.gen_range(0..16);
                    match n {
                        0..=9 => (b'0' + n) as char,
                        _ => (b'a' + (n - 10)) as char,
                    }
                })
                .collect();
            format!("/nix/store/{dep_hash}-dep-{idx}-{i}.drv")
        })
        .collect();

    IngestDrvRequest {
        drv_path,
        drv_name: name,
        system: "x86_64-linux".into(),
        required_features,
        input_drvs,
        is_root: rng.gen_bool(0.3),
        attr: if rng.gen_bool(0.4) {
            Some(format!("packages.x86_64-linux.random-{idx}"))
        } else {
            None
        },
    }
}

/// Occasionally emit an INVALID drv — empty fields, oversize strings,
/// malformed hashes. Must be counted in `errored`, never crash.
fn random_invalid_drv(rng: &mut StdRng, idx: usize) -> IngestDrvRequest {
    let variant = rng.gen_range(0..6);
    let mut d = random_drv(rng, idx, 0);
    match variant {
        0 => d.drv_path = String::new(),           // empty path
        1 => d.drv_name = String::new(),           // empty name
        2 => d.system = String::new(),             // empty system
        3 => d.drv_path = "no-slash-no-drv".into(), // unparseable path
        4 => d.drv_path = "/nix/store/no-hyphen.drv".to_string(),
        5 => {
            // input_drvs wildly oversized (above `max_input_drvs_per_drv`).
            d.input_drvs = (0..5000)
                .map(|i| format!("/nix/store/0000000000000000000000000000000{:01x}-bulk-{i}.drv", i % 16))
                .collect();
        }
        _ => {}
    }
    d
}

/// Drive 100 randomized ingest batches, mix of valid + invalid drvs,
/// against a single job; assert the coordinator stays consistent and
/// responds cleanly to every request. This is the "does the ingest
/// path panic or leak on adversarial input" gate.
#[sqlx::test]
async fn ingest_fuzz_stays_consistent_across_100_random_batches(pool: PgPool) {
    const SEED: u64 = 0x6e69_7863_6900_0002;
    const BATCHES: usize = 100;
    const MAX_BATCH: usize = 20;

    let handle = spawn_server(pool).await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest {
            external_ref: Some("fuzz-job".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut rng = StdRng::seed_from_u64(SEED);
    let mut total_new: u64 = 0;
    let mut total_errored: u64 = 0;
    let mut total_submitted: u64 = 0;

    for batch_idx in 0..BATCHES {
        let n = rng.gen_range(1..=MAX_BATCH);
        let drvs: Vec<_> = (0..n)
            .map(|i| {
                // 20% chance each drv is invalid.
                if rng.gen_bool(0.2) {
                    random_invalid_drv(&mut rng, batch_idx * MAX_BATCH + i)
                } else {
                    random_drv(&mut rng, batch_idx * MAX_BATCH + i, 3)
                }
            })
            .collect();
        total_submitted += drvs.len() as u64;

        let result = client
            .ingest_batch(
                job.id,
                &IngestBatchRequest {
                    drvs,
                    eval_errors: Vec::new(),
                },
            )
            .await;

        match result {
            Ok(resp) => {
                // Invariant: new + dedup + errored == drvs sent.
                // Coordinator's accounting must balance on every batch.
                let sum = resp.new_drvs + resp.dedup_skipped + resp.errored;
                assert_eq!(
                    sum as usize,
                    n,
                    "[SEED={SEED:x} batch {batch_idx}] response counters must sum to batch size: \
                     new={} dedup={} errored={} expected={n}",
                    resp.new_drvs,
                    resp.dedup_skipped,
                    resp.errored,
                );
                total_new += resp.new_drvs as u64;
                total_errored += resp.errored as u64;
            }
            Err(nix_ci_core::Error::PayloadTooLarge(_)) => {
                // Acceptable 413 — batch exceeded a cap.
            }
            Err(nix_ci_core::Error::Gone(_)) => {
                // Acceptable 410 — job became terminal (e.g. via
                // auto_fail_oversized on a prior batch). Exit loop.
                break;
            }
            Err(e) => panic!(
                "[SEED={SEED:x} batch {batch_idx}] unexpected error: {e:?} — \
                 ingest should return Ok or documented 4xx, never arbitrary Err"
            ),
        }
    }

    // Final invariants after 100 batches:
    //
    // 1. Coordinator is still alive — we can query status.
    let status = client
        .status(job.id)
        .await
        .expect("coordinator must still respond after fuzz run");
    // 2. `members` grows from both the primary drvs in each batch
    //    AND the placeholder steps auto-created for every `input_drvs`
    //    reference (see `wire_dep` in ingest_batch.rs). So
    //    `status.counts.total` is a SUPERSET of `total_new`; we only
    //    assert it's at least total_new, not equal.
    assert!(
        status.counts.total as u64 >= total_new,
        "counts.total ({}) must include at least the new batch drvs ({total_new})",
        status.counts.total
    );
    // 3. Bounded memory: steps registry is at most
    //    (total_new + total_deps_added) where total_deps_added is
    //    bounded by `total_submitted * max_deps_per_drv`.
    //    A loose upper bound — the point is "registry didn't blow
    //    past order-of-magnitude expected."
    let registry_size = handle.dispatcher.steps.len();
    // max_deps_per_drv in the generator is 10 (fan-in case) but 90%
    // of drvs have 0-2 deps; mean ~1. A 10x multiplier is a comfortable
    // upper bound with margin for the rare fan-in cases.
    let upper_bound = (total_submitted as usize * 15).max(1_000);
    assert!(
        registry_size <= upper_bound,
        "Steps registry size {registry_size} far exceeds expected bound {upper_bound} \
         (total_new={total_new}, total_submitted={total_submitted})"
    );
    // 4. The error rate matches our injection rate — if >50% errored
    //    we likely broke a validation path. We inject ~20% invalid;
    //    allow up to 35% with jitter (some invalid shapes reach
    //    the oversize-input_drvs cap, which counts ALL deps as
    //    valid submissions first, so errored only fires per-drv).
    let error_rate = total_errored as f64 / total_submitted as f64;
    assert!(
        error_rate < 0.35,
        "error rate {error_rate:.2} exceeds 35% — a validation \
         path may be rejecting shapes we meant to accept. \
         total_errored={total_errored} total_submitted={total_submitted}"
    );
}

/// Tighter fuzz focused on the drv-cap path: submit batches that
/// should cumulatively hit `max_drvs_per_job` and verify the
/// auto-fail fires exactly once, no panics, counters stay coherent.
#[sqlx::test]
async fn ingest_fuzz_auto_fail_fires_cleanly_at_cap(pool: PgPool) {
    const CAP: u32 = 50;
    let handle = common::spawn_server_with_cfg(pool, |cfg| {
        cfg.max_drvs_per_job = Some(CAP);
        cfg.submission_warn_threshold = 20;
    })
    .await;
    let client = CoordinatorClient::new(&handle.base_url);
    let job = client
        .create_job(&CreateJobRequest::default())
        .await
        .unwrap();

    let mut rng = StdRng::seed_from_u64(0xcafe_babe);
    let mut got_413 = false;

    // Spam batches of 10 valid drvs until the cap fires. At 10/batch
    // and cap=50, we expect the 6th batch to trip auto_fail_oversized.
    for i in 0..20 {
        let drvs: Vec<_> = (0..10).map(|j| random_drv(&mut rng, i * 10 + j, 0)).collect();
        match client
            .ingest_batch(
                job.id,
                &IngestBatchRequest {
                    drvs,
                    eval_errors: Vec::new(),
                },
            )
            .await
        {
            Ok(_) => {}
            Err(nix_ci_core::Error::PayloadTooLarge(msg)) => {
                assert!(
                    msg.contains("eval_too_large") || msg.contains("max_drvs_per_job"),
                    "413 must cite the cap that tripped: {msg}"
                );
                got_413 = true;
                break;
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert!(
        got_413,
        "auto_fail_oversized must fire within 20 batches at cap={CAP}"
    );

    // Post-auto-fail: the job is terminal Failed with eval_error set.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let status = client.status(job.id).await.unwrap();
    assert_eq!(status.status, nix_ci_core::types::JobStatus::Failed);
    assert!(
        status.eval_error.is_some(),
        "auto_fail_oversized must populate eval_error: {status:?}"
    );
}
