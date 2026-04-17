# Refactor Notes

A clarity-and-maintainability pass over `rust/crates/nix-ci-core/src`.
All work is on the current branch. No public APIs, CLI flags, env vars,
wire formats, or on-disk formats changed. 314/314 tests pass; clippy
`-D warnings` clean.

## Commits

1. **`fold active_claims saturating decrement into Submission helper`**
   Three callsites (`durable::reaper::reap_expired_claims`,
   `server::claim::admin_evict_claim`, `server::complete::complete`)
   had an identical "load, if > 0 fetch_sub(1)" sequence. Extracted
   `Submission::decrement_active_claim` so the saturating behavior and
   memory ordering live in one place.

2. **`fold Instant→wall-clock deadline conversion into helper`**
   Three copies of `Utc::now() + chrono::Duration::from_std(d)
   .unwrap_or(zero)` in `server::claim` (`list_claims`,
   `extend_claim`, `issue_claim`). Extracted `wall_deadline(Duration)`
   private helper. Callers now read as intent.

3. **`extract rearm_step_if_live for claim-eviction paths`**
   The reaper's deadline sweep and the admin force-evict endpoint both
   ran the same guarded "store runnable=true, re-check finished, undo
   or enqueue" sequence. Extracted
   `dispatch::rdep::rearm_step_if_live` — invariant-4 guard now lives
   in one place.

4. **`fold terminal-snapshot serialize+persist into writeback helper`**
   Five callsites (seal-time auto-fail, user `/fail`, user `/cancel`,
   graceful terminal in `check_and_publish_terminal`, oversized-
   ingest auto-fail) each built a `JobStatusResponse`, serialized it,
   and called `writeback::transition_job_terminal`. Added
   `writeback::persist_terminal_snapshot(pool, id, &snapshot)` so the
   serialize-then-write contract lives in one place. Error message on
   the (virtually unreachable) serialize failure unified to "serialize
   terminal snapshot"; the previous per-site variants only ever hit
   tracing logs because 5xx bodies are already sanitized.

5. **`fold per-job + fleet claim client into shared send_claim`**
   `CoordinatorClient::claim_as_worker` and `claim_any_as_worker` were
   identical except for the URL path. Added private
   `send_claim(url, ...)` helper; the two public entry points now
   differ only in URL construction.

6. **`fold claim_any main + timeout fleet scan into shared helper`**
   `server::claim::claim_any` had two copies of its "walk every live
   submission in schedule order, skip terminal + worker-capped,
   pop_runnable, terminal-fail-if-exhausted, else issue_claim" scan —
   one in the long-poll main loop, one in the post-timeout one-last-
   sweep arm. Extracted `scan_fleet_once` returning a three-way
   `FleetScan` outcome. Preserves the nuance that the main loop
   retries on `RestartScan` while the timeout arm just returns 204.

## Net impact

```
 rust/crates/nix-ci-core/src/client/http.rs         | -19 +21
 rust/crates/nix-ci-core/src/dispatch/rdep.rs       |    +24
 rust/crates/nix-ci-core/src/dispatch/submission.rs |    +12
 rust/crates/nix-ci-core/src/durable/reaper.rs      | -20 +6
 rust/crates/nix-ci-core/src/durable/writeback.rs   |    +17
 rust/crates/nix-ci-core/src/server/claim.rs        | -96 +90
 rust/crates/nix-ci-core/src/server/complete.rs     | -21 +3
 rust/crates/nix-ci-core/src/server/ingest_batch.rs | -10 +2
 rust/crates/nix-ci-core/src/server/jobs.rs         | -30 +8
```

Roughly -100 lines net across the touched source, every new helper
documented.

## Behavioral edge cases noticed but NOT fixed

1. **`ingest_batch::auto_fail_oversized` does not record
   `drvs_per_job`** while the three other forced-terminal paths
   (`jobs::finish_in_memory` and
   `complete::check_and_publish_terminal`) do. This may be
   intentional (an over-cap job has very few members stored, which
   would skew the histogram toward 0) or an oversight. Preserved
   as-is. See
   [rust/crates/nix-ci-core/src/server/ingest_batch.rs:368](rust/crates/nix-ci-core/src/server/ingest_batch.rs:368).

2. **`server::claim::list_claims` casts `u128 → u64` via
   `min(u128::from(u64::MAX)) as u64`**. Cosmetically awkward; could
   be `min(u64::MAX as u128) as u64`. Harmless. See
   [rust/crates/nix-ci-core/src/server/claim.rs:290](rust/crates/nix-ci-core/src/server/claim.rs:290).

3. **`validate_worker_id` exists in two variants** —
   `server::claim::validate_worker_id` (takes `&Option<String>`,
   accepts None/empty) and `server::ops::validate_worker_id` (takes
   `&str`, rejects empty). Different contracts (claim allows missing
   worker id; fence requires it), so fusing them needs thought.

## Follow-ups recommended but not done

- **`CreateJobRequest` scheduling options** (priority, max_workers,
  claim_deadline_secs) could be wrapped in a
  `JobSchedulingOptions` struct to shrink the `with_options` call
  signature. Scope creep for this pass; worth doing before adding a
  fifth option.

- **`runner::drv_parser::Parser` has four very similar `parse_*_list`
  loops** (`parse_outputs`, `parse_input_drvs`, `parse_string_list`,
  `parse_env`) with different tuple arities. A macro or a
  closure-based `parse_bracketed_list` helper could collapse them,
  but the tuple-shape variation makes the abstraction awkward — each
  call would need its own inner closure and a reader of the extracted
  helper still has to understand the outer loop. Judged not worth it.

- **`claim_any` and `claim` (per-job) still share a long-poll
  skeleton** (loop { scan; on deadline retry once }). The per-job
  variant scans a single submission, not the whole fleet. Extracting a
  shared generic long-poll would require closures / trait objects
  over both "one sub" and "all subs" — loss of readability exceeds
  the dedup gain.

- **`runner::submitter::BoundedCache::reconcile`** is O(n) per borrow
  because the walker mutates `inner` directly and we can't observe
  writes. The reconciliation is load-bearing for memory bounds and the
  existing test
  `bounded_cache_reconcile_is_linear_at_scale` enforces the intended
  complexity. A redesign (e.g., the walker exposing its own insertion
  log) could make reconcile free; not urgent.

## Verification

```
$ cargo clippy --all-targets -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 2.33s

$ cargo test --all-targets
# summed across 40 test binaries:
passed=314 failed=0 ignored=1
```

The single `ignored` test is the pre-existing
`parse_all_store_drvs` `#[ignore]` harness for scanning a live `/nix/store`.
