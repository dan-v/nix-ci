# Refactor Triage Scratch

Baseline: 314 tests pass (integration + unit), clippy -D warnings clean.

## Findings, prioritized

### a) Dead code / unused
- `DrvOutput` struct in drv_parser.rs is `#[allow(dead_code)]` annotated but
  `name`/`hash_algo`/`hash` fields are parsed+stored and fed into `is_fod()`
  via `outputs[0].hash.is_empty()`. Verified — all struct fields are written
  but only `hash` is read. This is legitimate: the parser captures them for
  future use and the allow is an explicit WIP marker. LEAVE (low risk).
- `ServerHandle.pool` / `.dispatcher` are `#[allow(dead_code)]` on the test
  harness because not every test uses them. Test code, leave.
- No production dead code found.

### b) Duplicated logic
1. **`sub.active_claims` saturating-decrement** — 3 callsites:
   - durable/reaper.rs:182-188
   - server/claim.rs:249-255 (admin_evict_claim)
   - server/complete.rs:69-78 (complete)
   All identical: load `active_claims`, if > 0 fetch_sub(1). Extract
   `Submission::decrement_active_claim()` as a method. **FIX NOW.**

2. **Step re-arm on evict/reaper** — 2 near-identical callsites:
   - durable/reaper.rs:189-208
   - server/claim.rs:260-270 (admin_evict_claim)
   Pattern: take claim, if step exists and !finished, store runnable=true,
   re-check finished (race guard), undo or enqueue. Extract
   `rearm_step_if_live(dispatcher, drv_hash)` in `dispatch::rdep`.
   **FIX NOW.**

3. **Fleet vs per-job claim long-poll** — server/claim.rs::claim +
   claim_any share the structure: loop { scan; on timeout retry once }.
   Different scan shape makes a clean helper awkward — the per-job code
   only scans ONE submission, fleet scans ALL. Extracting would require
   closures/generics that make both sites harder to read. **LEAVE.**
   (The "retry-once-on-timeout" sub-pattern is a candidate but would
   only save ~6 lines per site with worse readability.)

4. **`auto_fail_on_seal` (jobs.rs:139) vs `auto_fail_oversized`
   (ingest_batch.rs:368)** — both build a JobStatusResponse with
   `eval_error=Some(reason)`, call `writeback::transition_job_terminal`,
   remove submission, evict_claims_for, mark_terminal, publish
   JobDone, bump `jobs_terminal` counter. Only `sealed` differs (true vs
   false) and `auto_fail_on_seal` relies on `finish_in_memory` helper.
   Extract a shared `force_fail_job(state, id, reason, sealed)` in
   complete.rs. **FIX NOW.**

5. **`CoordinatorClient::claim_as_worker` vs `claim_any_as_worker`**
   — identical except URL path. Extract a private helper
   `do_claim(url, ...)`. **FIX NOW.** (~30 lines saved, no readability loss.)

6. **Wall-deadline construction** — 3 copies of the pattern
   `Utc::now() + chrono::Duration::from_std(remaining).unwrap_or(zero())`.
   Extract `wall_deadline(remaining: Duration) -> DateTime<Utc>` private
   helper. **FIX NOW.**

### c) Functions over ~50 lines / cyclomatic > 10
- `server::claim::claim` (~45 lines handler, inline retry on timeout).
  Acceptable; select! bodies inflate linecount.
- `server::claim::claim_any` (~70 lines). Two copies of the same
  inner scan — consolidating halves its size (see dedup #3 above
  — I reconsidered this; an internal closure is worth it here).
  **Actually FIX**: extract `scan_all_submissions_for_claim` helper
  shared by both the main and timeout re-scan path.
- `ingest_batch::submit_batch` (~215 lines w/ comments). Has 3 clearly
  labeled phases that are already well-commented. Splitting further
  would add ceremony without reducing complexity. **LEAVE.**
- `server::complete::complete` handler (~75 lines) — linear flow, no
  nesting. **LEAVE.**
- `runner::worker::run` (~140 lines) — main poll/claim/spawn loop; split
  claim error handling into helper is plausible but already well-
  labeled. **LEAVE.**
- `runner::worker::build` (~100 lines, heavy select!) — fine.
- `submission::detect_cycle` — iterative DFS, ~80 lines including color
  enum. Tight; **LEAVE**.

### d) Deep nesting
- No offenders over 3 levels outside of tokio `select!` arms (which is
  unavoidable). LEAVE.

### e) Anti-patterns / deprecated APIs
- None detected. Clippy with `-D warnings` is clean.

### f) Inconsistent naming / error handling / logging
- `validate_worker_id` exists twice with slightly different behavior
  (claim.rs takes Option<String>, ops.rs takes &str). Fine — distinct
  call contracts (fence requires non-empty, claim allows None).
- Logging conventions look consistent: tracing::warn/info/error with
  structured fields. LEAVE.

## Out-of-scope observations (not fixing)

- `server/claim.rs::list_claims` casts `u128 -> u64` via `min(u128::from(u64::MAX))`.
  Harmless but awkward. Could use `as u64` with `min(u64::MAX as u128)` — cosmetic.
- `runner/submitter.rs::BoundedCache` reconcile pattern works but is O(n)
  per borrow. The test `bounded_cache_reconcile_is_linear_at_scale`
  enforces the intended complexity. LEAVE.
- `CreateJobRequest` has several "Option<u32>" fields that each require
  `#[serde(default)]`. Wrapping them in a `JobSchedulingOptions` struct
  would be cleaner; scope creep, LEAVE.

## Plan of record

Six commits in order of risk (lowest first):

1. `Submission::decrement_active_claim` helper — 3 callsites.
2. `wall_deadline` private helper in `server::claim` — 3 callsites.
3. `dispatch::rdep::rearm_step_if_live` helper — 2 callsites (reaper,
   admin evict).
4. `server::complete::force_fail_job` helper replacing both
   `auto_fail_on_seal` and `auto_fail_oversized`.
5. `CoordinatorClient` shared `do_claim` private helper.
6. `server::claim` shared fleet-scan closure between main-loop and
   timeout-rescan.

After each commit: `cargo test --all-targets` + `cargo clippy --all-targets
-- -D warnings` must pass.
