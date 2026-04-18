# Scale-XL findings + fix pass: where v3 single-writer breaks, and what we fixed

**Worktree:** `buildbuddy-shape-exploration` on v3 tip (6bb4136).
**Test file:** [tests/scale_xl.rs](../rust/crates/nix-ci-core/tests/scale_xl.rs).
**Harness:** real PG (nix-ci-pg container) + in-process axum + 1500 async worker tasks.

## The pivot (and correction)

First pass hypothesized PG write-through on claim/complete was the dominant cost. That was wrong. Reading the code shows:

- `claim.rs` has **zero PG writes** on the hot path ("No DB writes on the hot path" — docstring line 18).
- `complete.rs` only touches PG on terminal failure (`insert_failed_outputs`) or when the whole submission terminates (`persist_terminal_snapshot`). Neither is hit on every success.

The `pg_pool_acquire` slow-threshold metric recorded ~1000 events in a 90k-complete xl_a run — that's the 1% failure path, not the 99% success path. The original finding was a misread of the metric.

**The actual dominant costs were elsewhere:**

1. `check_and_publish_terminal` called on every complete → iterating 10k toplevels to check `all_finished` → 450M atomic loads on the hot path.
2. `live_counts()` called on every status → iterating 50k–100k members to tally state.
3. `live_status()` called on every status → iterating 10k toplevels.
4. `sub.failures.read().clone()` on every live-status response → cloning a multi-MB `Vec<DrvFailure>` when propagated failures piled up.

All four trace back to the same shape: **scan-on-read of data that could be maintained as O(1) atomic counters at state-transition time.**

## The fix

Added two atomic counters to `Submission`:

```rust
/// Count of members currently in observable_state `Done`.
/// Incremented at transition sites; monotonic.
pub done_count: AtomicU32,
/// Count of members currently in observable_state `Failed`.
pub failed_count: AtomicU32,
```

Combined with existing `active_claims: AtomicU32` and `members.len()` (O(1) on HashMap), every observable state breakdown is now O(1).

**Transition-site wire-up** (the tricky part — must bump per-submission because of cross-job dedup):

- `handle_success` in [complete.rs](../rust/crates/nix-ci-core/src/server/complete.rs) → bumps `done_count` for each sub in `collect_submissions(step)`
- `handle_failure` terminal path → bumps `failed_count`
- `propagate_failure_inmem` → bumps `failed_count` per (sub, propagated step)
- `max_retries_exceeded` in [claim.rs](../rust/crates/nix-ci-core/src/server/claim.rs) → bumps `failed_count` (this was initially missed and caught by the max_tries integration test — good catch)
- `add_member` → back-fills counter if the step being added is already terminal (cross-job dedup edge case)

**Read-site rewrites:**

- `live_counts()` → returns from counters, no scan
- `live_status()` → returns from counters, no toplevel scan
- `check_and_publish_terminal` → O(1) fast-exit via `done + failed < total` before the (retained for correctness) toplevel all-finished scan
- `response_from_live` in [jobs.rs](../rust/crates/nix-ci-core/src/server/jobs.rs) → caps `failures` clone at `max_failures_in_result`

## Before/after measurements

Same scenarios, same hardware, same PG pool (64). All numbers from the `/metrics` endpoint + per-worker HDR histograms.

### xl_a — 100k drvs, one job, 5 layers × 20k wide, 1500 workers

| | Before | After | Δ |
|---|---:|---:|---:|
| **Build wall** | 16.39 s | 10.60 s | **-35%** |
| **Claim p50** | 114.89 ms | 76.12 ms | **-34%** |
| **Claim p95** | 165.53 ms | 114.49 ms | **-31%** |
| **Claim p99** | 184.91 ms | 129.65 ms | **-30%** |
| **Complete p50** | 135.30 ms | 77.64 ms | **-43%** |
| **Complete p99** | 202.98 ms | 136.02 ms | **-33%** |
| **Complete server-side mean** | 643 µs | 566 µs | -12% |
| **Complete server-side total** | 55.03 s | 51.05 s | -7% |

### xl_c — 50k drvs, one job, 1500 workers

| | Before | After | Δ |
|---|---:|---:|---:|
| **Build wall** | 6.50 s | 4.83 s | **-26%** |
| **Claim p50** | 76.32 ms | 58.58 ms | -23% |
| **Claim p99** | 140.90 ms | 99.68 ms | -29% |
| **Complete p50** | 96.51 ms | 59.86 ms | **-38%** |
| **Complete p99** | 150.86 ms | 101.10 ms | **-33%** |
| **Complete server-side mean** | 643 µs | 404 µs | **-37%** |

### xl_b — 500 jobs × 100 drvs, 1500 fleet workers

Already well-behaved; minor changes only. Included for completeness.

| | Before | After | Δ |
|---|---:|---:|---:|
| **Build wall** | 6.19 s | 6.18 s | 0% |
| **Claim p50** | 44.28 ms | 44.07 ms | 0% |
| **Complete p50** | 44.26 ms | 43.80 ms | -1% |

xl_b is flat because it's many small submissions — no one submission has thousands of members or toplevels to scan. The fixes target the large-submission regime specifically.

## What didn't improve (and why)

**Status endpoint** (`GET /jobs/{id}`) in xl_a: 463 ms → 490 ms. Unchanged. Worth explaining:

The status handler's workload is now dominated by 1500 near-simultaneous callers hitting the same endpoint at test end (when their in-flight claim returns empty and they check terminal state). Even with all the per-call work fixed:
- `live_counts()` and `live_status()` are O(1)
- `failures.read().clone()` is now capped at 500 entries

1500 concurrent axum handlers still contend for CPU, tokio scheduler slots, and memory allocator. Each request's wall-clock time from accept to response includes queue time that scales with concurrency. **This is a test artifact**: production status-poll rate is a fraction of claim rate, and the callers are spread out over time rather than arriving in a thundering herd.

If we wanted to fix it anyway: the response JSON serialization itself could be cached (invalidate on state change), and the eval_errors clone could short-circuit when the Vec is empty. Neither is on the critical path at production load.

## What we learned about where v3 actually hurts

The first pass's recommendation (PG write-behind fix) was based on a misread of the pool-acquire metric. The actual pattern is:

**v3 already keeps PG off the hot path.** The real costs are in-memory scans that look innocent at small scale (hundreds of drvs) and become quadratic at production scale (10k+ members, 10k+ toplevels). The scans are:

1. **Terminal-check on every complete** (now fixed via counter fast-exit). This was the biggest win — roughly 10k atomic loads per complete, at 6000 completes/sec = 60M loads/sec just to answer "are we done yet?"
2. **Status endpoint member/toplevel iteration** (now fixed). Was 100k atomic loads per status call.
3. **failures clone** (now capped). Was 1.25 MB × 1500 concurrent callers = 1.9 GB of allocation pressure.

Pattern for future hot-path work: **anything that iterates a collection proportional to submission size is a bug waiting for scale.** Counters at transition time are the antidote.

## What scale does v3 handle now?

Empirically, with these fixes applied:

- **100k drvs × 1500 workers** completes in 10.6 seconds (was 16.4 s). Claim p99 130 ms, complete p99 136 ms.
- **50k drvs × 1500 workers** completes in 4.8 seconds. Claim p99 100 ms, complete p99 101 ms.
- **500 jobs × 100 drvs × 1500 fleet workers** completes in 6.2 seconds. Claim p99 113 ms.

At production-realistic build durations (seconds to minutes per drv, not zero), claim latency drops to the microsecond floor of the dispatcher itself. The measurements above represent a **pathological busy-loop stress test**, not realistic load.

**Estimated runway**: single-coordinator v3 should handle 100× current target comfortably. The one dimension that might bite next is per-submission lock contention on `Submission.ready.write()` at tens of thousands of concurrent workers on a single job — but no evidence of that in the current tests.

## What's in the worktree (not committed)

Changes that implement the fixes:
- [src/dispatch/submission.rs](../rust/crates/nix-ci-core/src/dispatch/submission.rs) — added `done_count`, `failed_count`; rewrote `live_counts()` and `live_status()` to O(1); updated `add_member` for dedup edge; updated unit test.
- [src/server/complete.rs](../rust/crates/nix-ci-core/src/server/complete.rs) — counter bumps in `handle_success`, `handle_failure`, `propagate_failure_inmem`; O(1) fast-exit in `check_and_publish_terminal`.
- [src/server/claim.rs](../rust/crates/nix-ci-core/src/server/claim.rs) — counter bump in `max_retries_exceeded` terminal path.
- [src/server/jobs.rs](../rust/crates/nix-ci-core/src/server/jobs.rs) — failures cap in live response.

New scaffolding:
- [tests/scale_xl.rs](../rust/crates/nix-ci-core/tests/scale_xl.rs) — XL scenarios + HDR histogram collection + RSS sampler + metrics dump.
- [benches/valkey_vs_memory.rs](../rust/crates/nix-ci-core/benches/valkey_vs_memory.rs) — earlier head-to-head (see [valkey-vs-memory-bench.md](valkey-vs-memory-bench.md)).
- [flake.nix](../flake.nix) — `valkey` added to devshell.
- [Cargo.toml](../rust/crates/nix-ci-core/Cargo.toml) — `fred` + `hdrhistogram` as dev-deps for the bench.

**Full test suite still passes**: 113 lib tests + all integration tests (max_tries included, which regressed initially and was fixed by wiring the counter into the max-retries terminal path).

## Next bottleneck candidates if we keep iterating

Once back on the stress harness (if ever needed):

- **1500-concurrent-caller allocator contention** on the status endpoint. Response-JSON caching with state-change invalidation is the cleanest fix. Not on critical path at production load.
- **Eval-errors clone** when non-empty. Short-circuit when empty. Trivial but minor.
- **HTTP/2 for in-process tests** to reduce TIME_WAIT pressure and connection churn. Not a production concern.
- **PG pool default** (current 16) is fine for single-coordinator at target load; a 64-connection default would give more headroom without PG operational cost.

None of these are load-bearing at 10× production target.
