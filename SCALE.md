# Scale & reliability testing strategy

This document captures the five-layer strategy the coordinator uses to
prove it scales to production traffic and degrades gracefully when
the environment underneath it doesn't. It complements [SPEC.md](SPEC.md)
(exit criteria) by describing *how* we get each bar green, and the
**degradation contract** that tells operators what the coordinator
promises under overload.

## Layered strategy

| Layer | What it catches                                                        | Where it lives                              | Per-PR? |
|-------|------------------------------------------------------------------------|---------------------------------------------|---------|
| **L1** | Invariant logic under adversarial thread schedules                     | `crates/nix-ci-loom-checks/tests/invariants.rs` | ✅      |
| **L2** | Event-ordering bugs on the **real** dispatcher (seed-replayable)       | `crates/nix-ci-core/tests/sim.rs`           | ✅ (200 seeds); 10 000 nightly |
| **L3** | Latency / throughput regressions at 1000+ workers, 20K+ drvs           | `crates/nix-ci-core/tests/{scale,load}.rs`  | Nightly + main only |
|        | Memory-bound cleanup (claims map, steps registry, fleet scan)          | `crates/nix-ci-core/tests/memory_bounds.rs` | ✅      |
| **L4** | Degradation under PG outage + worker flood                             | `crates/nix-ci-core/tests/{degradation,pg_faults}.rs` | ✅ |
| **L5** | Production shadow + canary rollout                                      | External (staging + CCI mirror)             | Continuous |

The layers compose: a bug that passes L1+L2 but fails L3 is a scale
regression, not a correctness regression; a bug that passes L3 but fails
L4 means the critical path is fast but the degradation behavior is off.

### L1 — loom concurrency model checkers

Lives in the isolated `nix-ci-loom-checks` crate so `--cfg loom` doesn't
propagate to transitive deps (`concurrent-queue` has incompatible loom
branches). Each test reimplements the state machine of one invariant in
miniature using loom's `sync::*` / `atomic::*` drop-in replacements;
loom exhaustively explores every interleaving.

**Drift caveat**: model tests can pass while the real code diverges. The
*real-code* concurrency gates are TSan (`tests/property.rs`), the
deterministic sim (`tests/sim.rs`), and chaos (`tests/chaos.rs`). Loom
here is **executable contract documentation**, not the primary gate.

Currently covered: invariants 1 (Steps dedup), 3 (CAS-exactly-once),
4 (created-before-runnable) + H7 reservation-race.

```sh
cd rust && RUSTFLAGS="--cfg loom" \
    cargo test -p nix-ci-loom-checks --release -- --test-threads=1
```

### L2 — deterministic simulation

The highest-leverage layer. A seed-replayable event harness drives the
**real** dispatcher types (`Dispatcher`, `Submission`, `Step`,
`Claims`, `StepsRegistry`, `make_rdeps_runnable`, `pop_runnable`)
through randomized but reproducible schedules of ingest / seal / claim /
complete / fail / cancel events. After every event, four global
invariants are checked:

1. Claims-map size matches tracked issued claims.
2. No step is simultaneously `runnable && finished`.
3. Every live claim points to a resolvable step.
4. `Submission::active_claims` matches the subset of claims for that
   submission.

A failure emits the seed; the same seed reproduces the trajectory
exactly. This is the TigerBeetle / FoundationDB playbook and the
single biggest lever for "no production bug we didn't catch in sim."

Per-PR: 200 seeds × 2000 events = 400 000 events, ~1 s wall clock.
Nightly: 10 000 seeds × 3000 events = 30 000 000 events, ~90 s.

**Deferred**: the sim is single-threaded by design; true data races are
caught by L1 (loom) + TSan. Multi-threaded deterministic simulation
(e.g., via `madsim`) is a future investment.

```sh
cargo test -p nix-ci-core --features sim-test --test sim --release \
    -- --nocapture
SIM_SEEDS=10000 cargo test -p nix-ci-core --features sim-test \
    --test sim --release -- --nocapture
```

### L3 — scale ladder on real code + Postgres

Five shapes, each probing a specific bottleneck:

| Test                                  | Shape                                                       | Stresses                                |
|---------------------------------------|-------------------------------------------------------------|-----------------------------------------|
| `scale_1k_workers_fan_in`             | 5K drvs, 5-layer fan-in, 1000 workers                       | `notify_waiters` wake, claims-map write |
| `scale_tall_narrow_chain`             | 2000-deep linear chain, 32 workers                          | rdep propagation critical-path latency  |
| `scale_wide_flat_leaves`              | 20K independent drvs, 1000 workers                          | claim-path contention extreme           |
| `scale_100_submissions_fleet`         | 100 subs × 10 drvs, 256 fleet workers                       | `sorted_by_created_at` scan cost        |
| `scale_failures_vec_under_catastrophic_job` | 5K drvs × 100% failure                                | `failures` vec + JSONB size bounds      |

Each asserts a latency budget (p99 claim < 500ms–1s depending on
shape) and a runtime budget (< 180s per test). A 10× regression trips
these immediately; a 2× regression trips in a follow-up run with
tightened budgets.

Plus memory-ceiling probes (`tests/memory_bounds.rs`) that run per-PR:

* `claims_map_drains_after_all_completes` — no claim leak post-terminal.
* `steps_registry_gcd_after_submission_drop` — weak-ref GC is real.
* `fleet_scan_cost_sublinear_at_1k_subs` — single claim < 200ms at
  1000 live submissions.
* `ready_deques_drain_on_cancel` — cancelling a submission frees
  claim, member, and ready-queue memory.

```sh
DATABASE_URL=postgres://... cargo test -p nix-ci-core --features scale-test \
    --test scale --release -- --test-threads=1 --nocapture
```

### L4 — degradation contract

See [Degradation contract](#degradation-contract) below. Tests:

* `tests/degradation.rs::claim_shedding_at_threshold` — 503 + Retry-After.
* `tests/degradation.rs::active_claims_complete_despite_shedding` —
  shedding new claims doesn't starve existing ones.
* `tests/pg_faults.rs::claim_path_live_under_pool_exhaustion` — pool
  exhaustion doesn't stall in-memory hot path.
* `tests/pg_faults.rs::coordinator_survives_database_statement_timeout`
  — degraded PG produces bounded-time clean errors, never hangs.

### L5 — production shadow + canary (external)

* **Shadow**: tee a slice of real CCI traffic into a staging coordinator;
  compare outcomes (ground truth = existing CCI path).
* **Canary**: 1 % → 10 % → 50 % → 100 %, with automated rollback on
  SLO breach (claim p99, completion rate, 5xx rate).
* Prometheus alerts align with SPEC bars (claim-p99 > 200ms for 5min,
  pg_pool_idle=0 for 30s+, RSS growth rate, `overload_rejections`).

## Degradation contract

These are the formal commitments the coordinator makes under stress.
Each line is an **assertion** (not a goal): a CI test enforces it.

| Condition                                        | Claim (per-job & fleet) | Non-terminal `complete` | Terminal `complete` (writeback) | Ingest |
|--------------------------------------------------|-------------------------|-------------------------|--------------------------------|--------|
| Healthy PG + worker count ≤ threshold            | OK (p99 < 200ms)        | OK (in-memory)          | OK (PG write)                  | OK     |
| PG pool saturated (all 16 conns held)            | **OK** (no PG touch)    | **OK** (no PG touch)    | Blocks until `acquire_timeout=10s`, then 5xx | Ingest blocks on `failed_output_hits`; degrades to no-hits |
| `claims_in_flight` ≥ `max_claims_in_flight`      | **503 + Retry-After: 1** (clean shed) | OK                      | OK                             | OK     |
| Submission-member cap (`max_drvs_per_job`) exceeded | OK                    | OK                      | N/A                            | **413 + `eval_too_large`** |
| `failures` vec > `max_failures_in_result`        | OK                      | OK                      | Terminal JSONB truncated with marker | N/A |
| Coordinator SIGKILL mid-build                    | New calls see 410 (job gone) | 410 (job gone)          | Non-terminal → `clear_busy` cancels on restart | N/A |
| **Panic in handler** (catch_panic_layer)         | **500 + sanitized body** (connection stays up) | **500 + sanitized body** | **500 + sanitized body**     | **500 + sanitized body** |
| **Fleet worker ≥ threshold failures in window** | **204 on fleet claim** (auto-fenced for cooldown; per-job claims unaffected) | OK | OK | OK |
| **Claim extended past `max_claim_lifetime_secs`**| N/A (ceiling on existing claim) | N/A            | N/A                            | N/A (step re-armed on reap) |
| **`input_drvs.len()` > `max_input_drvs_per_drv`**| OK                      | OK                      | OK                             | **Drv skipped, `errored` incremented** |
| **`eval_errors.len()` > `max_eval_errors_per_batch`** | OK                 | OK                      | OK                             | **413 + cap-cite message** |
| **Worker reports exit 137/143/124**              | N/A (worker-side classification) | OK (Transient, re-armed) | N/A                         | N/A |

**Key invariants across all conditions:**

1. **Claim and non-terminal complete are pure in-memory.** Under PG
   outage they stay live; under claim-shedding they see 503, not a
   hang. Workers interpret both cleanly (503 → back off; 410 →
   submission gone).
2. **Shedding is a clean rejection, not a hang.** 503 + Retry-After: 1
   lets clients retry with fresh backoff. Existing claims continue.
3. **Memory stays bounded**: every unbounded-growth risk has an
   explicit cap (`max_drvs_per_job`, `max_failures_in_result`,
   `EVAL_ERRORS_CAP`, `max_input_drvs_per_drv`,
   `max_required_features_per_drv`, `max_eval_errors_per_batch`) +
   regression test.
4. **Failure is idempotent**: SIGKILL, PG drop, worker vanish all
   converge on a consistent state; callers retry; no partial writes.
5. **Only broken builds break builds.** The classifier's exit-code
   + stderr rule never converts an external-kill (137/143/124) or
   an unknown stderr × exit code into a terminal `BuildFailure`.
   The per-worker quarantine contains a single sick host before
   its failures spread across many drvs.
6. **Panics are contained.** The innermost axum layer catches
   panics and converts them to sanitized 500s, so no malformed
   submission can crash the coordinator or silently drop
   connections. `nix_ci_process_panics` counter + global panic
   hook surface the original payload to operators.

## Observability instrumentation

New histograms added to unblock diagnosis at scale:

| Metric                                        | What it tells you                                                 |
|-----------------------------------------------|-------------------------------------------------------------------|
| `nix_ci_fleet_scan_duration_seconds`          | `Submissions::sorted_by_created_at` cost under submission growth  |
| `nix_ci_rdep_propagation_duration_seconds`    | `make_rdeps_runnable` cost under fan-out                          |
| `nix_ci_pg_pool_acquire_duration_seconds`     | Pool starvation leading indicator                                 |
| `nix_ci_ingest_phase_duration_seconds{phase}` | Per-phase ingest attribution (parse / reserve / attach / enqueue) |
| `nix_ci_lock_wait_seconds{site}`              | Reserved for future lock-site instrumentation                     |
| `nix_ci_overload_rejections_total`            | Shedding rate (contract firing, not a bug)                        |

## Running it all

```sh
# Per-PR surface (fast, everything must be green):
cargo test -p nix-ci-core --tests
cargo test -p nix-ci-core --features sim-test --test sim --release
RUSTFLAGS="--cfg loom" cargo test -p nix-ci-loom-checks --release -- --test-threads=1

# Nightly (heavy):
SIM_SEEDS=10000 cargo test -p nix-ci-core --features sim-test --test sim --release
cargo test -p nix-ci-core --features scale-test --test scale --release -- --test-threads=1
cargo test -p nix-ci-core --features chaos-test --test chaos --release

# Single-seed sim replay (on failure):
# Loop over seeds 0..N and re-run the failing index.
```

## Sequencing (recommended)

1. **Land what's here** — L1 harnesses, L3 scale + memory-bounds tests,
   L4 degradation contract with the new `max_claims_in_flight` shed,
   L2 sim at 200 seeds/PR + 10 000/nightly, observability instrumentation,
   wired CI. Everything in this repo is mergeable.
2. **Tune degradation thresholds** per production observation. The
   default `max_claims_in_flight=None` means shedding is off until an
   operator opts in.
3. **Add shadow mode** — tee real traffic to a staging coordinator; this
   is an integration, not a code change.
4. **Canary with rollback** — wire SLO-driven automated rollback via
   whatever deployment system operates the coordinator fleet.
5. **Extend L2 to multi-threaded sim** (e.g., via `madsim`) if L1 + TSan
   don't cover a specific race class that shows up in production.

## What "100% confidence" actually means here

It means **every class of failure we've articulated has a named test
that fires before production sees it**. No guarantee of zero bugs —
nothing gives you that. But every known risk surface — correctness,
latency tails, unbounded memory, PG degradation, worker overload —
has a test, a budget, and a CI gate.

If a production incident reveals a failure we didn't articulate, the
first follow-up is "add a test to the matching layer." This is how the
gate tightens over time. The 30-day green-nightly requirement in
[SPEC.md](SPEC.md) T-NIGHTLY-GREEN is the outer boundary condition
that keeps the whole strategy honest.
