# nix-ci production-readiness spec

Quantitative exit criteria. Each bar is a binary pass/fail, verified by a
named test or measurement. "Done" = every bar green for 30 consecutive
days of nightly chaos-scale + property runs without a violation.

## Correctness (must all be green)

- **C-CORRECT-1**: Under `chaos` at `iters=100 jobs=200 workers=64 DAG=40`,
  zero dispatcher-invariant violations (the 8 invariants in
  `dispatch/mod.rs`). Measured by `property.rs` assertions.
- **C-CORRECT-2**: No drv ever reported failed when the underlying
  `nix build` exited 0. Measured by a synthetic build harness that
  records worker-observed exit code vs. coordinator-reported outcome.
- **C-CORRECT-3**: No drv ever silently dropped from a sealed,
  non-terminal job. Measured by `edge_cases::drvs_accounted_after_seal`
  (to be added).
- **C-CORRECT-4**: Cyclic dep graph fails the job cleanly with a
  named cause; never hangs. Measured by `edge_cases::cycle_fails_job`.
- **C-CORRECT-5**: Ingest that exceeds the per-job drv cap fails the
  job at ingest with `eval_too_large`, never OOMs. Measured by
  `edge_cases::oversized_ingest_rejects_cleanly`.
- **C-CORRECT-6**: Under PG fault injection (toxiproxy: random drop,
  1s delay, reject 10% of queries), zero terminal-state corruption.
  Measured by `pg_faults.rs` (to be added).

## Durability & recovery

- **D-RESTART-1**: Coordinator SIGKILL mid-ingest → restart → caller
  retries → new job completes. Measured by `resilience::sigkill_mid_ingest`.
- **D-RESTART-2**: Coordinator SIGKILL mid-complete → worker's retry
  either succeeds or observes `ignored:true`. Never double-issued claim.
  Measured by `resilience::sigkill_mid_complete`.
- **D-RESTART-3**: Coordinator SIGKILL mid-terminal-write → on restart,
  job has consistent state (either still pending or fully terminal with
  matching sentinel). Measured by `resilience::sigkill_mid_terminal`.
- **D-STARTUP-1**: Startup with 10K rows of pre-existing `jobs` and
  `failed_outputs` data completes in < 5s. Measured by `startup_bench`.

## Scale / latency (at production quiescence)

- **S-CLAIM-P99**: Under 10K-drv / 500-worker load, p99 claim latency
  < 200ms. Measured by `load.rs`, assert-enforced.
- **S-CLAIM-P50**: Same load, p50 < 20ms.
- **S-INGEST-THR**: Coordinator sustains ≥ 10K drvs/sec ingest throughput
  single-node. Measured by `load::ingest_throughput`.
- **S-MEM-10K**: 10K drvs × 100 concurrent submissions uses < 2 GiB RSS.
  Measured by `load::memory_budget` using `/proc/self/status` + metric.
- **S-WAKE-FLEET**: Fleet-claim wake-up of 1000 workers on 100
  submissions costs < 200ms total CPU. Measured by `load::fleet_wake`.
- **S-CLAIM-1K**: Under a 1000-worker, 5K-drv fan-in DAG, p99 claim
  latency < 500ms and runtime < 180s. Measured by
  `scale::scale_1k_workers_fan_in`.
- **S-CHAIN-DEPTH**: A 2000-deep linear dep chain terminates in
  < 180s (stresses `make_rdeps_runnable` critical path). Measured by
  `scale::scale_tall_narrow_chain`.
- **S-WIDE-CLAIM**: 20 000 independent leaf drvs with 1000 workers
  complete in < 180s with p99 < 1s. Measured by
  `scale::scale_wide_flat_leaves`.
- **S-FLEET-SCAN**: A single fleet-claim scan across 1000 live
  submissions completes in < 200ms. Measured by
  `memory_bounds::fleet_scan_cost_sublinear_at_1k_subs`.
- **S-SIM-SEEDS**: 10 000 nightly simulator seeds × 3000 events each
  produce zero invariant violations. Measured by
  `sim::sim_multiple_seeds_all_green` with `SIM_SEEDS=10000`.

## Observability

- **O-METRIC-PARITY**: For every terminal transition, exactly one
  `jobs_terminal{status=<x>}` counter increment. Verified by
  `observability::metric_parity`.
- **O-CLAIMS-INFLIGHT**: Under chaos-scale churn, `claims_in_flight`
  gauge converges to 0 within 5s of quiescence. Measured by
  `observability::gauge_converges`.
- **O-TRACE-E2E**: A single job's trace has spans from submitter →
  coordinator ingest → claim → complete, all under one root trace_id.
  Spot-checked via OTel Collector integration.

## Degradation (the "won't break production" bar)

- **R-SHED-CLEAN**: With `max_claims_in_flight=N`, the (N+1)-th
  concurrent claim returns HTTP 503 + `Retry-After: 1` within 100ms
  (no hang). Existing in-flight claims continue unaffected. Measured
  by `degradation::claim_shedding_at_threshold` and
  `degradation::active_claims_complete_despite_shedding`.
- **R-POOL-EXHAUST**: With every Postgres pool slot held, claim and
  non-terminal `complete` round-trips stay < 1s. Measured by
  `pg_faults::claim_path_live_under_pool_exhaustion`.
- **R-PG-DEGRADED**: With `statement_timeout=1ms` on the coordinator's
  database, every API call either succeeds or returns a bounded-time
  clean error (< 15s). No hangs. Measured by
  `pg_faults::coordinator_survives_database_statement_timeout`.
- **R-TERMINAL-JSONB-BOUNDED**: A catastrophic-failure job (every
  drv fails) produces a terminal `jobs.result` JSONB under 1 MiB,
  and `failures` is truncated at `max_failures_in_result` + a
  synthetic marker. Measured by
  `scale::scale_failures_vec_under_catastrophic_job`.
- **R-CLAIMS-NO-LEAK**: After every terminal transition, the
  in-memory claims map drops to 0 entries for the affected job.
  Measured by `memory_bounds::claims_map_drains_after_all_completes`.

## Deployability

- **P-NIXOS-COORDINATOR**: `nixosTest` spins up the coordinator module,
  polls `/healthz`, submits a job, sees it complete. Required green
  under `nix flake check`.
- **P-NIXOS-WORKER**: Same, for the worker module (once added).
- **P-BEARER-AUTH**: With `auth.bearer` enabled, unauthenticated requests
  receive 401; authenticated requests succeed. Measured by
  `http_auth::unauthorized_rejected`. (TLS is terminated by a layer in
  front of the coordinator and is explicitly out of scope here — the
  coordinator speaks plain HTTP / h2c.)
- **P-UPGRADE-SAFE**: Rolling deploy (coordinator N → N+1) does not
  corrupt any jobs.result or failed_outputs row that predates the
  deploy. Measured by `resilience::rolling_upgrade` (fixture DB dump
  → coordinator N+1 boot → integrity check).

## Process

- **T-CI-GREEN**: `main` is green for 30 consecutive days (`ci.yml`).
- **T-NIGHTLY-GREEN**: `chaos-scale` nightly is green for 30 days.
- **T-MIRI-GREEN**: `miri` job green on every PR.
- **T-TSAN-GREEN**: `tsan` job green on every PR.
- **T-DEP-AUDIT**: `cargo deny check` (or equivalent) runs on every PR
  and blocks on unaudited vulnerabilities.

## Hygiene

- No `TODO`, `FIXME`, or `XXX` in committed code without a matching
  issue link.
- Every doc-comment on a public item is accurate to the v3 design
  (no v2 language drift).
- Every dispatcher invariant has at least one failing-test-first
  regression guard.

## Known deferred

- **Claim-handoff on coordinator restart (C6)**: the v3 contract is
  "restart = cancel all in-flight jobs, caller retries." This means a
  30-min build that's 29 min in still re-runs from scratch on a
  redeploy. A v3.5 design could persist claim records to PG so
  workers' `/complete` calls succeed across a restart, but rehydrating
  the step graph is incompatible with the ephemeral-dispatcher
  position. For now: document the cost, set `graceful_shutdown_secs`
  generously, and route redeploys through a quiet window.

## Scoring

Run `scripts/spec_report.sh` to print a green/red table of every bar.
A single red bar means not-production-ready.

```
./scripts/spec_report.sh          # run every bar's test, color-coded output
./scripts/spec_report.sh --no-run # dry-run: just show the mapping
SPEC_REPORT_SKIP=S-MEM-10K,P-NIXOS-WORKER ./scripts/spec_report.sh
```

Exits 0 when every measured bar is green; 1 if any is red. "Done"
per the § Exit Criteria requires green for 30 consecutive days of
the nightly chaos-scale job (`T-NIGHTLY-GREEN`).
