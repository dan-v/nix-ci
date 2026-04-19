# Changelog

All notable changes to the `nix-ci` workspace. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/). Versioned per the
workspace `version` (currently `0.2.0`, unpublished).

## [Unreleased] — Production-readiness pass (April 2026)

Thematic, not strict-chronological. Twelve commits covering
correctness fixes that close real bugs, production polish that
turns documented defaults into enforced ones, HA plumbing that
takes the advisory-lock failover mechanism from "exists in code"
to "tested at three layers (in-process, nixosTest, orbstack
real-TCP + SIGKILL)", shadow-mode tooling, and test hardening.

### Fixed — correctness
- **Terminal-write wedge retry sweep.** A `/complete` crossing the
  last-drv-of-job boundary while PG is unreachable returned 500 to
  the worker AFTER removing the claim in memory. The worker's
  subsequent retries saw `ignored:true` and gave up; the
  submission sat "all members finished, !terminal, no jobs.result
  row" forever until heartbeat reap turned a successful build into
  a cancelled one. New background sweep (on `reaper_interval_secs`)
  retries `check_and_publish_terminal` on wedged submissions. New
  metric `nix_ci_terminal_writeback_retry_finalized_total`.
  Guarded by three tests in `resilience.rs`.
- **Axum graceful drain bounded.** `axum::serve(..).with_graceful_shutdown(..)`
  is unbounded by default — a pinned SSE subscriber kept SIGTERM
  hanging until systemd SIGKILLed. Serve task now runs in a
  dedicated tokio task with a `tokio::time::timeout` on the
  join; abort_handle fires on timeout. Default
  `graceful_shutdown_secs` bumped 30s → 60s to cover the
  longest legitimate long-poll (`max_claim_wait_secs`).
- **`cap_failures` strips `log_tail` past the triage head.** The
  `R-TERMINAL-JSONB-BOUNDED` < 1 MiB bar was only passing because
  the scale test used `log_tail: None`. Real workers attach up to
  64 KiB per failure; at the 500-failure cap a catastrophic job
  could produce 32 MiB JSONB. New constant
  `MAX_LOGTAILS_IN_RESULT = 10` — first 10 failures keep tails,
  rest are stripped to `None`. Full bytes still in `build_logs`.
- **Claim long-poll re-checks `draining` inside the loop.** An
  in-flight claim that was already waiting when `/admin/drain`
  fired could issue a new claim after the drain flag was set
  if a step became runnable via `notify_waiters`. Now checks at
  every loop iteration after wake.
- **All terminal-write paths route through `_observed` writeback.**
  Four terminal paths (`/fail`, `/cancel`, `auto_fail_on_seal`,
  `auto_fail_oversized`) bypassed `pg_pool_acquire_duration_seconds`,
  making pool starvation invisible on those endpoints.
- **Overload shedding and auto-quarantine enabled by default.**
  `max_claims_in_flight` (previously `None`) now defaults to
  `Some(25_000)`; `worker_quarantine_failure_threshold`
  (previously `None`) now defaults to `Some(5)` with existing
  300s window + 900s cooldown. Both remain Option so explicit
  `null` preserves the pre-default opt-out.
- **Drain rejects further ingest on existing jobs.**
  `/admin/drain` previously only blocked new job creation +
  claims; existing submissions could keep adding drvs. A
  streaming submitter would chase the operator's
  `in_flight_claims -> 0` convergence poll with a moving target.
- **`POST /admin/refute` accepts `worker_id`.** Schema migration
  `0006_failed_outputs_worker_id.sql` adds a nullable `worker_id`
  column; `insert_failed_outputs` records it on BuildFailure;
  `/admin/refute?worker_id=X` bulk-deletes entries from a
  suspected-sick worker in one call.

### Added — new signals
- **`nix_ci_terminal_writeback_retry_finalized_total`** counter
  (see wedge retry above).
- **`nix_ci_build_logs_byte_ceiling_prunes_total`** counter for
  the new `max_build_logs_bytes` disk-guard sweep (default 50
  GiB; kicks in after time-based retention).
- **`nix_ci_build_log_bytes_per_job`** histogram — cumulative
  gzipped log bytes per submission, observed at terminal.
- **`nix_ci_submission_log_bytes_warn_total`** counter — one
  increment per submission that crossed
  `build_log_bytes_per_job_warn` while live (default 100 MiB).
- **`JobStatusResponse.suspected_worker_infra: Option<String>`**
  — a terminal-time heuristic that answers "did my user's code
  break or did my fleet break?" Conditions: ≥ 3 originating
  failures, all in {Transient, DiskFull}, strict majority from
  one attributed worker_id. Per-job mode primarily.
- **`DrvFailure.worker_id: Option<String>`** — originating
  failures now carry the reporting worker's id. Feeds the
  heuristic above.
- **`/readyz` flips 503 on drain OR
  `claims_in_flight >= max_claims_in_flight`.** Load balancers
  steer away from draining or overloaded replicas without any
  coordination layer. Three new tests in `admin_ops.rs`.

### Added — HA + deployment
- **HA primary/standby** via `pg_advisory_lock` — already in the
  coordinator module but previously untested. Three layers of
  coverage: (1) `runner_sse_fails_cleanly_when_coord_unavailable`
  for the client-side fail-loud contract; (2)
  `nix/checks/coordinator-ha-failover.nix` — three-node VM test
  (db + two coordinators) under `nix flake check`; (3)
  `scripts/orbstack_harness/ha-failover.sh` — real-TCP scenario
  with `docker kill --signal=KILL`, measures failover in 1 second.
  Wired into `spec_report.sh` as `P-HA-FAILOVER`.
- **`docs/deployment-ha.md`** — HA deployment guide: mental
  model, NixOS + Kubernetes + HAProxy examples, what survives
  vs what doesn't, rolling deploy procedure, PG interaction,
  monitoring hooks.

### Added — shadow mode
- **`docs/shadow-mode.md`** — pattern doc for shadow + canary
  rollout. No coordinator-side code changes required; the work
  is in the caller + comparison tooling.
- **`nix-ci-compare`** binary (new in `nix-ci-harness` crate) —
  takes two `JobStatusResponse` JSONs, emits a divergence
  report with four-tier severity (matched / minor / major /
  critical). CI-friendly exit codes for rollback gating.

### Added — observability + perf
- **`by_job` secondary index on `Claims`** — `evict_claims_for`
  and the SSE progress tick are now O(claims-for-that-job)
  instead of O(total claims). Hot in large fleets where a
  cancel event previously cloned every Arc in the primary map.
- **Rolling-upgrade test** (`rolling_upgrade_preserves_terminal_rows`)
  and **orbstack `ha-failover.sh`** — empty-topic-topped previously
  unmapped `P-UPGRADE-SAFE` bar; both now enforce the
  byte-identity contract on pre-existing terminal rows.

### Added — testing
- **Ingest fuzz** (`ingest_fuzz.rs`) — 100 randomized batches
  mixing valid + malformed shapes, asserts response counters
  balance, memory stays bounded, error rate matches injection.
- **/metrics scrape contention test** — 50 concurrent scrapes
  interleaved with 50 concurrent job-creates; asserts all
  bodies parse with # EOF terminator.
- **Nine unit tests for `suspected_worker_infra` heuristic**
  covering the severity matrix (empty, below minimum, strict
  majority, 50/50 tie, BuildFailure excluded, propagated
  excluded, mixed infra categories, missing worker_id,
  empty worker_id).
- **Six unit tests for `nix-ci-compare`** covering the
  four-tier severity + filtering of propagated/truncated rows.
- **Five unit tests for the `by_job` claim index** including
  an insert/take stress that preserves the sum-of-secondary ==
  primary-len invariant.

### Removed
- None in this pass. All changes are additive or replace-
  internal-impl-only.

### Migration notes
- Operators running 0.2.0 or earlier should run
  `cargo sqlx migrate run` or accept the automatic in-
  `connect_and_migrate` migration; schema version bumps to
  include `0006_failed_outputs_worker_id.sql`.
- The default for `max_claims_in_flight` changed from `None` to
  `Some(25_000)`. Deployments with fleets larger than ~2500
  concurrent workers should review and raise this explicitly.
- The default for `worker_quarantine_failure_threshold` changed
  from `None` to `Some(5)`. Deployments with intentionally-flaky
  dedicated builders (e.g. testing-infrastructure CI) can opt
  out by setting it back to `null` in their config JSON.
- `graceful_shutdown_secs` default changed from 30s to 60s.
  Monitoring for "SIGTERM-to-exit time" alerts should be
  adjusted accordingly.

## [Previous Unreleased] — Simplification pass (April 2026)

Verified-dead-code reduction, exploratory-artifact cleanup. See
[`SIMPLIFICATION_REPORT.md`](../SIMPLIFICATION_REPORT.md) at repo root.

### Removed
- **`Error::Subprocess` variant.** Never constructed in production —
  the runner classifies subprocess failures through `anyhow` /
  `Error::Internal`. Its two self-tests went with it. **Behavior
  change for downstream consumers if any matched on this variant**:
  none exist today (`publish = false`), but the variant deletion is
  the nearest thing to a breaking change in this pass.
- **`Toplevels::contains()` method** on `dispatch/submission.rs`.
  Previously `pub` with zero callers.
- **`Ordering::Relaxed` placeholder** in `server/build_logs.rs`
  (commented as "defensive use ... in case future code paths grow";
  they haven't).
- **`degradation-test` Cargo feature.** Defined but no test gated
  behind it. `tests/degradation.rs` still runs unconditionally.

- **Valkey-vs-memory prototype bench.**
  `benches/valkey_vs_memory.rs` (~750 LOC), `docs/valkey-vs-memory-bench.md`,
  and the `fred` + `hdrhistogram` dev-dependencies. The bench
  answered its question (in-memory won); keeping the code cost a
  Valkey runtime to run it and 13 transitive packages in the
  lockfile.
- **`docs/design-stateless-coordinator.md`** exploration writeup
  (549 lines); author-marked "Exploration. Not committed."

- **Repo-root scratch:** `REFACTOR_NOTES.md`, `REFACTOR_TRIAGE.md`,
  `rust/RUST_REVIEW.md`. All triaged completed work.

### Changed
- Feature list on `nix-ci-core` shrunk from 5 → 4 (no
  `degradation-test`).

### Dependencies
- **Removed:** `fred` (9.x) and `hdrhistogram` (7.x) from
  `nix-ci-core` dev-dependencies. Transitively drops 13 packages
  from `Cargo.lock`.

### Public API
- `Error::Subprocess` removal is the only enum-variant change.
  Every other public type is untouched. No handler, CLI flag, route,
  or wire shape changed.

## [Previous Unreleased] — Rust review pass

Senior-Rust-engineer review of the workspace on branch `v3`. See
[`FOLLOWUPS.md`](FOLLOWUPS.md) for the deferred items.

### Changed
- **MSRV declared.** `workspace.package.rust-version = "1.82"` is now
  on the workspace manifest; every crate inherits it via
  `rust-version.workspace = true`. 1.82 was chosen because it's the
  oldest stable that carries `Option::is_none_or` (used in
  `dispatch/submission.rs`) and `std::iter::repeat_n` (used in
  `durable/writeback.rs`). This is a documentation change, not a
  build-behavior change — the tree already built on 1.82 — but from
  here on a PR that uses a newer language feature must also bump this
  field and take the CI hit. Not a breaking change for consumers.

### Fixed
- **`runner::artifacts::collect_failure_logs` no longer blocks the
  async runtime.** The post-run `std::fs::create_dir_all` and
  `std::fs::write` calls are now wrapped in
  `tokio::task::spawn_blocking`. In practice this was low impact
  (called once per `nix-ci run`, ≤10 small files) but it violated the
  "no blocking I/O on async tasks" contract. No observable behavior
  change.
- **`runner::artifacts::unique_filename` `unreachable!()` now carries
  a message.** The loop iterates `2u64..`; the message spells out
  why the `unreachable!` is actually unreachable (would require
  ~1.8e19 pre-registered filenames). Pure safety annotation.

### MSRV
- `rust-version = "1.82"` declared for the first time. Prior builds
  succeeded on 1.82+ by coincidence; the declaration makes that
  explicit.

### Public API
- No changes. Every finding that would have shifted a public type or
  signature is catalogued in `FOLLOWUPS.md` and deferred.

### Dependencies
- No adds, no removes, no version bumps.
