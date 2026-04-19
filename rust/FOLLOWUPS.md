# Rust-review follow-ups

Items identified during the review pass on branch `v3` that were
deliberately **not** taken in that pass. Each has `what / why / scope /
risk` so a future PR can decide whether to pick it up.

## Status markers

- **ADDRESSED** — landed; linked to the relevant commit/test.
- **DEFERRED** — intentionally not taken; rationale below.
- **OPEN** — still a candidate; not yet evaluated.

## Addressed by the production-readiness pass (April 2026)

See [CHANGELOG](./CHANGELOG.md) for the full list; the audit-flagged
items closed in that pass:

- **ADDRESSED: Overload shedding + auto-quarantine off by default.**
  Both now default to operationally-tuned values; explicit `null`
  preserves the opt-out. Commit `04a3a37`.
- **ADDRESSED: No cap on `build_logs` byte growth.** New
  `max_build_logs_bytes` (default 50 GiB) + byte-ceiling pruning
  in the cleanup loop. Commit `0c0ebb0`.
- **ADDRESSED: No per-job log attribution.** New
  `build_log_bytes_per_job` histogram + per-submission warn
  threshold. Commit `73c62f2`.
- **ADDRESSED: `/readyz` didn't reflect dispatcher health.** Now
  returns 503 on drain + overload. Commit `3e3324a`.
- **ADDRESSED: `evict_claims_for` O(total claims).** Secondary
  `by_job` index makes it O(claims-for-that-job); SSE progress
  tick benefits too. Commit `f3808c2`.
- **ADDRESSED: `failed_outputs` had no worker attribution.** New
  `worker_id` column + `/admin/refute?worker_id=X`. Commit `713815e`.
- **ADDRESSED: Per-job user couldn't tell infra from code.** New
  `JobStatusResponse.suspected_worker_infra` heuristic. Commit `6c67b6c`.
- **ADDRESSED: No HA failover test.** Three-layer coverage —
  in-process SSE failure contract, nixosTest, orbstack real-TCP
  + SIGKILL. Commits `8c64b61`, `4d32dac`, `9e75a9b`.
- **ADDRESSED: No HA deployment docs.** See
  [`docs/deployment-ha.md`](../docs/deployment-ha.md). Commit `9b635c1`.
- **ADDRESSED: Drain didn't reject new ingest on existing jobs.**
  Commit `5140411`.
- **ADDRESSED: `auto_fail_*` paths bypassed pool-acquire
  histogram.** All four now use `_observed`. Commit `27d9f83`.
- **ADDRESSED: `cap_failures` didn't strip `log_tail`.** Past
  first 10 entries, `log_tail` is `None`. Commit `0d6e4cf`.
- **ADDRESSED: Axum graceful drain unbounded.** Wrapped in
  `tokio::time::timeout(graceful_shutdown_secs, ..)` with
  abort_handle. Default bumped 30s → 60s. Commit `78e04a2`.
- **ADDRESSED: Terminal-write wedge on PG outage.** Periodic
  retry sweep on `reaper_interval_secs`. Commit `2a50c9c`.
- **ADDRESSED: Claim loop didn't re-check draining after wake.**
  Commit `ea6600e`.
- **ADDRESSED: No rolling-upgrade test.** New
  `rolling_upgrade_preserves_terminal_rows` in resilience.rs +
  wired into `spec_report.sh` as `P-UPGRADE-SAFE`. Commit `88233f2`.
- **ADDRESSED: No runner-SSE-across-restart test.** Two new tests
  in resilience.rs codifying the fail-loud-in-bounded-time
  contract. Commit `8c64b61`.
- **ADDRESSED: No ingest fuzz.** 100-batch randomized test with
  mixed valid+malformed inputs. Commit `ece9331`.
- **ADDRESSED: No shadow-mode tooling.** `nix-ci-compare` CLI
  + `docs/shadow-mode.md`. Commit `0d0829c`.

## Still open / deferred



## F-0. Simplification pass (April 2026) — deferred items

A ruthless-simplification pass ran over the tree and produced the
cuts described in `SIMPLIFICATION_REPORT.md`. The following items
were considered and explicitly NOT cut:

- **`tests/scale_xl.rs` + `docs/scale-xl-findings.md`.** Kept. Opt-in
  diagnostic harness that found the single-writer bottleneck driving
  commit 4fa7a1d. If a future hot-path change needs verification,
  run with `--features scale-test`. Remove if it hasn't been
  exercised for several quarters.
- **Duplicate `validate_worker_id` in `server/claim.rs` and
  `server/ops.rs`.** Signatures differ (`&Option<String>` vs `&str`)
  and contracts differ (claim allows None/empty; fence requires
  non-empty). Past triage already considered and deferred
  consolidation; re-consider only if a third call site appears.
- **Claim long-poll skeleton duplication** between per-job `claim`
  and fleet `claim_any` in `server/claim.rs`. Different scan shape
  (one submission vs all) makes a clean shared helper awkward. Left
  as-is per the prior refactor-triage assessment.
- **Trivial inline unit tests** (e.g.,
  `submissions_is_empty_tracks_inserts`,
  `is_empty_and_len_track_inserts_and_takes`, a few others). Flagged
  as low-signal-low-cost; cutting them saves nothing meaningful.
- **Table-driven consolidation** of the 7 `validate_catches_X`
  tests in `tests/config_json.rs`. Would save ~60 LOC. Trivial win;
  do it the next time this file gets touched.
- **`append_eval_errors()` O(N²) attr dedup loop** in
  `dispatch/submission.rs`. At the 500-cap this is 250K
  comparisons; not hot, but a HashSet<String> would make it O(N).
  Low priority.



## F-1. Install and wire review tooling into CI

**What**: Install `cargo-audit`, `cargo-udeps` (nightly), and
`cargo-semver-checks`. Add a CI job that runs them on every PR (or at
least nightly). `cargo-machete` is already installed and clean.

**Why**: The review spec calls these out as the right way to find
security-advisory hits, unused dependencies, and accidental semver
breakage. None were available in the dev shell at review time; I
verified `cargo-machete` reports no unused deps, but the three missing
tools would catch different things (CVEs, nightly-feature-detected
unused deps, public-API-shape changes).

**Scope**: One PR. Adds `pkgs.cargo-audit cargo-udeps cargo-semver-checks`
(or equivalents) to the Nix dev shell; adds a GitHub Actions / whatever-CI
step that runs `cargo audit`, `cargo +nightly udeps --all-targets`, and
`cargo semver-checks` (the last needs a published-baseline setup, which
is a no-op today because `publish = false`).

**Risk**: Low. The tools are advisory; a noisy finding blocks the PR
that introduces it, not the tree at rest.

## F-2. Run `cargo +MSRV check` on CI

**What**: Add a CI matrix entry that runs `cargo +1.82.0 check
--workspace --all-features` so the `rust-version = "1.82"` declaration
doesn't rot.

**Why**: The field just got added in this pass. Without a CI check, a
contributor using a newer toolchain can introduce a 1.84-only language
feature and the workspace Cargo.toml silently becomes wrong. The gate
turns "can I use this feature?" into a mechanical question instead of a
reviewer-memory question.

**Scope**: ~10 lines in CI config; nightly rustup to install `1.82.0`.

**Risk**: Low. First failure would be a PR that added a too-new
feature — catching it is the point.

## F-3. Pedantic-clippy cleanup in test files

**What**: The 100+ pedantic/nursery warnings from
`cargo clippy -- -W clippy::pedantic -W clippy::nursery` are all in
test files; the library `src/` is already clean. The bulk are
`uninlined_format_args` (autofixable via `cargo clippy --fix --tests`),
plus a handful of `doc_markdown` nits on test-module headers and one
`cast_possible_truncation` on a test helper.

**Why**: Running the autofix closes the gap and makes it easier to
turn on pedantic selectively in the future. Not load-bearing — tests
pass; the lint group is advisory.

**Scope**: One PR. Most hits are `cargo clippy --fix --tests`; the
`doc_markdown` hits need human review (some of the underscore-separated
identifiers are prose, not code).

**Risk**: Very low; test-only, behavior unchanged.

## F-4. `config::max_attempts: i32` → `NonZeroU32`

**What**: `ServerConfig::max_attempts` is currently `i32` with a runtime
`< 1` check in `validate()`. `std::num::NonZeroU32` makes the invariant
type-level.

**Why**: Smaller panic/validation surface; impossibility of zero is
expressed to the reader.

**Scope**: Breaks JSON config compatibility (`NonZeroU32` serializes
the same as `u32`, but a file with `"max_attempts": 0` would then fail
at parse time instead of validation time — different error message
path, probably acceptable). Also ripples through `Step::max_tries: i32`.

**Risk**: Medium — touches wire-config semantics. Would need a note in
the next release's migration section.

## F-5. `#[non_exhaustive]` on public enums

**What**: Mark `JobStatus`, `ErrorCategory`, `JobEvent`, and the
`Error` enum `#[non_exhaustive]`.

**Why**: Future-proofs downstream code against silent breakage when
variants are added. Required discipline if this crate is ever
published.

**Scope**: Four `#[non_exhaustive]` attributes plus downstream
`_ => ...` match arms where the compiler complains. Locally noisy;
semantic impact only when the crate is consumed outside the workspace.

**Risk**: Low if kept in-workspace (nothing to break). Higher if a
consumer outside the workspace starts matching exhaustively on these
enums — though the crate is currently `publish = false`, so there is
no such consumer.

## F-6. Sealed-trait pattern for `durable::logs::LogStore`

**What**: Add a `pub-in-private` `Sealed` supertrait to `LogStore` so
external crates can use the trait (as they already can today) but not
implement it.

**Why**: The trait is nominally a storage-backend plugin point, but
the only intended implementors are in-crate: `PgLogStore` today, a
future S3/GCS impl tomorrow. Sealing makes that intent explicit and
lets us evolve `LogStore`'s method set without worrying about
downstream breakage.

**Scope**: ~10 lines. Not urgent — no external impls exist.

**Risk**: Very low.

## F-7. `tokio::task::JoinSet` for runner orchestrator

**What**: The runner's `orchestrator.rs` spawns worker / heartbeat /
SSE / eval-kill tasks as separate `JoinHandle`s and awaits them one
at a time on shutdown. A `JoinSet` would model the group explicitly.

**Why**: Structured concurrency is the mantra. It'd also make it easy
to say "if any of these tasks panics, fail the run early" (which the
current code doesn't).

**Scope**: ~30 lines diff. Needs care because some tasks are
intentionally fire-and-forget (the eval-kill guard) and some produce
`Result<...>` (submitter, sse). A `JoinSet<Result<...>>` plus
`FuturesUnordered` for the void tasks would be the cleanest split.

**Risk**: Medium. Runner shutdown is load-bearing; a refactor here
should be accompanied by an integration test that deliberately panics
one of the tasks and verifies the others wind down.

## F-8. Merge duplicate transitive-dep versions where practical

**What**: `cargo tree --duplicates` reports base64, chrono,
crypto-common, hmac, md-5, sha2 appearing in two major versions each.

**Why**: Smaller build, smaller trust surface. In practice all of
these duplicates come from sqlx pulling its own pinned subset against
an old pin of a crypto crate (e.g., `sha2 0.10` alongside whatever is
the current one). Fixing would require waiting for sqlx to unify.

**Scope**: Upstream-paced; we'd need to submit PRs to sqlx or wait for
the next sqlx release that bumps these.

**Risk**: Out of our control.

## F-9. Dev-shell ergonomics for running the full test suite

**What**: Document (or bake into `nix develop`) the `DATABASE_URL`
plumbing so a new contributor can `cargo test --workspace` and have
the sqlx::test-backed integration tests actually run against the
bound-mounted Postgres container.

**Why**: The lib-only `cargo test --lib` path passes out of the box,
but the 40+ integration tests in `tests/` need `DATABASE_URL` pointed
at the `nix-ci-pg` container. Friction for first-run contributors.

**Scope**: README / CONTRIBUTING paragraph plus a `justfile` / `flake.nix`
alias.

**Risk**: Doc only. Low.
