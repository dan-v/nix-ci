# Changelog

All notable changes to the `nix-ci` workspace. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/). Versioned per the
workspace `version` (currently `0.2.0`, unpublished).

## [Unreleased] — Simplification pass (April 2026)

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
