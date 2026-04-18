# Simplification pass — report

Branch: `claude/jolly-gould-b151ab`  ·  Base: `origin/v3` (4675b13)

## Context

The pass was framed as a "ruthless" reduction. What actually fits
this tree is much narrower: it has already been through two
simplification rounds (REFACTOR_NOTES + REFACTOR_TRIAGE) and the
H1-H15 hardening series. The verified dead code was ~35 LOC; the
headline reductions are in exploratory docs and a prototype bench
whose question has been answered.

Per-file audit of all 40 source files + 60 test files + both
Cargo.toml files. Every cut was grep-verified before landing.

## Before / after

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Rust total LOC (incl. tests, benches) | 35,131 | 34,356 | **-775** |
| nix-ci-core `src/` LOC | 14,854 | 14,825 | **-29** |
| Integration test LOC | 18,014 | 18,014 | 0 |
| Benches LOC | 1,282 | 536 | **-746** |
| Repo-root markdown files | 5 | 2 | **-3** |
| Deleted non-Rust docs (bytes) | ~54 KB | — | **-54 KB** |
| `Cargo.lock` packages (direct + transitive) | 378 | 365 | **-13** |
| Cargo dev-dependencies | 6 | 4 | **-2** |
| Cargo feature flags (nix-ci-core) | 5 | 4 | **-1** |
| Tests removed (all suites) | — | — | 0 |
| Tests failing / skipped after pass | 0 | 0 | 0 |

No behaviour change, no schema change, no wire-protocol change.
Dispatcher invariants intact. SPEC bars and their tests untouched.

## Commits (in order)

| SHA | Summary |
|---|---|
| `98ec218` | simplify: drop four pieces of verified dead code |
| `8a8b9cf` | docs: remove completed-pass scratch (REFACTOR_NOTES, REFACTOR_TRIAGE, RUST_REVIEW) |
| `e75d3d7` | simplify: drop Valkey-vs-memory prototype bench and its deps |
| `1935bf7` | docs: delete stateless-coordinator exploration writeup |

## Cuts by category

### 1. Verified dead code (commit `98ec218`)

| Item | Location | Evidence |
|---|---|---|
| `let _ = Ordering::Relaxed;` placeholder | `server/build_logs.rs:91` | Comment admitted speculation; no consumers. |
| `Toplevels::contains()` method | `dispatch/submission.rs:49-51` | No callers in src, tests, benches, or docs. |
| `Error::Subprocess` variant + its two self-tests | `error.rs` | Only constructed inside its own test module. Runner's subprocess failures surface through `Error::Internal` / `anyhow`. |
| `degradation-test` Cargo feature | `nix-ci-core/Cargo.toml` | No file gated behind `#![cfg(feature = "degradation-test")]`; `tests/degradation.rs` runs unconditionally. |

Net: ~35 LOC. Build + `clippy -D warnings` clean.

### 2. Completed-pass scratch (commit `8a8b9cf`)

- `REFACTOR_NOTES.md`, `REFACTOR_TRIAGE.md` — both triaged a refactor
  that has landed on v3.
- `rust/RUST_REVIEW.md` — past review writeup; actionable items
  already in `rust/FOLLOWUPS.md`.

Net: 3 files, 722 lines / ~38 KB of repo-root clutter.

### 3. Prototype bench and its decision doc (commit `e75d3d7`)

- `benches/valkey_vs_memory.rs` (746 LOC)
- `docs/valkey-vs-memory-bench.md` (190 lines)
- `fred`, `hdrhistogram` dev-dependencies removed
- 13 packages removed from the transitive lockfile (fred's tree)

The in-memory dispatcher won; Valkey isn't on the roadmap; the
bench was frozen reference code.

### 4. Exploration doc (commit `1935bf7`)

- `docs/design-stateless-coordinator.md` (549 lines) — author's own
  header said "Exploration. Not committed." No in-tree referrer.

## Verified load-bearing — NOT cut

Listed because an earlier audit pass misidentified them and this is
the reproducible evidence:

- **All `ServerConfig` fields** previously flagged as orphaned
  (`flaky_retry_backoff_step_ms`, `max_input_drvs_per_drv`,
  `max_required_features_per_drv`, `max_eval_errors_per_batch`,
  `worker_quarantine_*`). Every one is read on a hot path:
  `server/complete.rs`, `server/ingest_batch.rs`, `server/ops.rs`.
- **Duplicate `validate_worker_id`** in `server/claim.rs` and
  `server/ops.rs`. Different preconditions (claim allows None/empty;
  fence requires non-empty). Past triage already considered and
  deferred consolidation.
- **`ClaimMode::Fleet`** — exercised by `tests/fleet.rs` and
  `nix-ci/src/main.rs` (`nix-ci worker` subcommand).
- **`DrvOutput.name` / `.hash_algo`** with `#[allow(dead_code)]` —
  parsed for Derive() tuple fidelity; removing them complicates the
  ATerm parser for no gain.
- **`LogStore` trait with one impl** — deliberate extensibility
  seam for S3/GCS; `FOLLOWUPS.md` F-6 has the plan.
- **`tests/scale_xl.rs` + `docs/scale-xl-findings.md`** — diagnostic
  harness that found the single-writer bottleneck (commit 4fa7a1d);
  opt-in, not in CI. Keeper.
- **`nix-ci-loom-checks`** crate — cheap, test-only, exhaustively
  exercises the 8 dispatcher invariants under loom.

## Verification

All four commits keep the tree green:

- `cargo check --workspace --all-targets` — clean
- `cargo clippy --workspace --lib -- -D warnings` — clean
- `cargo clippy --workspace --all-targets -- -D warnings` — clean

Full integration test run requires the `nix-ci-pg` Postgres container
and wasn't executed as part of this pass; nothing in the cuts
touches SQL, wire types, or handler logic.

## Risk assessment

All cuts are reversible via `git show`. No public API, wire format,
database schema, CLI surface, or config file shape was changed. The
only externally-visible delta is: four fewer markdown files in the
repo, one fewer Cargo feature, two fewer dev-dependencies.

## Honest summary

The tree is not 30-60% dead weight. It's closer to 2%. The user
already did the work. The useful product of this pass is the
*verified* dead-weight inventory — if a future reviewer repeats the
audit, they should land on the same list (or a strict subset) unless
new code has drifted in since. See `rust/FOLLOWUPS.md` for what was
considered and deferred.
