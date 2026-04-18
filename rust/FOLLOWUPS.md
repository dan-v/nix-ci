# Rust-review follow-ups

Items identified during the review pass on branch `v3` that were
deliberately **not** taken in that pass. Each has `what / why / scope /
risk` so a future PR can decide whether to pick it up.

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
