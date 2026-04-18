# Changelog

All notable changes to the `nix-ci` workspace. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/). Versioned per the
workspace `version` (currently `0.2.0`, unpublished).

## [Unreleased] — Rust review pass

Senior-Rust-engineer review of the workspace on branch `v3`. See
[`RUST_REVIEW.md`](RUST_REVIEW.md) for the full Phase-1 survey and
findings and [`FOLLOWUPS.md`](FOLLOWUPS.md) for the deferred items.

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
