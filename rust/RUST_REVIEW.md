# Rust review — nix-ci workspace

A senior-Rust-engineer pass across the `rust/` workspace on branch `v3`.
Baseline at start: `cargo check --workspace --all-targets` green;
`cargo clippy --workspace --all-targets -- -D warnings` green; 50+
integration tests.

This is a findings-only document. No code changes have been made.

---

## 1. Workspace layout

Three crates, `resolver = "2"`, edition 2021:

| Crate | Kind | Role |
|---|---|---|
| [`nix-ci-core`](crates/nix-ci-core/Cargo.toml) | `lib` | Dispatcher, server, client, runner, durable. One crate so the binary can link any combination. |
| [`nix-ci`](crates/nix-ci/Cargo.toml) | `bin` (`nix-ci`) | Thin CLI wrapper; all logic lives in core. |
| [`nix-ci-loom-checks`](crates/nix-ci-loom-checks/Cargo.toml) | `lib` (empty) + tests | Isolated `--cfg loom` model tests for two dispatcher invariants. Split into its own crate to contain `RUSTFLAGS="--cfg loom"` propagation — already the correct call; a transitive dep (`concurrent-queue` via tokio/sqlx) has buggy `#[cfg(loom)]` branches. |

Source: ~6,500 lines production, ~16,000 lines tests (~50 integration
test files).

## 2. Edition, MSRV, toolchain

- Edition `2021`.
- **No declared MSRV** in `Cargo.toml` (no `rust-version =` field).
  Given the dep surface (axum 0.8, sqlx 0.8, tokio 1.x with `test-util`),
  effective MSRV is modern stable. Declaring it explicitly would let
  CI catch an accidental feature bump.
- The `observability/mod.rs` tests use `unsafe { std::env::set_var(...) }`
  gated on 2024-edition semantics — the code already works on 2021 but
  is forward-compatible.

## 3. Async runtime

- **Tokio** (`"1"` with `full`). Workspace-level.
- `tokio::sync::{broadcast, watch, oneshot, mpsc, Mutex}` for async
  primitives.
- **Intentional `tokio::time::Instant` everywhere** scheduling-relevant
  (`dispatch/claim.rs`, `dispatch/submission.rs`, `durable/reaper.rs`,
  etc.) so tests can drive `tokio::time::pause()` + `advance()`. This is
  a deliberate policy documented at [`claim.rs:11-20`](crates/nix-ci-core/src/dispatch/claim.rs:11).
- `parking_lot` for sync locks (`Mutex`, `RwLock`) on hot paths that
  never hold across `.await`. Workspace lints `await_holding_lock =
  "deny"` enforce this.
- `async_trait` for `LogStore` (storage backend trait).
- `async_stream::stream!` for the SSE progress event merger.

## 4. Error handling strategy

Hybrid, correctly scoped:

- **Library (`nix-ci-core`)**: one domain `Error` enum in
  [`error.rs`](crates/nix-ci-core/src/error.rs), `thiserror`-derived,
  with explicit HTTP status mapping and a 4xx-keeps-message /
  5xx-sanitized-message split for the axum `IntoResponse` impl.
  Covered by 10+ tests asserting the sanitization contract — leaking
  a `/var/...` path in a 5xx body would be flagged.
- **Binary (`nix-ci/src/main.rs`)**: `anyhow::Result` with
  `anyhow!(...)` / `bail!(...)` for caller-facing CLI errors. Clean.
- **Internal parser** (`runner/drv_parser.rs`): uses `anyhow` internally
  for `.context(...)` chaining during recursive-descent parsing, then
  converts the whole chain to a typed `DrvParseError` at the public
  boundary via `DrvParseError::from_internal`. Captures context without
  leaking the dep into the library's public surface. Good pattern.
- `#[from]` impls on `Error` for `sqlx::Error`, `sqlx::migrate::MigrateError`,
  `reqwest::Error`, `std::io::Error`, `serde_json::Error` — each
  unambiguously maps to one variant. Manual `.map_err(...)` elsewhere
  preserves context (`Error::Internal(format!("serialize terminal snapshot: {e}"))`).

No `Box<dyn Error>` in public APIs. No `Err("string")`. No silent
`.unwrap_or_default()` hiding a real failure.

## 5. Unsafe surface

**None in production src.** Two files touch `unsafe`, both tests:

- [`src/observability/mod.rs:175-193`](crates/nix-ci-core/src/observability/mod.rs:175)
  — `unsafe { std::env::set_var / remove_var }` under `#[cfg(test)]`.
  Required by the 2024-edition `set_var` signature; safety comment
  references test-isolation limits.
- [`tests/config_json.rs`](crates/nix-ci-core/tests/config_json.rs) —
  same pattern for env-override tests.

Neither reaches production. `#[forbid(unsafe_code)]` on library modules
would be a defensible tightening but the current lint posture is
sufficient.

## 6. Third-party dependencies (workspace)

Grouped by role:

| Role | Crates |
|---|---|
| **Async / runtime** | tokio, tokio-stream, futures, async-stream, async-trait |
| **HTTP server** | axum 0.8 (with `macros`), tower-http (via axum) |
| **HTTP client** | reqwest 0.12 (rustls-tls, http2, stream, json), eventsource-stream |
| **Sync primitives** | parking_lot |
| **Database** | sqlx 0.8 (postgres, uuid, chrono, macros, migrate, runtime-tokio-rustls) |
| **Serialization** | serde, serde_json |
| **Errors** | anyhow, thiserror v2 |
| **Observability** | tracing, tracing-subscriber (env-filter, fmt, json), prometheus-client, opentelemetry 0.27 (+sdk, +otlp http-proto, +semantic-conventions, tracing-opentelemetry) |
| **CLI** | clap 4 (derive, env) |
| **IDs / time** | uuid (v4, serde), chrono (clock, serde) |
| **Compression** | flate2 |
| **Dev** | tempfile, rand (test only), criterion, loom (separate crate), tokio `test-util` |
| **Supply-chain policy** | `deny.toml` configured (advisories, licenses, bans). One currently-ignored advisory (RUSTSEC-2026-0097 rand, test-only). |

No CVEs in the direct-dep set at review time (cargo-deny config is
present; operational `cargo audit` status was not re-run in this pass).

`cargo tree -e normal --duplicates` shows the usual transitive
duplicates (base64 via hyper-util + sqlx + tonic, chrono via core +
sqlx-postgres, crypto-common via hmac/hkdf/md-5/sha2). None are owned
by nix-ci directly; unifying would require upstream version alignment.

## 7. Public API surface

`nix-ci-core`'s public `lib.rs`:

```rust
pub mod client;        // CoordinatorClient + http module
pub mod config;        // ServerConfig, RunnerConfig, ConfigErrors
pub mod dispatch;      // Dispatcher, Step/StepState, Submissions, Claims, rdep::*
pub mod durable;       // connect_and_migrate, CoordinatorLock, reaper, cleanup, writeback, logs::{LogStore, PgLogStore}
pub mod error;         // Error, Result
pub mod observability; // init_tracing, install_panic_hook, metrics::Metrics
pub mod runner;        // run (RunArgs, RunOutcome), worker, submitter, eval_jobs, sse, output, artifacts, drv_parser, drv_walk
pub mod server;        // AppState, build_router, run
pub mod types;         // All wire types (JobId, ClaimId, DrvHash, enums, request/response structs, events)

pub use error::{Error, Result};
pub use types::*;
```

The wire types in `types.rs` deliberately `pub`-expose every field with
`#[serde(default)]` where backwards-compat matters. `#[serde(tag = "kind")]`
on `JobEvent` gives a stable JSON-with-discriminator shape. Good
decisions for a service API.

No `#[non_exhaustive]` markers. Adding them to `ErrorCategory`,
`JobStatus`, and `JobEvent` would be a reasonable future-proofing step
for a published crate — not yet needed for an internal workspace.

## 8. Correctness & safety findings

### 8.1 Ownership / borrowing
- No egregious `.clone()` on hot paths I noticed.
  `StepsRegistry::get_or_create` is explicit about the dedup-hit fast
  path and only calls `factory()` on miss. `Submission::add_member`
  deliberately splits `contains_key` from `insert` to avoid cloning the
  `DrvHash` key on the hit path (comment at
  [submission.rs:264-267](crates/nix-ci-core/src/dispatch/submission.rs:264)).
- `Arc<Mutex<...>>` usage is bounded and justified:
  `parking_lot::Mutex<Registry>` for the metrics registry (rendering
  is read-mostly, single-writer for updates); `tokio::sync::Mutex<Option<Child>>`
  in `eval_jobs::KillHandle` (async mutation of the child process).
  No `Arc<Mutex<T>>` patterns that would be better modeled as an actor.
- Function signatures sensibly use `&Path`, `&str`, `&[String]` etc.;
  there is no general pattern of taking `String` / `PathBuf` / `Vec<T>`
  by value just for ergonomic reasons.

### 8.2 Unsafe audit
No production `unsafe`. No miri target to run.

### 8.3 Panic surface (production only)

99 `.unwrap()` occurrences workspace-wide; almost all are in `#[cfg(test)]`
blocks. The production-path panics I found:

| Location | Justification |
|---|---|
| [`config.rs:160`](crates/nix-ci-core/src/config.rs:160) `"127.0.0.1:8080".parse().unwrap()` | Literal socket addr in `Default::default`. Infallible. |
| [`server/ops.rs:75`](crates/nix-ci-core/src/server/ops.rs:75) `"application/openmetrics-text;...".parse().unwrap()` | Literal header value. Infallible. |
| [`server/build_logs.rs:107`](crates/nix-ci-core/src/server/build_logs.rs:107) `"text/plain; charset=utf-8".parse().unwrap()` | Same. |
| [`client/http.rs:54`](crates/nix-ci-core/src/client/http.rs:54) `Client::builder().build().expect("reqwest client build")` | reqwest builder is infallible with the features we enable. |
| [`dispatch/claim.rs:112`](crates/nix-ci-core/src/dispatch/claim.rs:112) `.expect("just checked present")` | Invariant proof inside a write-locked critical section. |
| [`server/complete.rs:106`](crates/nix-ci-core/src/server/complete.rs:106) `.expect("s.len() is always a char boundary")` | UTF-8 invariant proof. |
| [`server/mod.rs:135,140`](crates/nix-ci-core/src/server/mod.rs:135) `signal::ctrl_c().await.expect(...)` / `signal::unix::signal(...).expect(...)` | Signal-handler install at startup; failing loudly is correct. |
| [`dispatch/submission.rs:462`](crates/nix-ci-core/src/dispatch/submission.rs:462) `.expect("hash from members.keys()")` | Invariant proof — iterating `members.keys()` and immediately looking up the key. |

All are infallible in context or a fail-loud-at-startup decision.

One small finding: [`errors.push("max_attempts must be >= 1")`](crates/nix-ci-core/src/config.rs:280)
uses `i32` for `max_attempts` but a `u32` would make the `< 1` check
impossible to phrase wrongly. Minor type-signaling improvement.

### 8.4 Integer / arithmetic
- `fetch_add` on u32 counters uses the pattern
  `try_reserve_drvs(n, cap)`: fetch_add then range-check then roll back
  if over. The `saturating_add` at [submission.rs:192](crates/nix-ci-core/src/dispatch/submission.rs:192)
  prevents overflow from a pathologically large `n`.
- Several casts from `usize`/`u128` to `u64` or `i64` use explicit
  `.min(...)` / `as u64` conversions. The one `as u64` without a
  saturating step is at [events.rs:58](crates/nix-ci-core/src/server/events.rs:58)
  `saturating_duration_since(...).as_millis() as i64` — castable because
  a duration fits i64 millis for billions of years.

### 8.5 Division / modulo
`fmt_ms` (`output.rs:357`) divides by literal 60/3600 — constant and
non-zero. `attempts_left = tuning.complete_max_attempts - attempt`
(`worker.rs:397`) is guarded by the `attempt >= max_attempts` break
immediately above. No unguarded user-controlled division I could see.

## 9. Error handling — closer reading

### 9.1 `Error` enum — variant design
Variants directly model HTTP contract shapes (`BadRequest`, `NotFound`,
`Gone`, `PayloadTooLarge`, `Unauthorized`, `Forbidden`, `ServiceUnavailable`)
plus internal categories (`Db`, `Migrate`, `Http`, `Io`, `Serde`, `Config`,
`Internal`, `Subprocess`). Every variant that needs payload carries a
concrete message or structured fields (`Subprocess { tool, code }`).
This is exactly right for a library whose primary consumer is its own
axum surface; a caller who wants to handle `Db` differently from `Http`
can match on the variant.

### 9.2 Anti-patterns check
- **`.unwrap_or_default()` hiding failure** — used sparingly and only
  for display/serialization paths (e.g., preview of response body on
  error, metrics render, `format!` context extraction). Not hiding
  actionable errors.
- **`let _ = fallible_call();`** — used in: `sub.publish(...)` (broadcast
  channel, no-subscribers is normal; see comment at
  [submission.rs:376](crates/nix-ci-core/src/dispatch/submission.rs:376));
  `shutdown_tx.send(true)` (watch channel, receiver may be gone during
  shutdown); `state.dispatcher.claims.take(cid)` (under a race-close
  guard). Each one I checked has a comment or is inside a defensive
  cleanup path.
- **Generic "string" errors** — none I found. Internal `Error::Internal(format!(...))`
  strings are always context-ful and routed to tracing before being
  sanitized on the 5xx wire path.
- **Over-broad `catch + rewrap`** — the client-side `decode()` helper
  at [http.rs:504](crates/nix-ci-core/src/client/http.rs:504) maps HTTP
  statuses back into typed `Error` variants, preserving `NotFound` /
  `Gone` / `Unauthorized` / `PayloadTooLarge` distinctions. Only the
  long tail of "other 4xx" collapses to `BadRequest`. Reasonable.

Overall, the error handling is in senior-shop shape.

## 10. Trait / API design

- `LogStore` async trait ([durable/logs.rs:77](crates/nix-ci-core/src/durable/logs.rs:77))
  uses `#[async_trait]` with `Send + Sync + 'static` — required for the
  `Arc<dyn LogStore>` injected into `AppState`. Correct use.
- Newtype pattern is consistent: `JobId(Uuid)`, `ClaimId(Uuid)`,
  `DrvHash(String)`. Each gets `Debug`, `Clone`, `PartialEq`, `Eq`,
  `Hash` (and `Copy` for the Uuid-backed ones), plus `serde(transparent)`
  so wire bytes are just the inner value. No mixing-up of IDs at the
  type level.
- No over-use of trait objects: the one `dyn LogStore` swap-point is
  deliberate (planning an S3/GCS backend) and documented.
- No `Sealed` trait pattern yet. The only public trait (`LogStore`) is
  intended to be implementable externally (storage backends), so not
  sealing is correct.
- `Default` derivations are narrow: `Submissions`, `Claims`,
  `StepsRegistry`, and the `Deserialize`-driven wire structs where
  serde's `default` attribute needs them. Nothing gratuitous.

**One minor finding**: `BuildLogUploadMeta<'a>` borrows `drv_hash`
([client/http.rs:31](crates/nix-ci-core/src/client/http.rs:31)) but all
other fields are owned. The doc comment justifies it ("so the worker
doesn't have to clone the drv_hash on the hot path"). The struct is
still `Copy` on the borrow and owned otherwise — a reasonable choice
given `DrvHash` is a `String` wrapper. No action needed.

## 11. Async — closer reading

### 11.1 Cancellation safety (`tokio::select!` audit)

Every `tokio::select!` site I inspected either races (a) cancel-safe
primitives (`watch::Receiver::changed`, `broadcast::Receiver::recv`,
`tokio::time::sleep`, `Notify::notified`), or (b) the build child's
`wait()` (also cancel-safe, tokio handles reaping) against a cancel
edge where the drop side issues `start_kill()` + `wait().await` in the
cancel arm. Notable sites:

- [`worker.rs:624-652`](crates/nix-ci-core/src/runner/worker.rs:624)
  `build()` — races `child.wait()` / `shutdown.changed()` / `timeout_fut`.
  On cancel or timeout, explicitly `start_kill()` + `wait().await` +
  `tail_handle.await` before returning. Zombie-safe.
- [`server/claim.rs:106`](crates/nix-ci-core/src/server/claim.rs:106)
  `claim()` main loop — races `notify.notified()` / `sleep_until(deadline)`.
  Both are cancel-safe.
- [`runner/submitter.rs:72`](crates/nix-ci-core/src/runner/submitter.rs:72)
  — races `eval_rx.recv()` / `shutdown.changed()`. Both cancel-safe.
  Comment explicitly notes shutdown propagation reasoning.
- [`runner/sse.rs:67-131`](crates/nix-ci-core/src/runner/sse.rs:67) — two
  `select!` sites, both against `shutdown.changed()`. The stream
  `next()` path is cancel-safe; the `events_request().send()` future
  races cleanly.
- [`durable/reaper.rs:47-52`](crates/nix-ci-core/src/durable/reaper.rs:47),
  `cleanup.rs:27-33` — ticker vs shutdown. Standard pattern.

No `MutexGuard`-across-await instances flagged by the workspace
`await_holding_lock = "deny"` lint, and I didn't spot any manually.
The 8-invariant comment block at the top of `dispatch/mod.rs` makes
invariant 5 ("No `await` under a lock") an explicit contract.

### 11.2 Blocking-in-async audit

`std::fs::*` occurrences in src:

| Call site | Assessment |
|---|---|
| [`config.rs:227`](crates/nix-ci-core/src/config.rs:227) | Startup only. Fine. |
| [`config.rs:385`](crates/nix-ci-core/src/config.rs:385) | Bearer-token file load at startup. Fine. |
| [`runner/drv_walk.rs:61`](crates/nix-ci-core/src/runner/drv_walk.rs:61) | Called via `tokio::task::spawn_blocking` at [submitter.rs:223](crates/nix-ci-core/src/runner/submitter.rs:223). Correct. |
| [`runner/eval_jobs.rs:103`](crates/nix-ci-core/src/runner/eval_jobs.rs:103) | `File::create` during `Command::spawn` setup in `spawn()`. Setup-time, synchronous context before awaiting anything. Fine. |
| [`runner/artifacts.rs:25,57,155`](crates/nix-ci-core/src/runner/artifacts.rs:25) | **Called from the async `collect_failure_logs` handler after run completion.** Writes one file per originating failure to `<artifacts_dir>/build_logs/<drv_name>.log`. Bounded (typically ≤10 failures, each ≤64 KiB); runs once per run termination, not on the hot path. **Potential tightening**: wrap in `spawn_blocking`. Low priority — not materially async-unfriendly at runner scale (one process, one run). |

No `reqwest::blocking`, no `.blocking_recv()`, no `std::process::Command`
(tokio's async `Command` used throughout).

### 11.3 Task lifecycle

- Every `tokio::spawn` I found either lives on a `JoinSet` (worker's
  `build_and_report` pool), is held by a `JoinHandle` tied to a
  `AbortOnDrop` guard ([worker.rs:786-792](crates/nix-ci-core/src/runner/worker.rs:786),
  the lease-refresh guard), or is a long-running loop (reaper, cleanup,
  SSE, submitter, worker, heartbeat) that observes a `watch<bool>`
  shutdown signal.
- `drain_background_tasks` at [server/mod.rs:115](crates/nix-ci-core/src/server/mod.rs:115)
  bounds each handle's drain at `graceful_shutdown_secs` and aborts on
  overrun, explicitly citing the "stuck task wedges `pool.close()`"
  failure mode. Tested.
- All external HTTP calls go through `reqwest::Client` configured with
  a 75s timeout ([client/http.rs:51](crates/nix-ci-core/src/client/http.rs:51));
  the client-side `send_claim` adds `wait_secs + 15` to cover long-poll.
  DB calls go through a pool with `acquire_timeout(10s)` and a
  per-connection `statement_timeout` applied in
  `after_connect` ([durable/mod.rs:36](crates/nix-ci-core/src/durable/mod.rs:36)).
- `axum::serve(...).with_graceful_shutdown(...)` + explicit
  `pool.close().await` at the end. Correct.

### 11.4 Bounds & Pin

The one hand-pinned future is in `build()` at
[worker.rs:623](crates/nix-ci-core/src/runner/worker.rs:623):
`tokio::pin!(timeout_fut)` — required because the inner branch uses
`&mut timeout_fut`. Conventional.

## 12. Testing

### 12.1 Coverage by layer

- **Unit tests** in `#[cfg(test)] mod tests` inside many source files
  (error.rs, submission.rs, steps.rs, claim.rs, observability/mod.rs,
  server/complete.rs truncate_log tests, server/router.rs extractor
  tests, runner/output.rs state-machine, runner/worker.rs
  classify_stderr + backoff + build_timeout, runner/submitter.rs
  attr_is_cached + BoundedCache, runner/eval_jobs.rs parse_eval_line,
  runner/drv_parser.rs ATerm parse, runner/artifacts.rs sanitize,
  durable/logs.rs). Most test the public API; a few reach into
  `pub(crate)` helpers (`truncate_log`, `placeholder_name_from`,
  `drv_hash_from_path`) — defensible given the invariants they cover.
- **Integration tests** in `tests/` — 50+ files, covering auth, HTTP
  E2E, SSE, reaper, cleanup, cancel, backoff, claim lifecycle, ingest
  validation, observability, memory bounds, and more.
- **Property tests** — `tests/property.rs` (626 lines), `tests/drv_parser_fuzz.rs`.
- **Chaos + sim** — `tests/chaos.rs` (feature-gated `chaos-test`),
  `tests/sim.rs` (feature-gated `sim-test`, L2 deterministic).
- **Scale / degradation** — `tests/scale.rs` (feature-gated
  `scale-test`, 653 lines), `tests/degradation.rs` (feature-gated
  `degradation-test`), `tests/pg_faults.rs`.
- **Loom model tests** — `nix-ci-loom-checks/tests/invariants.rs` —
  deliberately small (two invariants), deliberately acknowledged as
  drift-risky (the header comment says so outright).
- **Benchmarks** — `benches/dispatcher.rs` via criterion; comment at
  [complete.rs:280](crates/nix-ci-core/src/server/complete.rs:280)
  notes that `compute_used_by_attrs_for_bench` is exposed hidden
  specifically for the bench suite.

### 12.2 Test-style notes
- `#[tokio::test(start_paused = true)]` with `tokio::time::advance` is
  used idiomatically for time-dependent tests (recent commit message
  "speed up claim_lease via tokio::time::pause + advance" confirms).
- `sqlx::test` for Postgres-dependent integration tests; `common/mod.rs`
  spawns an in-process axum server.
- One `#[test] #[ignore]` marks `parse_all_store_drvs` — manual run
  against a live `/nix/store`. Idiomatic use of `#[ignore]`.
- No `thread::sleep` in async tests I saw outside of deliberate
  "let FIFO Instant resolve" helpers.
- CI parallelism tuned: `--test-threads=4` (recent commit).

### 12.3 Small finding
[`durable/logs.rs:281`](crates/nix-ci-core/src/durable/logs.rs:281) has
`unreachable!("not exercised")` in a test-only `SeededStore::put` impl
— fine. [`runner/artifacts.rs:121`](crates/nix-ci-core/src/runner/artifacts.rs:121)
has `unreachable!()` at the end of an `unused` counter-suffix loop; the
loop runs `for i in 2..` (u64 iteration count), so the exit condition
is the `used.insert` returning true. `unreachable!()` with no message
is the weakest form — adding `"unique_filename loop exhausted u64 range"`
would be a belt-and-suspenders improvement, but realistically that loop
will terminate on iteration 2–3 in every realistic scenario.

## 13. Dependency / build hygiene

- `cargo check --workspace --all-targets` clean.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
  Workspace lints include `await_holding_lock = deny`,
  `await_holding_refcell_ref = deny`, `unused_must_use = deny`,
  `dead_code = warn`, `unused_imports = warn`.
- `deny.toml` configured with advisory, license, ban policies.
- No `#[allow(clippy::all)]` blanket allows. Specific targeted allows
  (`clippy::too_many_arguments` on `upload_log` and
  `upload_log_best_effort`; `clippy::mutable_key_type` on one test).
- `Cargo.lock` committed.
- `cargo tree --duplicates` shows base64/chrono/crypto-common/hmac
  transitive duplicates — all upstream-driven, none nix-ci-fixable.
- `cargo update` / `cargo audit` / `cargo +nightly udeps` were **not**
  re-run in this pass. The `deny.toml` already ignores
  RUSTSEC-2026-0097 (rand, test-only) with justification.
- No `cargo-semver-checks` — `publish = false` on all crates makes it
  optional.

## 14. Summary

This is a mature codebase that has already been through most of what a
senior-Rust reviewer would ask for:

- Clear async contract ("no await under lock", enforced by clippy lint).
- Scoped unsafe (none in production; test-only with safety comments).
- Domain-specific error type with explicit 4xx/5xx sanitization and
  tests that lock in the contract.
- Deliberate `tokio::time::Instant` policy so deterministic simulation
  and paused-time tests work.
- Structured concurrency via `JoinSet` + watch-channel shutdown.
- Graceful shutdown with bounded drain + abort fallback.
- Five-layer test strategy (L1 loom / L2 sim / L3 scale / L4 degradation
  / L5 chaos) per the memory log.

### Material findings (in descending priority)

1. **`artifacts::collect_failure_logs` does `std::fs::write` on the
   async task** after a run ends. Low-impact (≤10 files, ≤64 KiB each,
   once per run) but not strictly correct. Tightening to
   `spawn_blocking` would make it contract-clean. — Small fix.

2. **No declared MSRV** (`rust-version = "..."` absent). Declaring it
   would let CI block accidental feature bumps in a PR. — One-line add.

3. `artifacts::unique_filename` has a bare `unreachable!()` at the end
   of a loop that could in theory never terminate. Adding a message
   clarifies intent; realistically the loop exits on iteration 2–3. — Cosmetic.

4. **`config::max_attempts: i32`** forces a `< 1` check where `u32` and
   `NonZeroU32` would make the invariant type-level. Not worth breaking
   the JSON config schema for. — Deferred.

5. `#[non_exhaustive]` on `JobStatus`, `ErrorCategory`, `JobEvent`
   would be useful if this crate were ever published. Not yet needed. — Deferred.

### Tools not re-run in this pass
- `cargo +nightly miri` (no unsafe to check).
- `cargo +nightly udeps` — not re-run; worth doing.
- `cargo audit` — not re-run; worth doing.
- `cargo-semver-checks` — not applicable (publish = false).

### Recommendation

The codebase does not need a multi-phase rewrite. A short, targeted
follow-up would address the items in §14; beyond that, further
investment is better spent on product work than on style cleanup.
