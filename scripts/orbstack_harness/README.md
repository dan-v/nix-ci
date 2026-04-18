# L3.5 OrbStack harness

Out-of-process scale + reliability tests that run the real coordinator
binary against many mock-worker containers. Complements the
in-process `scale_xl.rs` harness by breaking past the single-process
ephemeral-port ceiling (~1500 workers on macOS) and exercising real
network hops between coordinator and workers.

## Prerequisites

- OrbStack's Docker engine (already set up for this project).
- The two long-running containers: `nix-ci-rust` (for cargo builds)
  and `nix-ci-pg` (Postgres 16).
- Release binaries built inside `nix-ci-rust`:

  ```
  docker exec -w /work/.claude/worktrees/<worktree>/rust nix-ci-rust \
    cargo build --release -p nix-ci -p nix-ci-harness
  ```

## Profiles

- `wide-fleet` — 50 containers × 50 mock workers (2500 fleet workers)
  claiming zero-duration work against a synthetic 10k-drv DAG.
  Primary assertion: job reaches `Done`, zero failures, no
  overload_rejections.
- `sick-worker-contained` — 5 healthy + 3 sick worker containers. Sick
  workers fail every claim with `BuildFailure`. Assertions: quarantine
  counter increments, no coordinator panics, job reaches terminal,
  `/admin/refute` clears the failed-outputs cache.

## Usage

```
./scripts/orbstack_harness/run.sh wide-fleet
./scripts/orbstack_harness/run.sh sick-worker-contained
```

Reports land under `scripts/orbstack_harness/out/`:
- `report.json` — driver report, SLO pass/fail
- `worker-*.json` — per-container mock-worker summaries

## Teardown

`run.sh` cleans up via EXIT trap. For stuck runs:

```
./scripts/orbstack_harness/run.sh teardown
```
