# Shadow mode + canary rollout

The recommended path from "nix-ci is ready to ship" to "nix-ci is the
primary CI pipeline" runs through **shadow mode** — parallel execution
of user traffic against both the incumbent CI system and nix-ci, with
outcome comparison — followed by a **canary**: fractional cutover
with automated rollback on SLO breach.

This document describes both patterns. Neither requires coordinator-
side code changes beyond what ships today; the work is almost
entirely in the caller (CCI, GitHub Actions, etc.) and in an
outcome-comparison tool.

## Why shadow before canary

The failure modes we're actually afraid of aren't caught by unit or
integration tests:

- **"Every drv builds correctly, but slower than CCI"** → p99 build
  time regresses; users notice before SREs do.
- **"Correctness under real workloads is subtly different"** → a
  derivation that's flaky-but-eventually-green on CCI might exhaust
  `max_attempts` on nix-ci and terminalize as failed. The failure
  taxonomy itself differs.
- **"The 5th percentile case we didn't think of"** → real nixpkgs
  traffic exercises shapes we don't test (bespoke overlays, vendored
  derivations, cross-compilation closures).

Shadow mode answers these by running nix-ci against real traffic
without putting it in the user-facing path: if nix-ci diverges on a
real build, we notice in the comparison tool, not in a user's Slack
ping. Once divergence rate is below a threshold (say 1 / 10k) for N
weeks, the team has earned the confidence to canary.

## Shadow mode architecture

```
                   user / GitHub PR
                          │
                          ▼
              ┌─────────────────────┐
              │  CCI trigger layer  │
              │  (fires BOTH paths) │
              └──────┬──────────────┘
                     │
        ┌────────────┴────────────┐
        │                         │
        ▼ primary                 ▼ shadow
┌───────────────┐         ┌───────────────────┐
│ existing CCI  │         │  staging nix-ci    │
│   pipeline    │         │   coordinator      │
└───────┬───────┘         └────────┬──────────┘
        │                          │
        │ user-visible             │ shadow-only;
        │ result                   │ no user output
        ▼                          ▼
    build-N.json              build-N-shadow.json
        │                          │
        └──────────┬───────────────┘
                   ▼
          nix-ci-compare
                   │
                   ▼
          divergence report
          (dashboard / alert
           on threshold breach)
```

### Key properties

1. **User traffic is unaffected.** Users see CCI's primary output;
   nix-ci runs entirely in parallel and its results are consumed
   only by the comparison tool.
2. **No coordinator-side "shadow" flag required.** From the
   coordinator's perspective, shadow traffic is normal traffic. The
   distinction ("this run's outcome is not user-facing") lives
   entirely in the caller.
3. **Isolated staging coordinator.** Shadow traffic goes to a
   staging coordinator that's NOT the production primary. Mixing
   them would couple production availability to shadow traffic
   volume and complicate the `max_claims_in_flight` tuning story.
4. **Idempotent external_ref mapping.** The shadow job uses the
   same `external_ref` as the primary job (e.g. the CCI build ID)
   so the comparison tool can pair them without a separate index.
   If the same build retries, both systems see the existing job
   id (idempotent on `jobs_external_ref_uniq`).

### Deployment shape

Add a second coordinator (HA or single-instance — both work, HA is
optional for shadow) pointed at its own Postgres. Extend the CCI
trigger to:

```pseudocode
def on_pr_push(pr):
    primary_result = run_existing_cci_pipeline(pr)      # required
    shadow_result = run_nix_ci(                         # best-effort
        coordinator_url=SHADOW_COORDINATOR_URL,
        external_ref=f"cci-{pr.build_id}",
    )
    # Dump BOTH outcomes for offline comparison.
    emit_json("primary", primary_result)
    emit_json("shadow", shadow_result)
    # primary_result alone drives user-visible status checks.
    return primary_result
```

Set a conservative timeout on the shadow call (say, 2× primary).
If nix-ci is slower, the user's PR UX isn't affected — the shadow
result is marked `timeout` in the divergence report.

### Comparison tool

`nix-ci-compare` (new binary in `nix-ci-harness`) takes two
`JobStatusResponse` JSONs and emits a divergence summary:

```
$ nix-ci-compare primary=<primary.json> shadow=<shadow.json>
{
  "matched": true,
  "status": { "primary": "done", "shadow": "done" },
  "counts_delta": { "total": 0, "failed": 0, "done": 0 },
  "failures_only_in_primary": [],
  "failures_only_in_shadow": [],
  "suspected_worker_infra": { "primary": null, "shadow": null }
}
```

On divergence:

```
{
  "matched": false,
  "divergences": [
    { "kind": "status_mismatch", "primary": "done", "shadow": "failed" },
    { "kind": "failures_only_in_shadow", "drvs": ["gcc-13.2.0"] },
    { "kind": "suspected_worker_infra", "shadow": "host-3" }
  ],
  "severity": "critical"
}
```

Severity rules:
- `critical`: terminal status differs (done-vs-failed). This is a
  correctness divergence — either nix-ci or CCI is wrong.
- `major`: failure set differs (one system failed drvs the other
  didn't). Usually a classifier or retry-budget delta.
- `minor`: counts differ but status matches (e.g. nix-ci retried
  a transient the primary didn't). Usually fine, track as noise.

The comparison tool's input format is simple enough that CCI can
normalize its existing pipeline output into it without coordinator
changes.

## Canary rollout

Once shadow-mode divergence is at acceptable noise levels (say <1%
minor, zero major/critical for 4+ weeks), canary starts.

```
                 user / GitHub PR
                        │
                        ▼
            ┌─────────────────────┐
            │   CCI trigger       │
            │   route by weight   │
            └──────┬──────────────┘
                   │
      ┌────────────┴──────────────┐
      │ P(1 - w)                  │ P(w)
      ▼                           ▼
  existing CCI             production nix-ci
                           (new user-facing path)

  weight w ramps: 1% → 10% → 50% → 100%
  automated rollback on SLO breach
```

Automated rollback triggers (examples):
- `nix_ci_jobs_terminal{status="failed"}` rate on canary-routed
  traffic > 1.5× incumbent failure rate for >10min.
- `nix_ci_claim_age_seconds` p99 > 2× incumbent p99 for >5min.
- `nix_ci_process_panics` > 0 in the canary coordinator.
- `nix_ci_terminal_writeback_retry_finalized_total` spike
  (indicates PG flakes that only the wedge-retry is covering up).

All of these are already exposed by the existing `/metrics`.

### Ramp discipline

- **1% for 1 week.** Enough signal to catch scale-related
  divergence, small enough to recover from any issue manually.
- **10% for 1 week.** First confidence in operations-under-load.
- **50% for 2 weeks.** Half the fleet is on nix-ci; this is the
  point where ops scripts / dashboards / muscle memory must have
  caught up.
- **100%.** Full cutover. Keep the existing CCI pipeline in
  shadow for another 4 weeks as a rollback ramp.

## What nix-ci exposes for both modes

The coordinator already emits the signals shadow-mode and canary
need:

| Signal | Metric / field | Used for |
|---|---|---|
| Terminal status parity | `jobs.status` via `GET /jobs/{id}` | Divergence detection |
| Failure set parity | `jobs.failures[]` | Per-drv divergence |
| Suspected-infra signal | `jobs.suspected_worker_infra` | Attribution of divergence to one host |
| Build duration | `nix_ci_build_duration_seconds` | p99 latency parity |
| Retry behavior | `nix_ci_builds_completed{outcome="flaky_retry"}` | Classifier delta |
| Per-job size | `nix_ci_drvs_per_job` | Shape distribution |
| Log bytes | `nix_ci_build_log_bytes_per_job` | Disk-cost projection |

No new coordinator-side features are needed for shadow or canary.
The entire delta is in the caller and the comparison tooling.

## In-repo tooling

Provided:

- `rust/crates/nix-ci-harness/src/bin/compare.rs` — a small CLI
  that takes two JobStatusResponse JSONs and emits a divergence
  report. Invoked from CCI post-build hooks.

Not provided (intentional — these are operator-specific):

- CCI trigger layer. Every CI system has a different extension
  mechanism. The shadow-call pseudo-code above is the contract;
  implementing it is the operator's job.
- Dashboard + alerting. `nix-ci-compare` emits JSON. Pipe it to
  whatever metrics store / alerting stack the operator uses.
- SLO definitions. Depends on workload characteristics the
  coordinator can't infer.

## Related references

- `SPEC.md` exit-criteria bars — ground truth for "ready for
  canary" (30 consecutive days of nightly-chaos green).
- `SCALE.md` L5 section — the original "shadow + canary" sketch
  this doc expands on.
- `docs/deployment-ha.md` — primary/standby setup; shadow mode
  doesn't require it but production probably does.
- `rust/crates/nix-ci-harness/src/bin/compare.rs` — the outcome-
  comparison CLI.
