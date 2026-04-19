# Deploying nix-ci for high availability

This document describes the primary/standby deployment pattern that
the NixOS `coordinator` module supports out of the box. No
application code beyond what ships today is required.

## The mental model

The coordinator's in-memory dispatcher is authoritative for in-flight
state; Postgres carries the durable envelope (terminal job rows,
`failed_outputs` TTL cache, `build_logs`). To avoid two coordinators
racing each other on the same Postgres, the coordinator acquires a
session-scoped **advisory lock** at boot, on a deterministic constant
key. Only one coordinator process holds the lock at a time; any
others block in `pg_advisory_lock()` until they can acquire it.

That's the whole HA mechanism. Run N identical coordinator instances
pointing at the same Postgres and the same lock key:

- One wins the lock on startup → it's the **primary**. It runs
  `clear_busy`, starts axum, serves traffic.
- Every other instance blocks in `acquire` → they're **standbys**.
  Their process is alive (`systemctl is-active` → yes) but axum
  never starts and the listen port is never bound. Requests to
  their port get ECONNREFUSED.
- When the primary's process exits for any reason (graceful
  `systemctl stop`, crash, SIGKILL, host power-off), its dedicated
  advisory-lock PgConnection drops. Postgres releases the lock.
  Exactly one standby unblocks, becomes the new primary, runs its
  own `clear_busy`, and starts serving.

**Measured failover time:** 1 second against real TCP + a real
`docker kill --signal=KILL` (see `scripts/orbstack_harness/ha-failover.sh`
and `nix/checks/coordinator-ha-failover.nix`). The latency is
dominated by how fast Postgres notices the dead client socket — near-
instant for a clean TCP close, sub-second even for an abrupt kill on
localhost.

## What survives vs what doesn't

| State | Survives failover? |
|---|---|
| Terminal `jobs.result` JSONB rows | **Yes** (Postgres) |
| `failed_outputs` TTL cache entries | **Yes** (Postgres) |
| `build_logs` archive | **Yes** (Postgres) |
| In-flight jobs (submission, step graph, claims) | **No** — flipped to `cancelled` by `clear_busy` on the new primary |
| Active SSE subscribers | **No** — existing connections break; the runner's SSE loop returns `Err(Internal("SSE /events returned 404"))` (see `runner_sse_fails_cleanly_when_coord_unavailable`) |

This is the C6 trade in SPEC.md: **failover is cheap; in-flight work
is not.** The caller (CCI) is expected to retry a cancelled job with
the same `external_ref` — idempotent on the `jobs_external_ref_uniq`
index — to pick up a cached terminal row if one was written before
the failover, or to submit a fresh job otherwise.

## NixOS configuration

Two hosts, same Postgres, same `lockKey`:

```nix
# Shared across hosts: point at the same Postgres.
let
  pgUrl = "postgres://nix_ci@pg.internal/nix_ci";
in {
  # host: coordinator-primary.example.com
  services.nix-ci-coordinator = {
    enable = true;
    databaseUrl = pgUrl;
    bind = "0.0.0.0:8080";
    # lockKey defaults to the shared constant
    # (NIX_CI_COORDINATOR_LOCK_KEY) — leaving it unset is correct.
    authBearer = "/run/keys/nix-ci-bearer";
    adminBearer = "/run/keys/nix-ci-admin-bearer";
  };

  # host: coordinator-standby.example.com — identical config.
  services.nix-ci-coordinator = {
    enable = true;
    databaseUrl = pgUrl;
    bind = "0.0.0.0:8080";
    authBearer = "/run/keys/nix-ci-bearer";
    adminBearer = "/run/keys/nix-ci-admin-bearer";
  };
}
```

Scale to 3+ hosts by repeating the config on additional nodes. Only
one ever serves at a time.

### Why `DynamicUser + LoadCredential`

The module uses systemd's `DynamicUser = true` and `LoadCredential`
for bearer tokens so the token never appears in `/proc/*/environ` or
in the config JSON. Both primary and standby hosts need the same
token values for workers to keep claiming after failover without
re-authenticating. Copy credential files to the same path on each
host.

## Load balancer configuration

Point a TCP or HTTP load balancer at each coordinator's `/readyz`:

- **Primary:** `/readyz` → 200.
- **Standby:** port not bound → connection refused; LB marks it
  DOWN. No handler-level 503 to misconfigure; this is an OS-level
  effect.
- **Draining primary (operator ran `POST /admin/drain`):** `/readyz`
  → 503 `draining`; LB stops sending new connections. Existing
  in-flight requests finish normally.
- **Overloaded primary (`claims_in_flight >= max_claims_in_flight`):**
  `/readyz` → 503 `overloaded`; LB steers to another replica if
  one is available. (In single-primary HA there won't be — the
  standby is still blocked in `acquire`. This signal is mainly for
  operator dashboards.)

### Example: HAProxy

```
frontend nix-ci
    bind *:80
    default_backend coord

backend coord
    option httpchk GET /readyz
    http-check expect status 200
    server primary   coord-a.example.com:8080 check inter 1s fall 2 rise 2
    server standby   coord-b.example.com:8080 check inter 1s fall 2 rise 2
```

With `fall 2 rise 2` at `inter 1s`, an LB notices primary death
within ~2 seconds and starts routing to the (newly-promoted) standby
as soon as its `/readyz` flips to 200.

### Example: Kubernetes

```yaml
apiVersion: v1
kind: Service
metadata:
  name: nix-ci-coordinator
spec:
  selector:
    app: nix-ci-coordinator
  ports:
  - port: 8080
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nix-ci-coordinator
spec:
  replicas: 2
  serviceName: nix-ci-coordinator
  template:
    spec:
      containers:
      - name: coord
        image: nix-ci:<tag>
        ports: [ { containerPort: 8080 } ]
        readinessProbe:
          httpGet: { path: /readyz, port: 8080 }
          periodSeconds: 1
          failureThreshold: 2
        livenessProbe:
          httpGet: { path: /healthz, port: 8080 }
          periodSeconds: 5
```

`readinessProbe` on `/readyz` ensures the standby pod stays out of
service rotation (probe fails because the port is refused).
`livenessProbe` on `/healthz` would ALSO fail on the standby because
the port isn't bound. That's fine for our purposes — a "liveness"
failure triggers a pod restart, which re-acquires the lock from
scratch. Alternatively, expose a process-level liveness check (e.g.
`/proc/$PID/status` from a shell) if you don't want the standby pod
to restart perpetually.

## Rolling deploy procedure

**Zero-work-loss deploy** (the normal case):

1. `POST /admin/drain` on the current primary. New claims return
   204; new ingests return 503; new jobs return 503. Existing
   in-flight work continues.
2. Poll `GET /admin/drain` until `in_flight_claims == 0` and
   `open_submissions == 0`.
3. Deploy the new binary to the standby host. Do NOT touch the
   primary yet.
4. `systemctl restart nix-ci-coordinator` on the standby → it exits
   → `systemctl start` runs the new binary → it blocks on the
   advisory lock again (primary still holds it).
5. `systemctl stop nix-ci-coordinator` on the primary → graceful
   shutdown → advisory lock releases → the (freshly-upgraded)
   standby unblocks and becomes the new primary.
6. Load balancer notices `/readyz` flip within ~2 seconds; traffic
   flows to the new primary.
7. Deploy the new binary to the old primary. `systemctl start`
   brings it up as the new standby.

**Steps that are NOT optional:**

- Step 1 (drain) is what makes the deploy zero-work-loss. Without
  it, in-flight claims at the moment of step 5 are cancelled by
  `clear_busy` on the new primary. Not a correctness issue — the
  caller's external_ref retry handles it — but it wastes worker
  compute on drvs that will be rebuilt.
- Step 4 (upgrade standby first) matters because the new binary's
  advisory-lock semantics must be compatible with the old binary's.
  Same constant lockKey means they will interleave correctly even
  during the deploy; no change to `lockKey` should ever ship without
  a full drain first.

**Emergency deploy** (primary broken, can't drain):

- `systemctl stop --no-block` or `kill -KILL` the broken primary.
  Standby takes over within 1 second (nixos-test measured; orbstack
  measured with SIGKILL + same result).
- In-flight work on the broken primary is lost; clear_busy flips
  those jobs to `cancelled`. Callers retry via external_ref.
- Deploy the new binary to the old primary host normally; it
  becomes the new standby.

## Postgres configuration

A single shared Postgres. This service can also be HA'd — use patroni
or a managed Postgres offering. The coordinator cares only about:

- Reachability from both hosts (`pg_hba.conf` opens 5432 to both).
- Same `lockKey` database — advisory locks are per-database.
- Shared `nix_ci` schema — migrations are idempotent (sqlx-migrate)
  and run at every coordinator startup. Primary runs them; standby
  also runs them on its turn, which is a no-op if the primary already
  migrated.

Advisory locks are session-scoped and release on session end — no
cleanup cron, no stale-lock detection needed.

## Monitoring / alerting

Key metrics to watch:

- **`nix_ci_jobs_reaped_total`** — a spike after a failover is
  expected (pre-failover in-flight jobs getting cancelled). Sustained
  non-zero without a deploy event means worker/network flakes,
  not HA.
- **`nix_ci_terminal_writeback_retry_finalized_total`** — non-zero
  after a failover is suspicious: it means the new primary is
  finalizing jobs whose terminal write failed on the old primary.
  Usually benign (PG flake during the exact `/complete` that
  finalized a job) but worth investigating.
- **`nix_ci_overload_rejections_total`** — non-zero on the new
  primary immediately after failover suggests the workload was
  already at shedding capacity when the primary died. Consider
  raising `max_claims_in_flight`.
- **`nix_ci_pg_pool_acquire_duration_seconds`** p99 — a p99 above
  100ms on either host = Postgres itself is the bottleneck, not
  the coordinator.

Alert rules (Prometheus):

```yaml
- alert: CoordinatorNotReady
  expr: up{job="nix-ci-coordinator"} == 1 and probe_success{endpoint="/readyz"} == 0
  for: 5m
  # Longer than normal failover — primary AND standby both down.
  labels: { severity: page }

- alert: FailoverOccurred
  expr: changes(nix_ci_jobs_created_total[10m]) > 0 and (time() - process_start_time_seconds < 120)
  for: 1m
  # Informational — someone should know a failover happened.
  labels: { severity: info }
```

## Known gotchas

- **`graceful_shutdown_secs`** bounds axum's own drain. If the
  primary has open SSE subscribers and an operator SIGTERMs without
  draining first, the coordinator waits up to `graceful_shutdown_secs`
  (default 60s) before exiting. During that window the advisory lock
  is still held — the standby can't take over. Drain first, or
  accept the bounded stall.
- **Postgres primary fails over** — if the shared Postgres itself
  fails over (e.g. a patroni promotion), both coordinators lose
  their PG connections, including the advisory-lock-holding one.
  After PG recovers, both coordinators attempt to re-acquire the
  lock. Exactly one wins. In-flight jobs on the pre-failover
  primary are lost the same way as a coordinator failover.
- **Do NOT change `lockKey` without a full drain first.** Two
  coordinators with different `lockKey` values will BOTH acquire
  their respective locks and BOTH start serving — split brain. The
  single default constant is chosen so operators never need to think
  about this.

## Related references

- `nix/nixosModules/coordinator.nix` — the module itself.
- `nix/checks/coordinator-ha-failover.nix` — three-node VM test
  proving the failover contract; runs under `nix flake check`.
- `scripts/orbstack_harness/ha-failover.sh` — real-TCP Docker-based
  scenario with a `docker kill --signal=KILL` primary stop.
- `SPEC.md` section "Known deferred" (C6) — the trade this HA model
  is built on.
- `SCALE.md` — degradation contract details (shedding, drain,
  quarantine, etc.) that layer on top of the HA mechanism.
