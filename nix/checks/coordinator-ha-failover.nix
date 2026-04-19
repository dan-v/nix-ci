{ pkgs, self, system }:
# NixOS VM test for the HA primary/standby contract: two coordinators
# pointed at the same Postgres with the same advisory-lock key MUST
# behave as follows:
#
#   1. Whichever reaches `pg_advisory_lock` first becomes primary.
#      Its axum server binds and serves — /readyz returns 200.
#   2. The other BLOCKS inside `CoordinatorLock::acquire` and
#      therefore never runs `axum::serve` — its :8080 port is NOT
#      bound, so `curl` against it returns connection-refused.
#      (This is cleaner than a "standby returns 503" signal —
#      advisory-lock blocking is an OS-level effect that can't be
#      accidentally subverted by handler-level logic.)
#   3. When the primary's process exits (for any reason — SIGTERM,
#      SIGKILL, crash), its dedicated CoordinatorLock connection
#      drops; Postgres releases the advisory lock; the standby
#      unblocks, runs `clear_busy`, and starts serving. Failover
#      time is bounded by how fast PG notices the connection drop.
#   4. Workloads submitted to the new primary work end-to-end.
#
# Load balancer pattern: point the LB at both nodes' /readyz. Only
# the primary answers 200; the standby's port is refused. The LB
# routes all traffic to the primary without any coordination layer.
pkgs.nixosTest {
  name = "nix-ci-coordinator-ha-failover";
  nodes = {
    # Shared Postgres on its own node. Both coordinators reach it
    # over the NixOS test's private network; coord-a and coord-b
    # DON'T share systemd, so clear_busy / advisory-lock sessions
    # are genuinely cross-host.
    db = { config, pkgs, ... }: {
      services.postgresql = {
        enable = true;
        ensureDatabases = [ "nix_ci" ];
        ensureUsers = [{
          name = "nix_ci";
          ensureDBOwnership = true;
        }];
        authentication = pkgs.lib.mkOverride 10 ''
          local all all trust
          host all all 0.0.0.0/0 trust
        '';
        settings = {
          listen_addresses = pkgs.lib.mkForce "'*'";
        };
      };
      networking.firewall.allowedTCPPorts = [ 5432 ];
    };

    # Primary candidate.
    coord_a = { config, pkgs, ... }: {
      imports = [ self.nixosModules.coordinator ];
      services.nix-ci-coordinator = {
        enable = true;
        databaseUrl = "postgres://nix_ci@db/nix_ci";
        bind = "0.0.0.0:8080";
        # Same lockKey as coord_b — that's what makes them a pair.
        # lockKey defaults to NIX_CI_COORDINATOR_LOCK_KEY; leaving
        # it at the default confirms the production default is
        # itself HA-ready without operator tuning.
      };
      networking.firewall.allowedTCPPorts = [ 8080 ];
      environment.systemPackages = [ pkgs.curl pkgs.jq ];
    };

    # Standby candidate. Identical config; advisory lock decides
    # who wins at boot time.
    coord_b = { config, pkgs, ... }: {
      imports = [ self.nixosModules.coordinator ];
      services.nix-ci-coordinator = {
        enable = true;
        databaseUrl = "postgres://nix_ci@db/nix_ci";
        bind = "0.0.0.0:8080";
      };
      networking.firewall.allowedTCPPorts = [ 8080 ];
      environment.systemPackages = [ pkgs.curl pkgs.jq ];
    };
  };

  testScript = ''
    import json
    import time

    # ── Phase 1: bring up DB, then coord_a as primary ──────────────
    db.start()
    db.wait_for_unit("postgresql.service")

    coord_a.start()
    coord_a.wait_for_unit("nix-ci-coordinator.service")
    # /readyz is the load balancer signal. The primary (advisory-lock
    # holder) answers 200 once clear_busy + migrations complete.
    coord_a.wait_until_succeeds("curl -fsS http://127.0.0.1:8080/readyz")

    # ── Phase 2: coord_b boots as standby ──────────────────────────
    # Critical: coord_b's systemd service DOES start (ExecStart runs),
    # but the process itself blocks inside CoordinatorLock::acquire
    # before axum::serve is even called. From the outside this looks
    # like "port 8080 never binds" — curl gets connection-refused,
    # not 503. That's the right answer: the LB naturally avoids
    # routing to a port that isn't listening.
    coord_b.start()
    coord_b.wait_for_unit("nix-ci-coordinator.service")

    # Give coord_b a moment to reach `CoordinatorLock::acquire`.
    time.sleep(3)

    # coord_b's :8080 port is refused — it's blocked inside the
    # acquire call, before axum binds. `curl -fsS` fails with
    # "connection refused" (exit code 7). Use `fail()` to assert
    # the command returned non-zero.
    coord_b.fail("curl -fsS http://127.0.0.1:8080/readyz")
    coord_b.fail("curl -fsS http://127.0.0.1:8080/healthz")

    # Verify the coord_b process is still running (it's just blocked).
    # systemctl is-active proves the service didn't crash-exit.
    coord_b.succeed("systemctl is-active nix-ci-coordinator.service")

    # Sanity: coord_a still serves — failover hasn't happened.
    coord_a.succeed("curl -fsS http://127.0.0.1:8080/readyz")

    # ── Phase 3: establish a job on coord_a so we can verify
    #            clear_busy fires on failover ─────────────────────────
    resp = coord_a.succeed(
        "curl -fsS -X POST http://127.0.0.1:8080/jobs "
        "-H 'content-type: application/json' "
        "-d '{\"external_ref\": \"pre-failover\"}'"
    )
    job_id = json.loads(resp)["id"]
    # Ingest one drv so the job is non-empty and clear_busy's
    # UPDATE has a concrete row to flip.
    drv_body = json.dumps({
        "drvs": [{
            "drv_path": "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-ha-leaf.drv",
            "drv_name": "ha-leaf",
            "system": "x86_64-linux",
            "is_root": True,
            "required_features": [],
            "input_drvs": []
        }],
        "eval_errors": []
    })
    coord_a.succeed(
        f"curl -fsS -X POST http://127.0.0.1:8080/jobs/{job_id}/drvs/batch "
        f"-H 'content-type: application/json' "
        f"-d '{drv_body}'"
    )
    # Don't seal — we want a live pending job clear_busy will cancel.

    # ── Phase 4: kill coord_a's coordinator; coord_b takes over ────
    # `systemctl stop` sends SIGTERM → the bounded graceful shutdown
    # runs → process exits → dedicated advisory-lock connection drops
    # → PG releases the lock → coord_b's acquire unblocks.
    coord_a.succeed("systemctl stop nix-ci-coordinator.service")

    # coord_a is no longer serving.
    coord_a.fail("curl -fsS http://127.0.0.1:8080/readyz")

    # coord_b unblocks within a few seconds. PG session-drop
    # detection depends on TCP-keepalive / idle timers; NixOS test
    # VMs are local so this is fast in practice, but we give a
    # generous 30s window.
    coord_b.wait_until_succeeds(
        "curl -fsS http://127.0.0.1:8080/readyz",
        timeout=30,
    )

    # ── Phase 5: clear_busy ran on coord_b at boot — the pre-failover
    #            job is now cancelled with the sentinel eval_error ──
    pre_status = coord_b.succeed(
        f"curl -fsS http://127.0.0.1:8080/jobs/{job_id}"
    )
    pre_parsed = json.loads(pre_status)
    assert pre_parsed["status"] == "cancelled", (
        f"pre-failover job must be flipped to cancelled by clear_busy; "
        f"got {pre_parsed}"
    )
    assert pre_parsed.get("eval_error", "") == "coordinator restarted; job aborted", (
        f"clear_busy sentinel missing or wrong: {pre_parsed}"
    )

    # ── Phase 6: a fresh job on coord_b works end-to-end ───────────
    resp2 = coord_b.succeed(
        "curl -fsS -X POST http://127.0.0.1:8080/jobs "
        "-H 'content-type: application/json' "
        "-d '{\"external_ref\": \"post-failover\"}'"
    )
    new_job_id = json.loads(resp2)["id"]
    assert new_job_id != job_id, "new job must have a fresh id"

    # Seal the empty new job — with no toplevels it terminalizes
    # immediately as Done. This is the minimum end-to-end proof
    # that coord_b's full request → dispatcher → terminal write
    # → JSONB persist path works.
    coord_b.succeed(
        f"curl -fsS -X POST http://127.0.0.1:8080/jobs/{new_job_id}/seal"
    )
    for _ in range(20):
        s = json.loads(coord_b.succeed(
            f"curl -fsS http://127.0.0.1:8080/jobs/{new_job_id}"
        ))
        if s["status"] == "done":
            break
        time.sleep(0.5)
    else:
        raise Exception(f"post-failover job did not terminalize Done: {s}")

    # ── Phase 7: confirm coord_a can re-start as standby ───────────
    # Start coord_a back up. It must now be the standby: block in
    # CoordinatorLock::acquire, port not bound. Roles have swapped.
    coord_a.succeed("systemctl start nix-ci-coordinator.service")
    time.sleep(3)
    coord_a.fail("curl -fsS http://127.0.0.1:8080/readyz")

    # And coord_b still holds the lock + still serves.
    coord_b.succeed("curl -fsS http://127.0.0.1:8080/readyz")
  '';
}
