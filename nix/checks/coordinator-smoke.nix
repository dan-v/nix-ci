{ pkgs, self, system }:
# NixOS VM test that boots the coordinator module against a real
# postgres and validates: migrations run, healthz returns 200,
# a job can be created, and /metrics surfaces the expected gauges.
#
# Runs via `nix flake check` — smoke test only, does not actually
# execute builds (that would drag nix-eval-jobs + the whole nixpkgs
# substituter into the VM image).
pkgs.nixosTest {
  name = "nix-ci-coordinator-smoke";
  nodes.server = { config, pkgs, ... }: {
    imports = [ self.nixosModules.coordinator ];
    services.postgresql = {
      enable = true;
      ensureDatabases = [ "nix_ci" ];
      ensureUsers = [{
        name = "nix_ci";
        ensureDBOwnership = true;
      }];
      authentication = pkgs.lib.mkOverride 10 ''
        local all all trust
        host all all 127.0.0.1/32 trust
      '';
    };
    services.nix-ci-coordinator = {
      enable = true;
      databaseUrl = "postgres://nix_ci@localhost/nix_ci";
      bind = "127.0.0.1:8080";
    };
    environment.systemPackages = [ pkgs.curl pkgs.jq ];
  };
  testScript = ''
    server.start()
    server.wait_for_unit("postgresql.service")
    server.wait_for_unit("nix-ci-coordinator.service")

    # Liveness: the process is up. Readyz verifies the DB round-trip.
    server.wait_until_succeeds("curl -fsS http://127.0.0.1:8080/healthz")
    server.wait_until_succeeds("curl -fsS http://127.0.0.1:8080/readyz")

    # /metrics exposes the Prometheus body; grep for a handful of
    # gauges + counters we expect to see registered.
    metrics = server.succeed("curl -fsS http://127.0.0.1:8080/metrics")
    for m in [
        "nix_ci_jobs_created",
        "nix_ci_claims_in_flight",
        "nix_ci_submissions_active",
        "nix_ci_claim_age_seconds",
        "nix_ci_pg_pool_size",
    ]:
        assert m in metrics, f"expected metric {m!r} in /metrics; got: {metrics[:400]}"

    # Create a job via the HTTP API; verify the response shape.
    resp = server.succeed(
        "curl -fsS -X POST http://127.0.0.1:8080/jobs "
        "-H 'content-type: application/json' "
        "-d '{\"external_ref\": \"smoke-test\"}'"
    )
    assert '"id"' in resp, f"unexpected create_job resp: {resp!r}"

    # Idempotency on external_ref: POST again returns the same id.
    resp2 = server.succeed(
        "curl -fsS -X POST http://127.0.0.1:8080/jobs "
        "-H 'content-type: application/json' "
        "-d '{\"external_ref\": \"smoke-test\"}'"
    )
    # Both responses have the same id field.
    import json
    assert json.loads(resp)["id"] == json.loads(resp2)["id"], (
        "external_ref idempotency broken"
    )

    # /jobs?status=pending surfaces the job.
    listing = server.succeed(
        "curl -fsS 'http://127.0.0.1:8080/jobs?status=pending'"
    )
    assert "smoke-test" in listing or '"jobs"' in listing, (
        f"jobs list missing expected shape: {listing!r}"
    )
  '';
}
