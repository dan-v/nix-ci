{ config, lib, pkgs, ... }:
let
  cfg = config.services.nix-ci-coordinator;
  # Parse host/port out of a postgres URL well enough for pg_isready to
  # probe it. Accepts `postgres://[user[:pass]@]host[:port]/db[?...]`.
  # This is deliberately simple: pg_isready itself only uses the host
  # and port, so we don't need to fully parse the URL.
  pgReadyScript = pkgs.writeShellScript "nix-ci-pg-wait" ''
    set -euo pipefail
    url="${cfg.databaseUrl}"
    # Strip scheme
    rest=''${url#*://}
    # Strip auth if present
    if [[ "$rest" == *"@"* ]]; then
      rest=''${rest#*@}
    fi
    # hostport = everything up to the first '/'
    hostport=''${rest%%/*}
    host=''${hostport%%:*}
    port=''${hostport##*:}
    if [[ "$port" == "$host" ]]; then
      port=5432
    fi
    # Poll pg_isready for up to 60s. systemd's own Restart handles
    # longer outages; this loop just smooths over the ~5-15s Postgres
    # takes to come up on a fresh boot.
    for _ in $(seq 1 60); do
      if ${pkgs.postgresql}/bin/pg_isready -h "$host" -p "$port" -q; then
        exit 0
      fi
      sleep 1
    done
    echo "nix-ci: Postgres at $host:$port not ready after 60s" >&2
    exit 1
  '';
in
{
  options.services.nix-ci-coordinator = {
    enable = lib.mkEnableOption "nix-ci coordinator HTTP server";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.nix-ci;
      defaultText = lib.literalExpression "pkgs.nix-ci";
      description = "The nix-ci package to use.";
    };

    databaseUrl = lib.mkOption {
      type = lib.types.str;
      example = "postgres://nix_ci@localhost/nix_ci";
      description = "PostgreSQL connection string.";
    };

    bind = lib.mkOption {
      type = lib.types.str;
      default = "0.0.0.0:8080";
      description = "Address the coordinator HTTP server binds to.";
    };

    lockKey = lib.mkOption {
      type = lib.types.int;
      default = 104369793433141249; # matches NIX_CI_COORDINATOR_LOCK_KEY
      description = ''
        Postgres advisory-lock key used to enforce single-writer.
        Must be identical across any coordinator instances that could
        fail over for this one; the standby blocks on this key.
      '';
    };

    gracefulShutdownSecs = lib.mkOption {
      type = lib.types.int;
      default = 30;
      description = ''
        How long the coordinator waits for in-flight requests and
        background tasks to drain before the process exits on
        SIGTERM. systemd TimeoutStopSec is set to this value plus a
        small grace window.
      '';
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
      example = "debug";
      description = "RUST_LOG level for the coordinator process.";
    };

    logJson = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Emit JSON-formatted logs (for log aggregation).";
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.services.nix-ci-coordinator = {
      description = "nix-ci coordinator";
      after = [ "network-online.target" "postgresql.service" ];
      wants = [ "network-online.target" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        RUST_LOG = cfg.logLevel;
        NIX_CI_LOG_JSON = if cfg.logJson then "1" else "0";
      };

      serviceConfig = {
        Type = "simple";
        # Wait for Postgres to accept connections before starting the
        # coordinator. Without this the coordinator can enter a
        # restart loop on fresh boot when postgresql.service is
        # declared "active" but pg still hasn't finished WAL replay.
        # ExecStartPre runs under the same DynamicUser as ExecStart.
        ExecStartPre = "${pgReadyScript}";
        ExecStart = lib.concatStringsSep " " [
          "${cfg.package}/bin/nix-ci"
          "server"
          "--database-url ${lib.escapeShellArg cfg.databaseUrl}"
          "--listen ${lib.escapeShellArg cfg.bind}"
          "--lock-key ${toString cfg.lockKey}"
        ];
        Restart = "on-failure";
        RestartSec = 5;
        # Give the coordinator a bounded window to finish its graceful
        # shutdown (drain in-flight requests, flush writebacks, close
        # the pool) before systemd SIGKILLs it.
        TimeoutStopSec = cfg.gracefulShutdownSecs + 5;

        # DynamicUser allocates an ephemeral UID per service invocation.
        # It implies many of the hardening flags below for free, but we
        # set them explicitly for clarity.
        DynamicUser = true;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        RestrictNamespaces = true;
        LockPersonality = true;
        RestrictRealtime = true;
        NoNewPrivileges = true;
      };
    };
  };
}
