{ config, lib, pkgs, ... }:
let
  cfg = config.services.nix-ci-coordinator;
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
        ExecStart = lib.concatStringsSep " " [
          "${cfg.package}/bin/nix-ci"
          "server"
          "--database-url ${lib.escapeShellArg cfg.databaseUrl}"
          "--listen ${lib.escapeShellArg cfg.bind}"
          "--lock-key ${toString cfg.lockKey}"
        ];
        Restart = "on-failure";
        RestartSec = 5;

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
