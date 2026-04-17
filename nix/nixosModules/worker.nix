{ config, lib, pkgs, ... }:
let
  cfg = config.services.nix-ci-worker;
in
{
  options.services.nix-ci-worker = {
    enable = lib.mkEnableOption "nix-ci fleet worker (long-polls coordinator for claims)";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.nix-ci;
      defaultText = lib.literalExpression "pkgs.nix-ci";
      description = "The nix-ci package to use.";
    };

    coordinatorUrl = lib.mkOption {
      type = lib.types.str;
      example = "http://coordinator.internal:8080";
      description = ''
        Base URL of the coordinator this worker polls. No trailing
        slash. The worker long-polls `GET /claim` against this
        endpoint.
      '';
    };

    authBearer = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      example = lib.literalExpression "/run/keys/nix-ci-token";
      description = ''
        Path to a file containing the bearer token for authenticating
        to the coordinator. Contents are read into the environment
        variable NIX_CI_AUTH_BEARER at service start. Use null when
        the coordinator has no auth configured.

        Keep the file root-readable only; systemd LoadCredential or a
        secrets agent is the expected provisioning path.
      '';
    };

    system = lib.mkOption {
      type = lib.types.str;
      default = pkgs.stdenv.hostPlatform.system;
      defaultText = lib.literalExpression "pkgs.stdenv.hostPlatform.system";
      description = "System triple this worker advertises (x86_64-linux, aarch64-linux, …).";
    };

    supportedFeatures = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [];
      example = [ "kvm" "big-parallel" ];
      description = ''
        Nix system features this worker can satisfy. Builds whose drv
        requires one of these are eligible to be claimed here; others
        fall through to another worker.
      '';
    };

    maxParallel = lib.mkOption {
      type = lib.types.int;
      default = 4;
      description = "Max concurrent builds this worker runs at a time.";
    };

    workerId = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "builder-01";
      description = ''
        Stable worker identifier surfaced on /claims for operator
        attribution. Defaults to hostname-pid-rand8.
      '';
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
      description = "RUST_LOG level for the worker process.";
    };

    logJson = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Emit JSON-formatted logs (for log aggregation).";
    };

    extraPackages = lib.mkOption {
      type = lib.types.listOf lib.types.package;
      default = [];
      description = ''
        Extra packages appended to the worker's PATH. nix + nix-eval-
        jobs are added automatically; use this for e.g. cachix,
        post-build-hook helpers, or custom wrappers.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.services.nix-ci-worker = {
      description = "nix-ci fleet worker";
      after = [ "network-online.target" "nix-daemon.service" ];
      wants = [ "network-online.target" "nix-daemon.service" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        RUST_LOG = cfg.logLevel;
        NIX_CI_LOG_JSON = if cfg.logJson then "1" else "0";
      };

      path = [ cfg.package pkgs.nix pkgs.nix-eval-jobs ] ++ cfg.extraPackages;

      serviceConfig = {
        Type = "simple";
        # Bearer token (if any) comes in via systemd credential. The
        # EnvironmentFile syntax below expects a `NIX_CI_AUTH_BEARER=...`
        # style file; the wrapper script reads the raw-token file and
        # exports the env var before execing nix-ci.
        ExecStart = pkgs.writeShellScript "nix-ci-worker-start" (''
          set -euo pipefail
        '' + (if cfg.authBearer != null then ''
          export NIX_CI_AUTH_BEARER="$(cat ${lib.escapeShellArg cfg.authBearer})"
        '' else "") + ''
          exec ${cfg.package}/bin/nix-ci worker \
            --coordinator ${lib.escapeShellArg cfg.coordinatorUrl} \
            --system ${lib.escapeShellArg cfg.system} \
            --max-parallel ${toString cfg.maxParallel} \
            ${lib.optionalString (cfg.supportedFeatures != []) "--features ${lib.escapeShellArg (lib.concatStringsSep "," cfg.supportedFeatures)}"} \
            ${lib.optionalString (cfg.workerId != null) "--worker-id ${lib.escapeShellArg cfg.workerId}"}
        '');
        Restart = "on-failure";
        RestartSec = 5;
        # Drain on SIGTERM: worker.rs uses a watch<bool> shutdown signal
        # with a 5s drain timeout; add a safety margin before systemd
        # SIGKILLs a slow nix build.
        TimeoutStopSec = 30;
        KillSignal = "SIGTERM";

        # The worker shells out to `nix build`, which needs access to
        # the nix daemon socket and the build sandbox. DynamicUser
        # would sandbox us away from both. Run as a dedicated static
        # user + group; `nix` handles its own hardening inside the
        # daemon.
        User = "nix-ci-worker";
        Group = "nix-ci-worker";
        # Per-invocation cache + log state. `StateDirectory` is picked
        # up by systemd's `STATE_DIRECTORY` env var which nix honors
        # for local derivations; worker itself keeps no durable state.
        StateDirectory = "nix-ci-worker";
        StateDirectoryMode = "0700";

        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        NoNewPrivileges = true;
        LockPersonality = true;
        RestrictRealtime = true;
        # NOT RestrictNamespaces — nix sandbox uses user + mount
        # namespaces; restricting them breaks builds.
      };
    };

    users.users.nix-ci-worker = {
      isSystemUser = true;
      group = "nix-ci-worker";
      description = "nix-ci worker service";
      # The nix daemon handles sandbox UID mapping internally; the
      # worker itself only needs to talk to the daemon socket.
      extraGroups = [ "nix-users" ];
    };
    users.groups.nix-ci-worker = {};

    # Nix daemon must be running — the worker has no fallback single-
    # user-mode path.
    nix.settings.trusted-users = [ "nix-ci-worker" ];
  };
}
