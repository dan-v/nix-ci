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

    pushCache = lib.mkOption {
      type = lib.types.nullOr (lib.types.submodule {
        options = {
          name = lib.mkOption {
            type = lib.types.str;
            example = "my-org-overlay";
            description = ''
              Cache name (as the Cachix server knows it). For
              app.cachix.org this is the short name (`my-org`); for
              self-hosted instances it's whatever the server's
              configuration assigns.
            '';
          };
          host = lib.mkOption {
            type = lib.types.nullOr lib.types.str;
            default = null;
            example = "https://cachix.internal.example.com";
            description = ''
              Base URL of the Cachix instance. Leave null to target
              app.cachix.org (the default). Set this to your internal
              cachix-self-hosted URL to push to a private deployment.
              Translated into `cachix --host <url>` at hook time.
            '';
          };
          authTokenFile = lib.mkOption {
            type = lib.types.path;
            example = lib.literalExpression "/run/keys/cachix-token";
            description = ''
              Path to a file containing the Cachix auth token. The file
              must be readable by the nix-daemon user (not the worker
              user). Contents are read per hook invocation so rotating
              the file takes effect without restarting the daemon.
            '';
          };
          extraArgs = lib.mkOption {
            type = lib.types.listOf lib.types.str;
            default = [];
            example = [ "--compression-method" "zstd" ];
            description = ''
              Additional flags passed to `cachix push` verbatim.
              Escape hatch for options this module doesn't surface
              directly (compression, signing key paths, etc.).
            '';
          };
        };
      });
      default = null;
      description = ''
        If set, every successful build on this host pushes its output
        paths to the given Cachix cache via a nix daemon
        post-build-hook. This is the mechanism by which other workers
        and CCI jobs substitute from the cache rather than rebuilding
        drvs this worker already finished. Without it, the global
        dedup value prop collapses to "one builder coordinates but
        everyone still builds everything."

        The post-build-hook runs under the nix daemon (not the worker
        process), which is why the auth token file must be daemon-
        readable.

        Works with both app.cachix.org (leave `host = null`) and
        self-hosted Cachix deployments (set `host` to your instance's
        base URL). For stores that aren't Cachix-protocol-compatible —
        Attic, harmonia, bespoke scripts — set `pushCache = null` and
        configure `nix.settings.post-build-hook` directly.
      '';
    };
  };

  config = lib.mkIf cfg.enable (let
    # When pushCache is set, we install a post-build-hook that pushes
    # OUT_PATHS to the configured Cachix cache. The hook runs under
    # the nix daemon so it has access to the built paths; the auth
    # token is read from disk at hook-invocation time (nix-daemon
    # re-execs the hook per build).
    pushHook = lib.optionalAttrs (cfg.pushCache != null) {
      nix.settings.post-build-hook = pkgs.writeShellScript "nix-ci-post-build-push" ''
        set -euo pipefail
        if [ -z "''${OUT_PATHS:-}" ]; then exit 0; fi
        token=$(cat ${lib.escapeShellArg cfg.pushCache.authTokenFile})
        exec ${pkgs.cachix}/bin/cachix \
          ${lib.optionalString (cfg.pushCache.host != null) "--host ${lib.escapeShellArg cfg.pushCache.host}"} \
          --auth-token "$token" \
          push ${lib.escapeShellArg cfg.pushCache.name} \
          ${lib.escapeShellArgs cfg.pushCache.extraArgs} \
          $OUT_PATHS
      '';
    };
  in lib.mkMerge [ pushHook {
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
        # Load bearer tokens via systemd credential store so they never
        # appear in `/proc/*/environ`. The wrapper script resolves the
        # token from the credential file at exec time.
        LoadCredential = lib.optional (cfg.authBearer != null)
          "auth_bearer:${cfg.authBearer}";
        ExecStart = pkgs.writeShellScript "nix-ci-worker-start" (''
          set -euo pipefail
        '' + (if cfg.authBearer != null then ''
          export NIX_CI_AUTH_BEARER="$(cat "''${CREDENTIALS_DIRECTORY}/auth_bearer")"
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
        ProtectKernelTunables = true;
        ProtectKernelLogs = true;
        ProtectClock = true;
        ProtectHostname = true;
        ProtectControlGroups = true;
        NoNewPrivileges = true;
        LockPersonality = true;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        # Network surface: the worker talks HTTP/HTTPS to the
        # coordinator and to the nix daemon over a unix socket. Allow
        # only those families; deny AF_NETLINK / AF_PACKET / AF_BLUETOOTH
        # etc. Netlink in particular is the common escape route for a
        # compromised build helper.
        RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
        # NOT RestrictNamespaces — nix sandbox uses user + mount
        # namespaces; restricting them breaks builds. Same for
        # ProtectKernelModules (nix sandbox mounts overlayfs) and
        # MemoryDenyWriteExecute (some fixed-output derivations JIT).
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
  } ]);
}
