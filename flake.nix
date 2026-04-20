{
  description = "nix-ci: distributed Nix build coordinator";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };

    detsys-nix-src.url = "github:DeterminateSystems/nix-src/e9b4735be7b90cf49767faf5c36f770ac1bdc586";
    detsys-nix-eval-jobs.url = "github:DeterminateSystems/nix-eval-jobs/39f6f6f631d8c1c4e2b41157e4f9a3ed7a30571f";
  };

  outputs =
    inputs@{ self, flake-parts, ... }:
    let
      gitRevision =
        if self ? shortRev then
          self.shortRev
        else if self ? dirtyShortRev then
          self.dirtyShortRev
        else
          "dirty";
    in
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];

      perSystem =
        { pkgs, system, ... }:
        {
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [
              inputs.rust-overlay.overlays.default
              self.overlays.detsysToolchain
              self.overlays.default
            ];
          };

          legacyPackages = pkgs;
          packages = import ./nix/packages { inherit pkgs; };

          # NixOS VM tests. `nix flake check` runs these, proving the
          # coordinator module + package + systemd wiring hang together
          # end-to-end. Smoke-only: no actual builds, just health +
          # HTTP API surface.
          checks = pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
            coordinator-smoke = import ./nix/checks/coordinator-smoke.nix {
              inherit pkgs system;
              inherit self;
            };
            # Two-node VM test proving the worker NixOS module starts,
            # authenticates, and drives a claim through to a reported
            # completion against a real coordinator.
            worker-smoke = import ./nix/checks/worker-smoke.nix {
              inherit pkgs system;
              inherit self;
            };
            # Three-node VM test (db + two coordinators) proving the
            # advisory-lock primary/standby contract: only one of two
            # identically-configured coordinators serves at a time, and
            # failover on primary stop is automatic within seconds.
            coordinator-ha-failover = import ./nix/checks/coordinator-ha-failover.nix {
              inherit pkgs system;
              inherit self;
            };
          };

          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              (rust-bin.stable.latest.default.override {
                extensions = [
                  "rust-src"
                  "rust-analyzer"
                  "clippy"
                ];
              })
              pkg-config
              openssl
              postgresql_16
              valkey
              cachix
              nix
              nix-eval-jobs
            ];
          };
        };

      flake = {
        overlays.detsysToolchain = final: _prev: {
          nix = inputs.detsys-nix-src.packages.${final.stdenv.hostPlatform.system}.nix;
          nix-eval-jobs =
            inputs.detsys-nix-eval-jobs.packages.${final.stdenv.hostPlatform.system}.nix-eval-jobs;
        };
        overlays.default = import ./nix/overlays/default.nix { inherit gitRevision; };
        nixosModules = import ./nix/nixosModules;
      };
    };
}
