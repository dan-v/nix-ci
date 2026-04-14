{ inputs, self, ... }:

{
  imports = [
    inputs.git-hooks.flakeModule
    inputs.treefmt-nix.flakeModule
  ];

  perSystem =
    { config, pkgs, system, ... }:
    {
      treefmt = {
        projectRootFile = "flake.nix";
        programs = {
          nixfmt.enable = true;
          rustfmt = {
            enable = true;
            edition =
              (builtins.fromTOML (builtins.readFile ../../rust/Cargo.toml)).workspace.package.edition;
          };
          yamlfmt.enable = true;
        };
      };

      # Disable the sandboxed pre-commit derivation: clippy needs network +
      # rustc which aren't available in a Nix build sandbox. The treefmt
      # check still runs in flake check.
      pre-commit.check.enable = false;
      pre-commit.settings.hooks = {
        treefmt.enable = true;
        clippy =
          let
            rust = pkgs.rust-bin.stable.latest.default;
          in
          {
            enable = true;
            entry = toString (
              pkgs.writeShellScript "clippy-hook" ''
                cd rust && ${rust}/bin/cargo clippy --all-targets -- -D warnings
              ''
            );
            files = "\\.rs$";
            pass_filenames = false;
          };
      };

      devShells.default = pkgs.mkShell {
        shellHook = config.pre-commit.installationScript;
        buildInputs = with pkgs; [
          (rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" "rust-analyzer" "clippy" ];
          })
          pkg-config
          openssl
          postgresql_16
          sqlx-cli
          cachix
        ];
      };
    };
}
