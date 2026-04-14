{
  lib,
  rustPlatform,
  pkg-config,
  openssl,
  makeWrapper,
  nix,
  nix-eval-jobs,
  gitRevision ? "unknown",
}:

rustPlatform.buildRustPackage {
  pname = "nix-ci";
  version = "0.2.0";

  # Trim the Nix build sandbox to just the rust workspace. README/DESIGN
  # changes won't bust the cache.
  src = lib.fileset.toSource {
    root = ../../..;
    fileset = lib.fileset.unions [
      ../../../rust
    ];
  };
  sourceRoot = "source/rust";

  cargoLock = {
    lockFile = ../../../rust/Cargo.lock;
  };

  nativeBuildInputs = [
    pkg-config
    makeWrapper
  ];
  buildInputs = [ openssl ];

  # Integration tests require Postgres — skip in the Nix build.
  # They run in CI via GitHub Actions with a Postgres service.
  doCheck = false;

  postInstall = ''
    wrapProgram $out/bin/nix-ci \
      --prefix PATH : ${lib.makeBinPath [ nix nix-eval-jobs ]}
  '';

  NIX_CI_GIT_REVISION = gitRevision;
  OPENSSL_NO_VENDOR = 1;

  meta = with lib; {
    description = "Distributed Nix build coordinator with global build deduplication";
    license = with licenses; [ mit asl20 ];
    mainProgram = "nix-ci";
    platforms = platforms.linux;
  };
}
