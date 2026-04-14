{ final, prev, gitRevision }:
{
  nix-ci = final.callPackage ./nix-ci { inherit gitRevision; };

  nix-ci-image = final.dockerTools.buildLayeredImage {
    name = "nix-ci";
    tag = gitRevision;
    contents = [
      final.nix-ci
      final.cacert
    ];
    config = {
      Entrypoint = [ "${final.nix-ci}/bin/nix-ci" ];
      Env = [
        "SSL_CERT_FILE=${final.cacert}/etc/ssl/certs/ca-bundle.crt"
        "NIX_SSL_CERT_FILE=${final.cacert}/etc/ssl/certs/ca-bundle.crt"
      ];
    };
  };
}
