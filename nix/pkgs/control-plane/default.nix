{ stdenv
, git
, lib
, pkgs
}:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  project-builder = pkgs.callPackage ../control-plane/cargo-project.nix { inherit version; };
  agent = { name, src }: stdenv.mkDerivation {
    inherit src;
    name = "${name}-${version}";
    installPhase = ''
      mkdir -p $out/bin
      cp $src/bin/${name} $out/bin/${name}-agent
    '';
  };
  components = { src }: {
    jsongrpc = agent { inherit src; name = "jsongrpc"; };
    core = agent { inherit src; name = "core"; };
    rest = agent { inherit src; name = "rest"; };
  };
in
{
  LIBCLANG_PATH = project-builder.LIBCLANG_PATH;
  PROTOC = project-builder.PROTOC;
  PROTOC_INCLUDE = project-builder.PROTOC_INCLUDE;
  inherit version;

  release = components { src = project-builder.release; };
  debug = components { src = project-builder.debug; };
}
