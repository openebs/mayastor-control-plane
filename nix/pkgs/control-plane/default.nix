{ stdenv
, git
, lib
, pkgs
}:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  project-builder = pkgs.callPackage ../control-plane/cargo-project.nix { inherit version; };
  agent = { name, src, suffix ? "agent" }: stdenv.mkDerivation {
    inherit src;
    name = "${name}-${version}";
    binary = "${name}-${suffix}";
    installPhase = ''
      mkdir -p $out/bin
      cp $src/bin/${name} $out/bin/${name}-${suffix}
    '';
  };
  components = { src }: {
    jsongrpc = agent { inherit src; name = "jsongrpc"; };
    core = agent { inherit src; name = "core"; };
    rest = agent { inherit src; name = "rest"; suffix = "api"; };
    msp-operator = agent { inherit src; name = "msp-operator"; };
    csi-controller = agent { inherit src; name = "csi-controller"; };
  };
in
{
  LIBCLANG_PATH = project-builder.LIBCLANG_PATH;
  PROTOC = project-builder.PROTOC;
  PROTOC_INCLUDE = project-builder.PROTOC_INCLUDE;
  inherit version;

  release = components { src = project-builder.release; };
  debug = components { src = project-builder.debug; };
  coverage = components { src = project-builder.coverage; };
}
