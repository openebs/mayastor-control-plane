{ stdenv, git, lib, pkgs, allInOne, incremental }:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  project-builder =
    pkgs.callPackage ../control-plane/cargo-project.nix { inherit version allInOne incremental; };
  installer = { name, src, suffix ? "" }:
    stdenv.mkDerivation {
      inherit src;
      name = "${name}-${version}";
      binary = "${name}${suffix}";
      installPhase = ''
        mkdir -p $out/bin
        cp $src/bin/${name} $out/bin/${name}${suffix}
      '';
    };

  components = { buildType, builder }: rec {
    agents = rec {
      recurseForDerivations = true;
      agents_builder = { buildType, builder }: builder.build { inherit buildType; cargoBuildFlags = [ "-p agents" ]; };
      agent_installer = { name, src }: installer { inherit name src; suffix = "agent"; };
      jsongrpc = agent_installer {
        src = agents_builder { inherit buildType builder; };
        name = "jsongrpc";
      };
      core = agent_installer {
        src = agents_builder { inherit buildType builder; };
        name = "core";
      };
      ha = {
        node = agent_installer {
          src = agents_builder { inherit buildType builder; };
          name = "agent-ha-node";
        };
        cluster = agent_installer {
          src = agents_builder { inherit buildType builder; };
          name = "agent-ha-cluster";
        };
      };
    };

    api-rest = installer {
      src = builder.build { inherit buildType; cargoBuildFlags = [ "-p rest" ]; };
      name = "rest";
      suffix = "-api";
    };

    operators = rec {
      operator_installer = { name, src }: installer { inherit name src; suffix = "-operator"; };
      diskpool = operator_installer {
        src = builder.build { inherit buildType; cargoBuildFlags = [ "-p operator-diskpool" ]; };
        name = "operator-diskpool";
      };
      recurseForDerivations = true;
    };

    csi = rec {
      csi_installer = { name }: installer {
        inherit name;
        src =
          if allInOne then
            builder.build { inherit buildType; cargoBuildFlags = [ "-p csi-driver" ]; }
          else
            builder.build { inherit buildType; cargoBuildFlags = [ "--bin ${name}" ]; };
      };

      controller = csi_installer {
        name = "csi-controller";
      };
      node = csi_installer {
        name = "csi-node";
      };
      recurseForDerivations = true;
    };
  };
in
{
  LIBCLANG_PATH = project-builder.LIBCLANG_PATH;
  PROTOC = project-builder.PROTOC;
  PROTOC_INCLUDE = project-builder.PROTOC_INCLUDE;
  inherit version;

  release = components { builder = project-builder; buildType = "release"; };
  debug = components { builder = project-builder; buildType = "debug"; };
}
