{ stdenv, git, lib, pkgs, allInOne, incremental }:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  gitVersions = {
    "version" = version;
    "long" = builtins.readFile "${versionDrv.long}";
    "tag_or_long" = builtins.readFile "${versionDrv.tag_or_long}";
  };

  project-builder =
    pkgs.callPackage ../control-plane/cargo-project.nix { inherit gitVersions allInOne incremental; };
  installer = { name, src }:
    stdenv.mkDerivation {
      inherit src;
      name = "${name}-${version}";
      binary = "${name}";
      installPhase = ''
        mkdir -p $out/bin
        cp $src/bin/${name} $out/bin/${name}
      '';
    };

  components = { buildType, builder }: rec {
    agents = rec {
      recurseForDerivations = true;
      agent_installer = { name }: installer {
        inherit name;
        src =
          if allInOne then
            builder.build { inherit buildType; cargoBuildFlags = [ "-p agents" ]; }
          else
            builder.build { inherit buildType; cargoBuildFlags = [ "--bin ${name}" ]; };
      };
      jsongrpc = agent_installer {
        name = "jsongrpc";
      };
      core = agent_installer {
        name = "core";
      };
      ha = {
        node = agent_installer {
          name = "agent-ha-node";
        };
        cluster = agent_installer {
          name = "agent-ha-cluster";
        };
      };
    };

    api-rest = installer {
      src = builder.build { inherit buildType; cargoBuildFlags = [ "-p rest" ]; };
      name = "rest";
    };

    operators = rec {
      operator_installer = { name, src }: installer { inherit name src; };
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
  inherit version gitVersions project-builder;

  release = components { builder = project-builder; buildType = "release"; };
  debug = components { builder = project-builder; buildType = "debug"; };
}
