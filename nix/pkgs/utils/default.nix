{ git, lib, stdenv, clang, openapi-generator, pkgs, which, sources }:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  channel = import ../../lib/rust.nix { inherit sources; };

  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix: lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;
  src = whitelistSource ../../../. [
    ".git"
    "Cargo.lock"
    "Cargo.toml"
    "common"
    "composer"
    "control-plane"
    "deployer"
    "kubectl-plugin"
    "openapi"
    "rpc"
    "tests"
    "scripts"
  ];

  naersk = pkgs.callPackage sources.naersk {
    rustc = channel.static.stable;
    cargo = channel.static.stable;
  };

  components = { release ? false }: {
    kubectl-plugin = naersk.buildPackage {
      inherit release src;
      preBuild = ''
        # don't run during the dependency build phase
        if [ ! -f build.rs ]; then
          patchShebangs ./scripts/generate-openapi-bindings.sh
          ./scripts/generate-openapi-bindings.sh
        fi
        sed -i '/ctrlp-tests.*=/d' ./kubectl-plugin/Cargo.toml
      '';
      cargoBuildOptions = attrs: attrs ++ [ "-p" "kubectl-plugin" ];
      CARGO_BUILD_TARGET = "x86_64-unknown-linux-musl";
      CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";
      nativeBuildInputs = [ clang openapi-generator which git ];
      doCheck = false;
      usePureFromTOML = true;
    };
  };
in
{
  inherit version;

  release = components { release = true; };
  debug = components { release = false; };
}
