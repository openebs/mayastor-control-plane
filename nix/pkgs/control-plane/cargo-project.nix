{ stdenv
, clang
, git
, lib
, llvmPackages
, makeRustPlatform
, openssl
, pkg-config
, protobuf
, sources
, pkgs
, gitVersions
, openapi-generator
, which
, systemdMinimal
, utillinux
, sourcer
  # with allInOne set to true all components are built as part of the same "cargo build" derivation
  # this allows for a quicker build of all components but slower single components
  # with allInOne set to false each component gets its own "cargo build" derivation allowing for faster
  # individual builds but making the build of all components at once slower
, allInOne ? true
  # EXPERIMENTAL incremental allows for faster incremental builds as the build dependencies are cached
  # it might make the initial build slightly slower as it's done in two steps
  # for this we use naersk which is not as fully fledged as the builtin rustPlatform so it should only be used
  # for development and not for CI
, incremental ? false
}:
let
  channel = import ../../lib/rust.nix { inherit sources; };
  stable_channel = {
    rustc = channel.default.stable;
    cargo = channel.default.stable;
  };
  rustPlatform = makeRustPlatform {
    rustc = stable_channel.rustc;
    cargo = stable_channel.cargo;
  };
  naersk = pkgs.callPackage sources.naersk {
    rustc = stable_channel.rustc;
    cargo = stable_channel.cargo;
  };
  LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
  PROTOC = "${protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${protobuf}/include";
  version = gitVersions.version;
  src_list = [
    "Cargo.lock"
    "Cargo.toml"
    ".cargo"
    "common"
    "control-plane"
    "deployer"
    "k8s"
    "openapi/Cargo.toml"
    "openapi/build.rs"
    "rpc"
    "scripts/rust/generate-openapi-bindings.sh"
    "scripts/rust/branch_ancestor.sh"
    "tests/io-engine"
    "utils"
  ];
  src = sourcer.whitelistSource ../../../. src_list;
  buildProps = rec {
    name = "control-plane-${version}";
    inherit version src;

    GIT_VERSION_LONG = "${gitVersions.long}";
    GIT_VERSION = "${gitVersions.tag_or_long}";

    inherit LIBCLANG_PATH PROTOC PROTOC_INCLUDE;
    nativeBuildInputs = [ clang pkg-config openapi-generator which git llvmPackages.bintools ];
    buildInputs = [ llvmPackages.libclang protobuf openssl.dev systemdMinimal.dev utillinux.dev ];
    doCheck = false;
  };
  release_build = { "release" = true; "debug" = false; };
in
let
  build_with_naersk = { buildType, cargoBuildFlags }:
    naersk.buildPackage (buildProps // {
      release = release_build.${buildType};
      cargoBuildOptions = attrs: attrs ++ cargoBuildFlags;
      preBuild = ''
        # don't run during the dependency build phase
        if [ ! -f build.rs ]; then
          patchShebangs ./scripts/rust
        fi
        # remove the tests lib dependency since we don't run tests during this build
        find . -name \*.toml | xargs -I% sed -i '/^io-engine-tests.*=/d' %
      '';
      doCheck = false;
      usePureFromTOML = true;
    });
  build_with_default = { buildType, cargoBuildFlags }:
    rustPlatform.buildRustPackage (buildProps // {
      inherit buildType cargoBuildFlags;

      preBuild = "patchShebangs ./scripts/rust";

      cargoLock = {
        lockFile = ../../../Cargo.lock;
      };
    });
  builder = if incremental then build_with_naersk else build_with_default;
in
{
  inherit LIBCLANG_PATH PROTOC PROTOC_INCLUDE version src;

  build = { buildType, cargoBuildFlags ? [ ] }:
    if allInOne then
      builder { inherit buildType; cargoBuildFlags = [ "-p rpc" "-p agents" "-p rest" "-p k8s-operators" "-p csi-driver" ]; }
    else
      builder { inherit buildType cargoBuildFlags; };
}
