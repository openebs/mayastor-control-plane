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
, version
, openapi-generator
, which
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
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix: lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;
  LIBCLANG_PATH = "${llvmPackages.libclang}/lib";
  PROTOC = "${protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${protobuf}/include";

  buildProps = rec {
    name = "control-plane-${version}";
    inherit version;

    src = whitelistSource ../../../. [
      ".git"
      "Cargo.lock"
      "Cargo.toml"
      "common"
      "control-plane"
      "deployer"
      "kubectl-plugin"
      "openapi"
      "rpc"
      "tests"
      "scripts"
    ];

    inherit LIBCLANG_PATH PROTOC PROTOC_INCLUDE;
    nativeBuildInputs = [ clang pkg-config openapi-generator which git pkgs.breakpointHook ];
    buildInputs = [ llvmPackages.libclang protobuf openssl ];
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
          patchShebangs ./scripts/rust/generate-openapi-bindings.sh
          ./scripts/rust/generate-openapi-bindings.sh
        fi
        # remove the tests lib dependency since we don't run tests during this build
        find . -name \*.toml | xargs -I% sed -i '/^ctrlp-tests.*=/d' %
      '';
      doCheck = false;
      usePureFromTOML = true;
    });
  build_with_default = { buildType, cargoBuildFlags }:
    rustPlatform.buildRustPackage (buildProps // {
      inherit buildType cargoBuildFlags;

      preBuild = "patchShebangs ./scripts/rust/generate-openapi-bindings.sh";

      cargoLock = {
        lockFile = ../../../Cargo.lock;
        outputHashes = {
          "nats-0.15.2" =
            "sha256:1whr0v4yv31q5zwxhcqmx4qykgn5cgzvwlaxgq847mymzajpcsln";
          "composer-0.1.0" =
            "sha256:1xrpvf7fgjphhm17x3vs3ysmqs9g8j7w006n9r4ab78prp84h5ha";
        };
      };
    });
  builder = if incremental then build_with_naersk else build_with_default;
in
{
  inherit LIBCLANG_PATH PROTOC PROTOC_INCLUDE version;

  build = { buildType, cargoBuildFlags ? [ ] }:
    if allInOne then
      builder { inherit buildType; cargoBuildFlags = [ "-p rpc" "-p agents" "-p rest" "-p msp-operator" "-p csi-controller" ]; }
    else
      builder { inherit buildType cargoBuildFlags; };
}
