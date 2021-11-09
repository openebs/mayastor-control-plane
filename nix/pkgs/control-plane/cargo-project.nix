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
}:
let
  channel = import ../../lib/rust.nix { inherit sources; };
  rustPlatform = makeRustPlatform {
    rustc = channel.default.stable;
    cargo = channel.default.stable;
  };
  rustNightly = makeRustPlatform {
    rustc = channel.default.nightly;
    cargo = channel.default.nightly;
  };
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix:
            lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
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
      "composer"
      "control-plane"
      "deployer"
      "kubectl-plugin"
      "openapi"
      "rpc"
      "tests"
      "scripts"
    ];
    cargoBuildFlags = [ "-p rpc" "-p agents" "-p rest" "-p msp-operator" "-p csi-controller" ];

    cargoLock = {
      lockFile = ../../../Cargo.lock;
      outputHashes = {
        "nats-0.15.2" = "sha256:1whr0v4yv31q5zwxhcqmx4qykgn5cgzvwlaxgq847mymzajpcsln";
      };
    };

    preBuild = "patchShebangs ./scripts/generate-openapi-bindings.sh";

    inherit LIBCLANG_PATH PROTOC PROTOC_INCLUDE;
    nativeBuildInputs = [
      clang
      pkg-config
      openapi-generator
      which
    ];
    buildInputs = [
      llvmPackages.libclang
      protobuf
      openssl
    ];
    doCheck = false;
  };
in
{
  inherit LIBCLANG_PATH PROTOC PROTOC_INCLUDE version;
  release = rustPlatform.buildRustPackage
    (buildProps // {
      buildType = "release";
      buildInputs = buildProps.buildInputs;
    });
  debug = rustPlatform.buildRustPackage
    (buildProps // {
      buildType = "debug";
      buildInputs = buildProps.buildInputs;
    });
  coverage = rustNightly.buildRustPackage
    (buildProps // {
      RUSTFLAGS = "-Z instrument-coverage";
      buildType = "release";
      buildInputs = buildProps.buildInputs;
    });
}
