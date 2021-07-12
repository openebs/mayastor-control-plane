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
}:
let
  channel = import ../../lib/rust.nix { inherit sources; };
  rustPlatform = makeRustPlatform {
    rustc = channel.stable.rust;
    cargo = channel.stable.cargo;
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
      "Cargo.lock"
      "Cargo.toml"
      "common"
      "composer"
      "control-plane"
      "openapi"
      "tests-mayastor"
      "deployer"
    ];
    cargoBuildFlags = [ "-p agents" "-p rest" ];

    cargoLock = {
      lockFile = ../../../Cargo.lock;
      outputHashes = {
        "rpc-0.1.0" = "0ghiqlc4qz63znn13iibb90k77j9jm03vcgjqgq17jsw4dhswsvb";
      };
    };

    inherit LIBCLANG_PATH PROTOC PROTOC_INCLUDE;
    nativeBuildInputs = [
      clang
      pkg-config
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
}
