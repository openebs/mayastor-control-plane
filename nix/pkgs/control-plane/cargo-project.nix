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
    #cargoSha256 = "0000000000000000000000000000000000000000000000000000";
    cargoSha256 = "1bg09ws384di6kb896ynm4na0n10v3k823j6xqvdxpxz5ybgvhba";
    inherit version;

    src = whitelistSource ../../../. [
      "Cargo.lock"
      "Cargo.toml"
      "control-plane"
      "composer"
      "tests-mayastor"
    ];
    cargoBuildFlags = [ "-p mbus_api" "-p agents" "-p rest" ];

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
    verifyCargoDeps = false;
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
