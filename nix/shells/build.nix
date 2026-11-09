# Native dependencies required to build the rust code
{ pkgs ? import ../pkgs.nix }:
with pkgs;
mkShellNoCC {
  name = "build-shell";
  buildInputs = [
    llvmPackages.bintools
    clang
    # aws-lc-rs fips
    cmake
    go
    perl
    llvmPackages.libclang
    pkg-config
    # openapi/build.rs bindings generation
    paperclip
    which
    git
  ] ++ lib.optional (system == "aarch64-darwin") darwin.apple_sdk.frameworks.Security;

  nativeBuildInputs = [ rustPlatform.bindgenHook ];

  shellHook = ''
    export PROTOC="${protobuf}/bin/protoc"
    export PROTOC_INCLUDE="${protobuf}/include"
  '';
}
