{ git, lib, stdenv, openapi-generator, pkgs, which, sources, llvmPackages, protobuf }:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  channel = import ../../lib/rust.nix { inherit sources; };
  project-builder =
    pkgs.callPackage ../control-plane/cargo-project.nix { inherit version; };
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix: lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;
  src = whitelistSource ../../../. project-builder.src_list;

  LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
  PROTOC = "${protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${protobuf}/include";

  naersk_package = channel: pkgs.callPackage sources.naersk {
    rustc = channel.stable;
    cargo = channel.stable;
  };
  naersk = naersk_package channel.static;
  naersk_cross = naersk_package channel.windows_cross;
  arch = with lib; head (splitString "-" stdenv.hostPlatform.system);

  components = { release ? false }: {
    # TODO: currently broken, at least 1 package fails to cross compile to windows (brotli2)
    windows-gnu = {
      kubectl-plugin = naersk_cross.buildPackage {
        inherit release src version;
        name = "kubectl-plugin";

        preBuild = ''
          # don't run during the dependency build phase
          if [ ! -f build.rs ]; then
            patchShebangs ./scripts/rust/generate-openapi-bindings.sh
            ./scripts/rust/generate-openapi-bindings.sh --skip-git-diff
          fi
          sed -i '/deployer-cluster.*=/d' ./control-plane/plugin/Cargo.toml
          export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_RUSTFLAGS="-C link-args=''$(echo $NIX_LDFLAGS | tr ' ' '\n' | grep -- '^-L' | tr '\n' ' ')"
          export NIX_LDFLAGS=
          export NIX_LDFLAGS_FOR_BUILD=
          export OPENSSL_STATIC=1
          export OPENSSL_DIR=${pkgs.pkgsCross.mingwW64.openssl.dev};
        '';
        cargoBuildOptions = attrs: attrs ++ [ "-p" "kubectl-plugin" "--no-default-features" "--features" "tls" ];
        buildInputs = with pkgs.pkgsCross.mingwW64.windows; [ mingw_w64_pthreads pthreads ];
        nativeBuildInputs = [ pkgs.pkgsCross.mingwW64.clangStdenv.cc openapi-generator which git pkgs.pkgsCross.mingwW64.openssl.dev ];
        doCheck = false;
        usePureFromTOML = true;

        PROTOC = "${protobuf}/bin/protoc";
        CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";
        CARGO_BUILD_TARGET = "x86_64-pc-windows-gnu";
        CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = with pkgs.pkgsCross.mingwW64.clangStdenv;
          "${cc}/bin/${cc.targetPrefix}cc";
      };
      recurseForDerivations = true;
    };
    linux-musl = {
      kubectl-plugin = naersk.buildPackage {
        inherit release src version;
        name = "kubectl-plugin";

        preBuild = ''
          # don't run during the dependency build phase
          if [ ! -f build.rs ]; then
            patchShebangs ./scripts/rust/generate-openapi-bindings.sh
            ./scripts/rust/generate-openapi-bindings.sh --skip-git-diff
          fi
          sed -i '/deployer-cluster.*=/d' ./control-plane/plugin/Cargo.toml
          export OPENSSL_STATIC=1
        '';
        inherit LIBCLANG_PATH PROTOC PROTOC_INCLUDE;
        cargoBuildOptions = attrs: attrs ++ [ "-p" "kubectl-plugin" ];
        nativeBuildInputs = with pkgs; [ pkgconfig clang openapi-generator which git pkgsStatic.openssl.dev ];
        doCheck = false;
        usePureFromTOML = true;

        CARGO_BUILD_TARGET = "${arch}-unknown-linux-musl";
        CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";
      };
      recurseForDerivations = true;
    };
    # Can only be built on apple-darwin
    apple-darwin = {
      kubectl-plugin = naersk.buildPackage {
        inherit release src version;
        name = "kubectl-plugin";

        preBuild = ''
          # don't run during the dependency build phase
          if [ ! -f build.rs ]; then
            patchShebangs ./scripts/rust/generate-openapi-bindings.sh
            ./scripts/rust/generate-openapi-bindings.sh --skip-git-diff
          fi
          sed -i '/deployer-cluster.*=/d' ./control-plane/plugin/Cargo.toml
        '';
        inherit LIBCLANG_PATH PROTOC PROTOC_INCLUDE;
        cargoBuildOptions = attrs: attrs ++ [ "-p" "kubectl-plugin" ];
        nativeBuildInputs = with pkgs; [ clang openapi-generator which git openssl.dev ];
        doCheck = false;
        usePureFromTOML = true;

        CARGO_BUILD_TARGET = "x86_64-apple-darwin";
      };
      recurseForDerivations = true;
    };
  };
in
{
  inherit version;

  release = components { release = true; };
  debug = components { release = false; };
}
