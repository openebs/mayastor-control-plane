{ norust ? false, devrustup ? true, rust-profile ? "nightly", io-engine ? "" }:
let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [ (_: _: { inherit sources; }) (import ./nix/overlay.nix { }) ];
  };
in
with pkgs;
let
  norust_moth =
    "You have requested an environment without rust, you should provide it! Hint: use rustup tool.";
  devrustup_moth =
    "You have requested an environment for rustup, you should provide it!";
  io-engine-moth = "Using the following io-engine binary: ${io-engine}";
  channel = import ./nix/lib/rust.nix { inherit sources; };
  # python environment for tests/bdd
  pytest_inputs = python3.withPackages
    (ps: with ps; [ virtualenv grpcio grpcio-tools black ]);
  rust_chan = channel.default_src;
  rust = rust_chan.${rust-profile};
in
mkShell {
  name = "control-plane-shell";
  buildInputs = [
    cargo-expand
    cargo-udeps
    clang
    commitlint
    cowsay
    docker
    e2fsprogs
    etcd
    fio
    git
    jq
    libxfs
    llvmPackages.libclang
    nvme-cli
    openapi-generator
    openssl
    pkg-config
    pre-commit
    pytest_inputs
    python3
    tini
    udev
    utillinux
    which
  ] ++ pkgs.lib.optional (!norust) rust;

  LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
  PROTOC = "${protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${protobuf}/include";

  # variables used to easily create containers with docker files
  ETCD_BIN = "${pkgs.etcd}/bin/etcd";

  # using the nix rust toolchain
  USE_NIX_RUST = "${toString (!norust)}";
  # copy the rust toolchain to a writable directory, see: https://github.com/rust-lang/cargo/issues/10096
  # the whole toolchain is copied to allow the src to be retrievable through "rustc --print sysroot"
  RUST_TOOLCHAIN = "/tmp/rust-toolchain/${rust.version}";
  RUST_TOOLCHAIN_NIX = "${rust}";

  NODE_PATH = "${nodePackages."@commitlint/config-conventional"}/lib/node_modules";

  shellHook = ''
    ./scripts/nix/git-submodule-init.sh
    pre-commit install
    pre-commit install --hook commit-msg

    ${pkgs.lib.optionalString (norust) "cowsay ${norust_moth}"}
    ${pkgs.lib.optionalString (norust) "echo"}

    rustup_channel="${lib.strings.concatMapStringsSep "-" (x: x) (lib.lists.drop 1 (lib.strings.splitString "-" rust.version))}" \
    dev_rustup="${toString (devrustup)}" devrustup_moth="${devrustup_moth}" . ./scripts/rust/env-setup.sh

    export WORKSPACE_ROOT=`pwd`
    [ ! -z "${io-engine}" ] && cowsay "${io-engine-moth}"
    [ ! -z "${io-engine}" ] && export IO_ENGINE_BIN="${io-engine-moth}"
    export PATH="$PATH:$(pwd)/target/debug"

    DOCKER_CONFIG=~/.docker/config.json
    if [ -f "$DOCKER_CONFIG" ]; then
      DOCKER_TOKEN=$(cat ~/.docker/config.json | jq '.auths."https://index.docker.io/v1/".auth // empty' | tr -d '"' | base64 -d)
      export DOCKER_USER=$(echo $DOCKER_TOKEN | cut -d':' -f1)
      export DOCKER_PASS=$(echo $DOCKER_TOKEN | cut -d':' -f2)
    fi
  '';
}
