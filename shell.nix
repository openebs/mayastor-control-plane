{ norust ? false, io-engine ? "" }:
let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [ (_: _: { inherit sources; }) (import ./nix/overlay.nix { }) ];
  };
in
with pkgs;
let
  norust_moth =
    "You have requested an environment without rust, you should provide it!";
  io-engine-moth = "Using the following io-engine binary: ${io-engine}";
  channel = import ./nix/lib/rust.nix { inherit sources; };
  # python environment for tests/bdd
  pytest_inputs = python3.withPackages
    (ps: with ps; [ virtualenv grpcio grpcio-tools black ]);
  rust_chan = channel.default_src;
in
mkShell {
  name = "control-plane-shell";
  buildInputs = [
    cargo-expand
    cargo-udeps
    clang
    cowsay
    docker
    etcd
    fio
    git
    jq
    libudev
    libxfs
    llvmPackages.libclang
    nats-server
    nvme-cli
    openapi-generator
    openssl
    pkg-config
    pre-commit
    pytest_inputs
    python3
    tini
    utillinux
    which
  ] ++ pkgs.lib.optional (!norust) rust_chan.nightly;

  LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
  PROTOC = "${protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${protobuf}/include";

  # variables used to easily create containers with docker files
  ETCD_BIN = "${pkgs.etcd}/bin/etcd";
  NATS_BIN = "${pkgs.nats-server}/bin/nats-server";

  # using the nix rust toolchain
  USE_NIX_RUST = "${toString (!norust)}";
  # copy the rust-src to a writable directory, see: https://github.com/rust-lang/cargo/issues/10096
  RUST_SRC_CLONE = "/tmp/rust-src";

  shellHook = ''
    ./scripts/nix/git-submodule-init.sh
    ${pkgs.lib.optionalString (norust) "cowsay ${norust_moth}"}
    ${pkgs.lib.optionalString (norust) "echo 'Hint: use rustup tool.'"}
    ${pkgs.lib.optionalString (norust) "echo"}
    if [ -n "$USE_NIX_RUST" ]; then
      RUST_SRC_PATH="${rust_chan.nightly_src}/lib/rustlib/src/rust/library/"
      if ! diff "$RUST_SRC_PATH" "$RUST_SRC_CLONE" &>/dev/null; then
        rm -rf "$RUST_SRC_CLONE"
        mkdir -p "$RUST_SRC_CLONE" 2>/dev/null
        cp -r "$RUST_SRC_PATH"/* "$RUST_SRC_CLONE"
        chmod -R 775 "$RUST_SRC_CLONE"
      fi
    fi
    pre-commit install
    pre-commit install --hook commit-msg
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
