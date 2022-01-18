{ norust ? false
, mayastor ? ""
}:
let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/overlay.nix)
    ];
  };
in
with pkgs;
let
  norust_moth = "You have requested an environment without rust, you should provide it!";
  mayastor_moth = "Using the following mayastor binary: ${mayastor}";
  channel = import ./nix/lib/rust.nix { inherit sources; };
  # python environment for tests/bdd
  pytest_inputs = python3.withPackages
    (ps: with ps; [ virtualenv grpcio grpcio-tools black ]);
in
mkShell {
  name = "mayastor-control-plane-shell";
  buildInputs = [
    clang
    cowsay
    docker
    etcd
    llvmPackages_11.libclang
    nats-server
    openssl
    pkg-config
    pkgs.openapi-generator
    pre-commit
    pytest_inputs
    python3
    utillinux
    which
    tini
    nvme-cli
    fio
  ] ++ pkgs.lib.optional (!norust) channel.default.nightly;

  LIBCLANG_PATH = "${llvmPackages_11.libclang.lib}/lib";
  PROTOC = "${protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${protobuf}/include";

  # variables used to easily create containers with docker files
  ETCD_BIN = "${pkgs.etcd}/bin/etcd";
  NATS_BIN = "${pkgs.nats-server}/bin/nats-server";

  shellHook = ''
    ${pkgs.lib.optionalString (norust) "cowsay ${norust_moth}"}
    ${pkgs.lib.optionalString (norust) "echo 'Hint: use rustup tool.'"}
    ${pkgs.lib.optionalString (norust) "echo"}
    pre-commit install
    pre-commit install --hook commit-msg
    export MCP_SRC=`pwd`
    [ ! -z "${mayastor}" ] && cowsay "${mayastor_moth}"
    [ ! -z "${mayastor}" ] && export MAYASTOR_BIN="${mayastor}"
  '';
}
