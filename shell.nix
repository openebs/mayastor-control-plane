{ norust ? false
, nomayastor ? false
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
  nomayastor_moth = "You have requested an environment without mayastor, you should provide it!";
  channel = import ./nix/lib/rust.nix { inherit sources; };
  mayastor = import pkgs.mayastor-src { };
in
mkShell {
  name = "mayastor-control-plane-shell";
  buildInputs = [
    clang
    cowsay
    fio
    git
    llvmPackages.libclang
    nats-server
    nvme-cli
    openssl
    pkg-config
    pre-commit
    python3
    utillinux
    which
    docker
    etcd
    pkgs.openapi-generator
  ]
  ++ pkgs.lib.optional (!norust) channel.nightly
  ++ pkgs.lib.optional (!nomayastor) mayastor.units.debug.mayastor;

  LIBCLANG_PATH = control-plane.LIBCLANG_PATH;
  PROTOC = control-plane.PROTOC;
  PROTOC_INCLUDE = control-plane.PROTOC_INCLUDE;

  # variables used to easily create containers with docker files
  ETCD_BIN = "${pkgs.etcd}/bin/etcd";
  NATS_BIN = "${pkgs.nats-server}/bin/nats-server";

  shellHook = ''
    ${pkgs.lib.optionalString (norust) "cowsay ${norust_moth}"}
    ${pkgs.lib.optionalString (norust) "echo 'Hint: use rustup tool.'"}
    ${pkgs.lib.optionalString (norust) "echo"}
    ${pkgs.lib.optionalString (nomayastor) "cowsay ${nomayastor_moth}"}
    ${pkgs.lib.optionalString (nomayastor) "echo 'Hint: build mayastor from https://github.com/openebs/mayastor.'"}
    ${pkgs.lib.optionalString (nomayastor) "echo"}
    pre-commit install
    pre-commit install --hook commit-msg
    export MCP_SRC=`pwd`
  '';
}
