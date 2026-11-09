# Runtime dependencies required by the tests
{ pkgs ? import ../pkgs.nix }:
with pkgs;
let
  # python environment for tests/bdd
  pytest_inputs = python3.withPackages
    (ps: with ps; [ virtualenv grpcio grpcio-tools black isort autoflake ]);
in
mkShellNoCC {
  name = "test-shell";
  buildInputs = [
    docker
    etcd
    fio
    rustfs-cli
    pytest_inputs
    python3
  ] ++ lib.optionals (system != "aarch64-darwin") [
    e2fsprogs
    xfsprogs_5_16
    btrfs-progs
    nvme-cli
    # python3.9-pyopenssl-22.0.0 marked as broken but fixed on master..
    tini
    udev
    lvm2
    rdma-core
  ];

  shellHook = ''
    # variables used to easily create containers with docker files
    export ETCD_BIN="${etcd}/bin/etcd"
    export ETCDCTL_API="3"
    # point `rc` at the deployer's object store, so `rc ls local/default` works out of the box
    export RC_HOST_local="http://GKde9ade9ade9ade9ade9ade9a:a9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9ed@localhost:3900"
  '';
}
