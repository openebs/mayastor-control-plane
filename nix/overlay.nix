self: super: {
  images = super.callPackage ./pkgs/images { };
  mayastor-src = super.fetchFromGitHub rec {
    name = "mayastor-${rev}-source";
    owner = "openebs";
    repo = "Mayastor";
    # Use rev from the RPC patch in the workspace's Cargo.toml
    rev = (builtins.fromTOML (builtins.readFile ../Cargo.toml)).patch.crates-io.rpc.rev;
    sha256 = "0sh8rnmbkxfg36pgsp060kyk83nwqlhs4fxzasq3zk5bn4mqhs47";
  };
  control-plane = super.callPackage ./pkgs/control-plane { };
}
