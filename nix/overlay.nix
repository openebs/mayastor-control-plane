self: super: {
  images = super.callPackage ./pkgs/images { };
  mayastor-src = super.fetchFromGitHub {
    owner = "openebs";
    repo = "Mayastor";
    # Use rev from the RPC patch in the workspace's Cargo.toml
    rev = (builtins.fromTOML (builtins.readFile ../Cargo.toml)).patch.crates-io.rpc.rev;
    sha256 = "0znrxkr3fkzhhxzwdj5y4y76nsadmxpdiy08969phwzf4gf091yk";
  };
  control-plane = super.callPackage ./pkgs/control-plane { };
}
