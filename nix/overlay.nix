self: super: {
  images = super.callPackage ./pkgs/images { };
  mayastor-src = super.fetchFromGitHub rec {
    name = "mayastor-${rev}-source";
    owner = "openebs";
    repo = "Mayastor";
    # Use rev from the RPC patch in the workspace's Cargo.toml
    rev = (builtins.fromTOML (builtins.readFile ../Cargo.toml)).patch.crates-io.rpc.rev;
    sha256 = "1qdr9aj3z5jpbdrzqdxkh3ga98wq9ivsr5qrc1g6n0j9w5pjk2ry";
  };
  control-plane = super.callPackage ./pkgs/control-plane { };
  openapi-generator = super.callPackage ./pkgs/openapi-generator { };
}
