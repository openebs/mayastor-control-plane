self: super: {
  images = super.callPackage ./pkgs/images { };
  mayastor-src = super.fetchFromGitHub rec {
    name = "mayastor-${rev}-source";
    owner = "openebs";
    repo = "Mayastor";
    # Use rev from the RPC patch in the workspace's Cargo.toml
    rev = (builtins.fromTOML (builtins.readFile ../Cargo.toml)).patch.crates-io.rpc.rev;
    sha256 = "uLdGaHuHRV3QEcnBgMmzYtXLXur+BgAdzVbGLe6vX4M=";

  };
  control-plane = super.callPackage ./pkgs/control-plane { };
  openapi-generator = super.callPackage ./pkgs/openapi-generator { };
}
