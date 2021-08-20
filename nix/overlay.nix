self: super: {
  images = super.callPackage ./pkgs/images { };
  mayastor-src = super.fetchFromGitHub rec {
    rev = "ebe0c03e5f472e94c74889d0a1797949f7e28166";
    name = "mayastor-${rev}-source";
    owner = "openebs";
    repo = "Mayastor";
    # Use rev from the RPC patch in the workspace's Cargo.toml
    sha256 = "uLdGaHuHRV3QEcnBgMmzYtXLXur+BgAdzVbGLe6vX4M=";

  };
  control-plane = super.callPackage ./pkgs/control-plane { };
  openapi-generator = super.callPackage ./pkgs/openapi-generator { };
}
