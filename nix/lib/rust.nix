{ sources ? import ../sources.nix }:
let
  pkgs =
    import sources.nixpkgs { overlays = [ (import sources.rust-overlay) ]; };
  static_target =
    pkgs.rust.toRustTargetSpec pkgs.pkgsStatic.stdenv.hostPlatform;
in
rec {
  rust_default = { override ? { } }: rec {
    nightly_pkg = pkgs.rust-bin.nightly."2021-11-22";
    stable_pkg = pkgs.rust-bin.stable.latest;

    nightly = nightly_pkg.default.override (override);
    stable = stable_pkg.default.override (override);

    nightly_src = nightly_pkg.rust-src;
    release_src = stable_pkg.rust-src;
  };
  default = rust_default { };
  default_src = rust_default {
    override = { extensions = [ "rust-src" ]; };
  };
  static = rust_default { override = { targets = [ "${static_target}" ]; }; };
  windows_cross = rust_default {
    override = { targets = [ "${pkgs.rust.toRustTargetSpec pkgs.pkgsCross.mingwW64.hostPlatform}" ]; };
  };
}
