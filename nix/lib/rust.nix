{ sources ? import ../sources.nix }:
let
  pkgs =
    import sources.nixpkgs { overlays = [ (import sources.rust-overlay) ]; };
  static_target = pkgs.rust.toRustTargetSpec pkgs.pkgsStatic.stdenv.hostPlatform;
in
rec {
  rust_default = { override ? { } }: rec {
    nightly = pkgs.rust-bin.nightly."2021-06-22".default.override (override);
    stable = pkgs.rust-bin.stable.latest.default.override (override);
  };
  default = rust_default { };
  static = rust_default { override = { targets = [ "${static_target}" ]; }; };
  windows_cross = rust_default {
    override = { targets = [ "${pkgs.rust.toRustTargetSpec pkgs.pkgsCross.mingwW64.hostPlatform}" ]; };
  };
}
