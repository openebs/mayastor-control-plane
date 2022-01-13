{ sources ? import ../sources.nix }:
let
  pkgs =
    import sources.nixpkgs { overlays = [ (import sources.rust-overlay) ]; };
  static_target =
    pkgs.rust.toRustTargetSpec pkgs.pkgsStatic.stdenv.hostPlatform;
in
rec {
  rust_default = { override ? { } }: rec {
    nightly = pkgs.rust-bin.nightly."2021-11-22".default.override (override);
    stable = pkgs.rust-bin.stable.latest.default.override (override);
  };
  default = rust_default { };
  default_src = rust_default {
    override = { extensions = [ "rust-src" ]; };
  };
  static = rust_default { override = { targets = [ "${static_target}" ]; }; };
}
