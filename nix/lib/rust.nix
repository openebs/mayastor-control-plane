{ pkgs }:
let
  sources = import ../sources.nix;
  makeRustTarget = platform: platform.rust.rustcTargetSpec;
  static_target = makeRustTarget pkgs.pkgsStatic.stdenv.hostPlatform;
in
rec {
  inherit makeRustTarget;
  naersk_package = channel: pkgs.callPackage sources.naersk {
    rustc = channel.stable;
    cargo = channel.stable;
    # Rewrite crates.io API URLs to static CDN to avoid intermittent 403s
    fetchurl = attrs@{ url, ... }:
      let
        m = builtins.match "https://crates\\.io/api/v1/crates/([^/]+)/([^/]+)/download" url;
        resolvedUrl =
          if m != null then
            "https://static.crates.io/crates/${builtins.elemAt m 0}/${builtins.elemAt m 0}-${builtins.elemAt m 1}.crate"
          else url;
      in
      pkgs.fetchurl (attrs // { url = resolvedUrl; });
  };
  rust_default = { override ? { } }: rec {
    nightly_pkg = pkgs.rust-bin.nightly."2025-06-26";
    stable_pkg = pkgs.rust-bin.stable."1.88.0";

    nightly = nightly_pkg.default.override (override);
    stable = stable_pkg.default.override (override);

    nightly_src = nightly_pkg.rust-src;
    release_src = stable_pkg.rust-src;
  };
  default = rust_default { };
  default_src = rust_default {
    override = { extensions = [ "rust-src" ]; };
  };
  static = { target ? makeRustTarget pkgs.pkgsStatic.stdenv.hostPlatform }: rust_default {
    override = { targets = [ "${target}" ]; };
  };
  hostStatic = rust_default { override = { targets = [ "${makeRustTarget pkgs.pkgsStatic.stdenv.hostPlatform}" ]; }; };
  windows_cross = rust_default {
    override = { targets = [ "${makeRustTarget pkgs.pkgsCross.mingwW64.hostPlatform}" ]; };
  };
}
