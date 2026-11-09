# The pinned rust toolchain only, eg: to update the Cargo.lock
# cc: optional compiler to propagate, so its setup hook configures the cc env (NIX_CC, LD, etc)
{ pkgs ? import ../pkgs.nix, rust-profile ? "stable", cc ? null }:
let
  channel = import ../lib/rust.nix { inherit pkgs; };
  rust = channel.default_src.${rust-profile}.overrideAttrs (_: {
    propagatedBuildInputs = [ ];
    depsHostHostPropagated = pkgs.lib.optional (cc != null) cc;
    depsTargetTargetPropagated = [ ];
  });
in
pkgs.mkShellNoCC {
  name = "rust-shell";
  buildInputs = [ rust ];
  passthru = { inherit rust; };
  # the propagated cc's setup hook sets these
  shellHook = pkgs.lib.optionalString (cc != null) ''
    unset CC
    unset AR
  '';
}
