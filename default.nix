{ system ? null, allInOne ? true, incremental ? false }:
let
  sources = import ./nix/sources.nix;
  hostSystem = (import sources.nixpkgs { }).hostPlatform.system;
  pkgs = import sources.nixpkgs {
    overlays = [ (_: _: { inherit sources; }) (import ./nix/overlay.nix { inherit allInOne incremental; }) ];
    system = if system != null then system else hostSystem;
  };
in
pkgs
