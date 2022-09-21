{ system ? null, allInOne ? true, incremental ? false, img_tag ? "" }:
let
  sources = import ./nix/sources.nix;
  hostSystem = (import sources.nixpkgs { }).hostPlatform.system;
  pkgs = import sources.nixpkgs {
    overlays = [ (_: _: { inherit sources; }) (import ./nix/overlay.nix { inherit allInOne incremental img_tag; }) ];
    system = if system != null then system else hostSystem;
  };
in
pkgs
