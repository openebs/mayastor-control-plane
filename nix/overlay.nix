{ allInOne ? true, incremental ? false, img_tag ? "", tag ? "", img_org ? "" }:
self: super: {
  sourcer = super.callPackage ./lib/sourcer.nix { };
  images = super.callPackage ./pkgs/images { inherit img_tag img_org; };
  control-plane = super.callPackage ./pkgs/control-plane { inherit allInOne incremental tag; };
  openapi-generator = super.callPackage ./pkgs/openapi-generator { };
  xfsprogs_5_16 = (import (super.sources).nixpkgs-22_05 { }).xfsprogs;
}
