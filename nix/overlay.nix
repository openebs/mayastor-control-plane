{ allInOne ? true, incremental ? false, img_tag ? "", tag ? "" }:
self: super: {
  images = super.callPackage ./pkgs/images { inherit img_tag; };
  control-plane = super.callPackage ./pkgs/control-plane { inherit allInOne incremental tag; };
  openapi-generator = super.callPackage ./pkgs/openapi-generator { };
  xfsprogs_5_11 = (import (super.sources).nixpkgs-v1 { }).xfsprogs;
}
