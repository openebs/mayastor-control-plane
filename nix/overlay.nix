{ allInOne ? true, incremental ? false }:
self: super: {
  images = super.callPackage ./pkgs/images { };
  control-plane = super.callPackage ./pkgs/control-plane { inherit allInOne incremental; };
  utils = super.callPackage ./pkgs/utils { inherit incremental; };
  openapi-generator = super.callPackage ./pkgs/openapi-generator { };
}
