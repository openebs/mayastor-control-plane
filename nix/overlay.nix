self: super: {
  images = super.callPackage ./pkgs/images { };
  control-plane = super.callPackage ./pkgs/control-plane { };
  utils = super.callPackage ./pkgs/utils { };
  openapi-generator = super.callPackage ./pkgs/openapi-generator { };
}
