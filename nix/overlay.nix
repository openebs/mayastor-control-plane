self: super: {
  images = super.callPackage ./pkgs/images { };
  control-plane = super.callPackage ./pkgs/control-plane { };
  openapi-generator = super.callPackage ./pkgs/openapi-generator { };
}
