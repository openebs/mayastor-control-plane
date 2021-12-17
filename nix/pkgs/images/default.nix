# It would be cool to produce OCI images instead of docker images to
# avoid dependency on docker tool chain. Though the maturity of OCI
# builder in nixpkgs is questionable which is why we postpone this step.

{ busybox, dockerTools, lib, utillinux, control-plane, tini }:
let
  image_suffix = { "release" = ""; "debug" = "-dev"; "coverage" = "-cov"; };
  build-control-plane-image = { build, name, config ? { } }:
    dockerTools.buildImage {
      tag = control-plane.version;
      created = "now";
      name = "mayadata/mcp-${name}${image_suffix.${build}}";
      contents = [ tini busybox control-plane.${build}.${name} ];
      config = {
        Entrypoint = [ "tini" "--" control-plane.${build}.${name}.binary ];
      } // config;
    };
  build-agent-image = { build, name, config ? { } }:
    build-control-plane-image { inherit build name; };
  build-rest-image = { build }:
    build-control-plane-image {
      inherit build;
      name = "rest";
      config = {
        ExposedPorts = {
          "8080/tcp" = { };
          "8081/tcp" = { };
        };
      };
    };
  build-msp-operator-image = { build }:
    build-control-plane-image {
      inherit build;
      name = "msp-operator";
    };
  build-csi-controller-image = { build }:
    build-control-plane-image {
      inherit build;
      name = "csi-controller";
    };
in
let
  build-agent-images = { build }: {
    core = build-agent-image {
      inherit build;
      name = "core";
    };
    jsongrpc = build-agent-image {
      inherit build;
      name = "jsongrpc";
    };
  };
  build-operator-images = { build }: {
    msp = build-msp-operator-image { inherit build; };
  };
  build-csi-images = { build }: {
    controller = build-csi-controller-image { inherit build; };
  };
in
let
  build-images = { build }: {
    agents = build-agent-images { inherit build; } // {
      recurseForDerivations = true;
    };
    operators = build-operator-images { inherit build; } // {
      recurseForDerivations = true;
    };
    csi = build-csi-images { inherit build; } // {
      recurseForDerivations = true;
    };
    rest = build-rest-image { inherit build; };
  };
in
{
  release = build-images { build = "release"; };
  debug = build-images { build = "debug"; };
}
