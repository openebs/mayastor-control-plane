# It would be cool to produce OCI images instead of docker images to
# avoid dependency on docker tool chain. Though the maturity of OCI
# builder in nixpkgs is questionable which is why we postpone this step.

{ busybox
, dockerTools
, lib
, utillinux
, control-plane
, tini
}:
let
  build-control-plane-image = { build, name, binary, config ? { } }: dockerTools.buildImage {
    tag = control-plane.version;
    created = "now";
    name = "mayadata/mayastor-${name}";
    contents = [ tini busybox control-plane.${build}.${name} ];
    config = { Entrypoint = [ "tini" "--" "${binary}" ]; } // config;
  };
  build-agent-image = { build, name, config ? { } }: build-control-plane-image {
    inherit build name;
    binary = "${name}-agent";
  };
  build-rest-image = { build }: build-control-plane-image {
    inherit build;
    name = "rest";
    binary = "rest";
    config = { ExposedPorts = { "8080/tcp" = { }; "8081/tcp" = { }; }; };
  };
  agent-images = { build }: {
    core = build-agent-image { inherit build; name = "core"; };
    jsongrpc = build-agent-image { inherit build; name = "jsongrpc"; };
  };
in
{
  agents = agent-images { build = "release"; };
  agents-dev = agent-images { build = "debug"; };
  rest = build-rest-image {
    build = "release";
  };
  rest-dev = build-rest-image {
    build = "debug";
  };
}
