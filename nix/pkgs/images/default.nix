# It would be cool to produce OCI images instead of docker images to
# avoid dependency on docker tool chain. Though the maturity of OCI
# builder in nixpkgs is questionable which is why we postpone this step.

{ stdenv
, busybox
, dockerTools
, git
, lib
, utillinux
, control-plane
, tini
}:
let
  env = stdenv.lib.makeBinPath [ busybox utillinux ];
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
  agent-images = { build }: {
    core = build-agent-image { inherit build; name = "core"; };
    rest = build-agent-image {
      inherit build; name = "rest";
      config = { ExposedPorts = { "8080/tcp" = { }; "8081/tcp" = { }; }; };
    };
    jsongrpc = build-agent-image { inherit build; name = "jsongrpc"; };
  };
in
{
  agents = agent-images { build = "release"; };
  agents-dev = agent-images { build = "debug"; };
}
