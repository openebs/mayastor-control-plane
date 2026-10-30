# It would be cool to produce OCI images instead of docker images to
# avoid dependency on docker tool chain. Though the maturity of OCI
# builder in nixpkgs is questionable which is why we postpone this step.

{ pkgs, xfsprogs_5_16, rdma-core, busybox, dockerTools, lib, e2fsprogs_1_46_5, btrfs-progs, utillinux, fetchurl, fetchpatch, control-plane, tini, sourcer, img_tag ? "", img_org ? "", img_prefix }:
let
  repo-org = if img_org != "" then img_org else "${builtins.readFile (pkgs.runCommand "repo_org" {
    buildInputs = with pkgs; [ git ];
   } ''
    export GIT_DIR="${sourcer.git-src}/.git"
    cp ${sourcer.repo-org}/git-org-name.sh .
    patchShebangs ./git-org-name.sh
    ./git-org-name.sh ${sourcer.git-src} --case lower --remote origin > $out
  '')}";
  xfsprogs = xfsprogs_5_16;
  e2fsprogs_1_46_2 = (e2fsprogs_1_46_5.overrideAttrs (oldAttrs: rec {
    version = "1.46.2";
    src = fetchurl {
      url = "mirror://sourceforge/${oldAttrs.pname}/${oldAttrs.pname}-${version}.tar.gz";
      sha256 = "1mawh41ikrxy2nwhxdrza0dcxhs061mfrq8jraghbp2vyss2d7zp";
    };
    patches = [
      (fetchpatch {
        name = "CVE-2022-1304.patch";
        url = "https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/patch/?id=ab51d587bb9b229b1fade1afd02e1574c1ba5c76";
        sha256 = "sha256-YEEow34/81NBOc6F6FS6i505FCQ7GHeIz0a0qWNs7Fg=";
      })
      (fetchpatch {
        # avoid using missing __GNUC_PREREQ(X,Y)
        url = "https://raw.githubusercontent.com/void-linux/void-packages/9583597eb3e6e6b33f61dbc615d511ce030bc443/srcpkgs/e2fsprogs/patches/fix-glibcism.patch";
        sha256 = "1gfcsr0i3q8q2f0lqza8na0iy4l4p3cbii51ds6zmj0y4hz2dwhb";
        excludes = [ "lib/ext2fs/hashmap.h" ];
        extraPrefix = "";
      })
    ];
    doCheck = false;
  }));
  tag = if img_tag != "" then img_tag else control-plane.version;
  image_suffix = { "release" = ""; "debug" = "-dev"; "coverage" = "-cov"; };
  license_src = sourcer.whitelistSource ../../.. [ "LICENSE" ];
  vendor = "OpenEBS";
  maintainer = "OpenEBS Authors (https://github.com/openebs/community)";
  # `release` identifies a build of a given `version`. Nothing here
  # distinguishes one rebuild of a version from another, so it is fixed rather
  # than threaded through from a build argument nothing would ever set.
  release = "1";
  build-control-plane-image = { buildType, name, package, description, summary ? description, config ? { }, user ? null }:
    dockerTools.buildImage {
      inherit tag;
      created = "now";
      extraCommands = ''
        mkdir -p licenses
        cp --no-preserve=mode ${license_src}/LICENSE licenses/LICENSE
      '';
      name = "${repo-org}/${img_prefix}-${name}${image_suffix.${buildType}}";
      copyToRoot = [ tini busybox package pkgs.cacert ];
      config = {
        Entrypoint = [ "tini" "--" package.binary ];
        Labels = {
          inherit vendor maintainer summary description release;
          name = "${img_prefix}-${name}${image_suffix.${buildType}}";
          version = tag;
        };
      } // lib.optionalAttrs (user != null) {
        User = user;
      } // config;
    };
  descriptions = {
    core = "Mayastor core agent - the control plane's central agent, reconciling nodes, pools, volumes and nexuses.";
    jsongrpc = "Mayastor JSON-gRPC agent - relays JSON-RPC calls to the data plane on behalf of the control plane.";
    ha = {
      node = "Mayastor HA node agent - watches local NVMe-oF paths and reports broken ones to the cluster agent.";
      cluster = "Mayastor HA cluster agent - drives switchover of volume targets when a node reports a failed path.";
    };
    diskpool = "Mayastor DiskPool operator - reconciles DiskPool custom resources against control plane pools.";
    controller = "Mayastor CSI controller - provisions, attaches and snapshots Mayastor volumes.";
    node = "Mayastor CSI node plugin - stages, mounts and unmounts Mayastor volumes on the node.";
    api-rest = "Mayastor REST API server - serves the control plane's OpenAPI over HTTP and HTTPS.";
  };
  build-agent-image = { buildType, name, user ? null }:
    build-control-plane-image {
      inherit buildType user;
      name = "agent-${name}";
      package = control-plane.${buildType}.agents.${name};
      description = descriptions.${name};
    };
  build-agent-cat-image = { buildType, name, category, user ? null }:
    build-control-plane-image {
      inherit buildType user;
      name = "agent-${category}-${name}";
      package = control-plane.${buildType}.agents.${category}.${name};
      description = descriptions.${category}.${name};
    };
  build-rest-image = { buildType, user ? null }:
    build-control-plane-image {
      inherit buildType user;
      name = "api-rest";
      package = control-plane.${buildType}.api-rest;
      description = descriptions.api-rest;
      config = {
        ExposedPorts = {
          "8080/tcp" = { };
          "8081/tcp" = { };
        };
      };
    };
  build-operator-image = { buildType, name, user ? null }:
    build-control-plane-image {
      inherit buildType user;
      name = "operator-${name}";
      package = control-plane.${buildType}.operators.${name};
      description = descriptions.${name};
    };
  build-csi-image = { buildType, name, config ? { }, user ? null }:
    build-control-plane-image {
      inherit buildType config user;
      name = "csi-${name}";
      package = control-plane.${buildType}.csi.${name};
      description = descriptions.${name};
    };
in
let
  build-agent-images = { buildType }: {
    core = build-agent-image {
      inherit buildType;
      name = "core";
      user = "65534:65534";
    };
    jsongrpc = build-agent-image {
      inherit buildType;
      name = "jsongrpc";
    };
    ha = rec {
      build-ha-agent-image = { buildType, name, user ? null }:
        build-agent-cat-image {
          inherit buildType name user;
          category = "ha";
        };
      node = build-ha-agent-image {
        inherit buildType;
        name = "node";
      };
      cluster = build-ha-agent-image {
        inherit buildType;
        name = "cluster";
        user = "65534:65534";
      };
    };
  };
  build-operator-images = { buildType }: {
    diskpool = build-operator-image { inherit buildType; name = "diskpool"; user = "65534:65534"; };
  };
  build-csi-images = { buildType }: {
    controller = build-csi-image { inherit buildType; name = "controller"; user = "65534:65534"; };
    node = build-csi-image {
      inherit buildType;
      name = "node";
      config = {
        Env = [ "PATH=${lib.makeBinPath [ "/" xfsprogs rdma-core e2fsprogs_1_46_2 btrfs-progs utillinux ]}" ];
      };
    };
  };
in

let
  build-images = { buildType }: {
    agents = build-agent-images { inherit buildType; } // {
      recurseForDerivations = true;
    };
    operators = build-operator-images { inherit buildType; } // {
      recurseForDerivations = true;
    };
    csi = build-csi-images { inherit buildType; } // {
      recurseForDerivations = true;
    };
    rest = build-rest-image { inherit buildType; user = "65534:65534"; };
  };
in
{
  release = build-images { buildType = "release"; };
  debug = build-images { buildType = "debug"; };
}
