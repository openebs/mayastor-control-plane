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
  }));
  tag = if img_tag != "" then img_tag else control-plane.version;
  image_suffix = { "release" = ""; "debug" = "-dev"; "coverage" = "-cov"; };
  build-control-plane-image = { buildType, name, package, config ? { } }:
    let
      imageContents = [ tini busybox package ];
      mergedRootfs = pkgs.buildEnv {
        name = "rootfs-${img_prefix}-${name}${image_suffix.${buildType}}-${tag}";
        paths = imageContents;
        pathsToLink = [ "/" ];
      };
      sbom = pkgs.runCommand "sbom-${img_prefix}-${name}${image_suffix.${buildType}}-${tag}" {
        nativeBuildInputs = with pkgs; [ syft ];
      } ''
        mkdir -p "$out/share/sbom"
        syft "dir:${mergedRootfs}" -o "spdx-json=$out/share/sbom/image.spdx.json"
      '';
    in
    dockerTools.buildImage {
      inherit tag;
      created = "now";
      name = "${repo-org}/${img_prefix}-${name}${image_suffix.${buildType}}";
      copyToRoot = imageContents ++ [ sbom ];
      passthru = {
        inherit sbom;
      };
      config = {
        Entrypoint = [ "tini" "--" package.binary ];
      } // config;
    };
  build-agent-image = { buildType, name }:
    build-control-plane-image {
      inherit buildType;
      name = "agent-${name}";
      package = control-plane.${buildType}.agents.${name};
    };
  build-agent-cat-image = { buildType, name, category }:
    build-control-plane-image {
      inherit buildType;
      name = "agent-${category}-${name}";
      package = control-plane.${buildType}.agents.${category}.${name};
    };
  build-rest-image = { buildType }:
    build-control-plane-image {
      inherit buildType;
      name = "api-rest";
      package = control-plane.${buildType}.api-rest;
      config = {
        ExposedPorts = {
          "8080/tcp" = { };
          "8081/tcp" = { };
        };
      };
    };
  build-operator-image = { buildType, name }:
    build-control-plane-image {
      inherit buildType;
      name = "operator-${name}";
      package = control-plane.${buildType}.operators.${name};
    };
  build-csi-image = { buildType, name, config ? { } }:
    build-control-plane-image {
      inherit buildType config;
      name = "csi-${name}";
      package = control-plane.${buildType}.csi.${name};
    };
in
let
  build-agent-images = { buildType }: {
    core = build-agent-image {
      inherit buildType;
      name = "core";
    };
    jsongrpc = build-agent-image {
      inherit buildType;
      name = "jsongrpc";
    };
    ha = rec {
      build-ha-agent-image = { buildType, name }:
        build-agent-cat-image {
          inherit buildType name;
          category = "ha";
        };
      node = build-ha-agent-image {
        inherit buildType;
        name = "node";
      };
      cluster = build-ha-agent-image {
        inherit buildType;
        name = "cluster";
      };
    };
  };
  build-operator-images = { buildType }: {
    diskpool = build-operator-image { inherit buildType; name = "diskpool"; };
  };
  build-csi-images = { buildType }: {
    controller = build-csi-image { inherit buildType; name = "controller"; };
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
    rest = build-rest-image { inherit buildType; };
  };
in
{
  release = build-images { buildType = "release"; };
  debug = build-images { buildType = "debug"; };
}
