{ lib, stdenv, git, tag ? "" }:
let
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix: lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;
in
stdenv.mkDerivation {
  name = "git-version";
  src = whitelistSource ../../. [ ".git" ];
  outputs = [ "out" "long" "tag_or_long" ];

  buildCommand = ''
    cd $src
    export GIT_DIR=".git"

    vers=${tag}
    if [ -z "$vers" ]; then
      vers=`${git}/bin/git tag --points-at HEAD`
    fi
    if [ -z "$vers" ]; then
      vers=`${git}/bin/git rev-parse --short=12 HEAD`
    fi
    echo -n $vers >$out

    if [ "${tag}" != "" ]; then
      vers="${tag}-0-g$(${git}/bin/git rev-parse --short=12 HEAD)"
    else
      vers=$(${git}/bin/git describe --abbrev=12 --always --long)
    fi
    echo -n $vers >$long

    # when we point to a tag, it's just the tag
    vers=${tag}
    if [ -z "$vers" ]; then
        vers=$(${git}/bin/git describe --abbrev=12 --always)
    fi
    echo -n $vers >$tag_or_long
  '';
}
