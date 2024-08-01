{ lib, stdenv, git, sourcer, tag ? "" }:
let
  whitelistSource = src: allowedPrefixes:
    builtins.path {
      filter = (path: type:
        (lib.any
          (allowedPrefix:
            (lib.hasPrefix (toString (src + "/${allowedPrefix}")) path) ||
            (type == "directory" && lib.hasPrefix path (toString (src + "/${allowedPrefix}")))
          )
          allowedPrefixes)
        # there's no reason for this to be part of the build
        && path != (toString (src + "/utils/dependencies/scripts/release.sh"))
      );
      path = src;
      name = "controller";
    };
in
{
  inherit whitelistSource;

  git-src = whitelistSource ../../. [ ".git" ];
  repo-org = whitelistSource ../../utils/dependencies/scripts [ "git-org-name.sh" ];
}
