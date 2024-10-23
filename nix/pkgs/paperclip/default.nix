{ lib, sources, pkgs }:
let
  src_json = lib.importJSON ./source.json;
  hostPlatform = pkgs.rust.toRustTargetSpec pkgs.pkgsStatic.stdenv.hostPlatform;
in
pkgs.stdenv.mkDerivation {
  pname = src_json.repo;
  version = src_json.rev;

  src = pkgs.fetchurl {
    name = "paperclip-${src_json.rev}.tar.gz";
    url = "https://github.com/${src_json.owner}/${src_json.repo}/releases/download/${src_json.rev}/paperclip-${hostPlatform}.tar.gz";
    sha256 = src_json.hash;
  };

  sourceRoot = ".";

  installPhase = ''
    runHook preInstall
    install -m755 -D paperclip-ng $out/bin/paperclip-ng
    runHook postInstall
  '';

  meta = with lib; {
    homepage = "https://github.com/${src_json.owner}/${src_json.repo}";
    description = "OpenApi v3 Generator";
  };
}
