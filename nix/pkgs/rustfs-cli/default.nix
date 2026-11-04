{ lib, pkgs }:
let
  src_json = lib.importJSON ./source.json;
  platforms = {
    "x86_64-linux" = "linux-amd64";
    "aarch64-linux" = "linux-arm64";
    "x86_64-darwin" = "macos-amd64";
    "aarch64-darwin" = "macos-arm64";
  };
  system = pkgs.stdenv.hostPlatform.system;
  platform = platforms.${system} or (throw "rustfs-cli: unsupported system ${system}");
in
pkgs.stdenv.mkDerivation {
  pname = "rustfs-cli";
  version = lib.removePrefix "v" src_json.rev;

  # The linux assets without the `-gnu` suffix are static-pie, so the binary is installed as-is with no patchelf.
  src = pkgs.fetchurl {
    name = "rustfs-cli-${platform}-${src_json.rev}.tar.gz";
    url = "https://github.com/${src_json.owner}/${src_json.repo}/releases/download/${src_json.rev}/rustfs-cli-${platform}-${src_json.rev}.tar.gz";
    sha256 = src_json.hash.${platform};
  };

  sourceRoot = ".";

  installPhase = ''
    runHook preInstall
    install -m755 -D rc $out/bin/rc
    runHook postInstall
  '';

  meta = with lib; {
    homepage = "https://github.com/${src_json.owner}/${src_json.repo}";
    description = "S3-compatible command-line client, used against the deployer's object store";
    mainProgram = "rc";
    platforms = builtins.attrNames platforms;
  };
}
