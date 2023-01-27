{ pkgs, lib, stdenv, fetchFromGitHub, maven, jdk, jre, makeWrapper }:

let
  src = fetchFromGitHub (lib.importJSON ./source.json);
  version = "6.1.0-${src.rev}";

  # perform fake build to make a fixed-output derivation out of the files downloaded from maven central
  deps = stdenv.mkDerivation {
    name = "openapi-generator-${version}-deps";
    inherit version;
    inherit src;
    nativeBuildInputs = [ jdk maven ];
    buildPhase = ''
      runHook preBuild

      while mvn package -Dmaven.test.skip=true -Dmaven.repo.local=$out/.m2; [ $? = 1 ]; do
        echo "timeout, restart maven to continue downloading"
      done

      runHook postBuild
    '';
    # keep only *.{pom,jar,sha1,nbm} and delete all ephemeral files with lastModified timestamps inside
    installPhase =
      "find $out/.m2 -type f -regex '.+\\(\\.lastUpdated\\|resolver-status\\.properties\\|_remote\\.repositories\\)' -delete";
    outputHashAlgo = "sha256";
    outputHashMode = "recursive";
    outputHash = if stdenv.hostPlatform.isDarwin then "sha256-9Li0uSD39ZwptIRgOXeBkLeZvfy/9w69faNDm75zdws=" else "sha256-MieSA5Y8u35H1xdP27A+YDekyyQ6CThNXOjQ82ArM7U=";
  };
in
stdenv.mkDerivation rec {
  inherit version;
  inherit src;
  pname = "openapi-generator-cli";
  jarfilename = "openapi-generator-cli.jar";

  nativeBuildInputs = [ jre maven makeWrapper ];

  buildPhase = ''
    runHook preBuild

    # 'maven.repo.local' must be writable so copy it out of nix store
    cp -R $src repo
    chmod +w -R repo
    cd repo
    mvn package --offline -Dmaven.test.skip=true -Dmaven.repo.local=$(cp -dpR ${deps}/.m2 ./ && chmod +w -R .m2 && pwd)/.m2

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    install -D modules/openapi-generator-cli/target/${jarfilename} "$out/share/java/${jarfilename}"

    makeWrapper ${jre}/bin/java $out/bin/${pname} --add-flags "-jar $out/share/java/${jarfilename}"

    runHook postInstall
  '';

  meta = with lib; {
    description =
      "Allows generation of API client libraries (SDK generation), server stubs and documentation automatically given an OpenAPI Spec";
    homepage = "https://github.com/openebs/openapi-generator";
    license = licenses.asl20;
    maintainers = [ maintainers.tiagolobocastro ];
  };
}
