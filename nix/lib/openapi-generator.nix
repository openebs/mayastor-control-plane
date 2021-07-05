{ lib, stdenv, fetchFromGitHub, maven, jdk, jre, makeWrapper }:

let
  rev = "79b2076";
  version = "5.2.0-${rev}";

  src = fetchFromGitHub {
    owner = "openebs";
    repo = "openapi-generator";
    rev = "${rev}";
    #sha256 = lib.fakeSha256;
    sha256 = "121zghqs5jgpc8bxh3nqpgvjfqj5jbwwawq7kcy1d5abxgvfy3wh";
  };

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
    installPhase = ''find $out/.m2 -type f -regex '.+\(\.lastUpdated\|resolver-status\.properties\|_remote\.repositories\)' -delete'';
    outputHashAlgo = "sha256";
    outputHashMode = "recursive";
    outputHash = "1ikrd5ssb3mn4hww5y0ijyz22367yv9hgqrwm6h1h5icny401xlf";
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
    description = "Allows generation of API client libraries (SDK generation), server stubs and documentation automatically given an OpenAPI Spec";
    homepage = "https://github.com/openebs/openapi-generator";
    license = licenses.asl20;
    maintainers = [ maintainers.tiagolobocastro ];
  };
}
