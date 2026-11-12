{ norust ? false, devrustup ? true, rust-profile ? "stable", io-engine ? "" }:
let
  pkgs = import ./nix/pkgs.nix;
in
with pkgs;
let
  norust_moth =
    "You have requested an environment without rust, you should provide it! Hint: use rustup tool.";
  devrustup_moth =
    "You have requested an environment for rustup, you should provide it!";
  io-engine-moth = "Using the following io-engine binary: ${io-engine}";
  rust-shell = import ./nix/shells/rust.nix { inherit pkgs rust-profile; cc = clang; };
  build-shell = import ./nix/shells/build.nix { inherit pkgs; };
  test-shell = import ./nix/shells/test.nix { inherit pkgs; };
  rust = rust-shell.rust;
  usePreCommit = builtins.getEnv "IN_NIX_SHELL" == "impure" && builtins.getEnv "CI" != "1";
  pre-commit = pkgs.runCommand "pre-commit" { } ''
    mkdir -p $out/bin
    cp ${pkgs.pre-commit}/bin/pre-commit $out/bin/pre-commit
  '';
in
mkShellNoCC {
  name = "control-plane-shell";
  inputsFrom = [ build-shell test-shell ] ++ pkgs.lib.optional (!norust) rust-shell;
  buildInputs = [
    commitlint
    cowsay
    jq
    nixpkgs-fmt
    nix
    openapi-generator-cli
    utillinux
  ] ++ pkgs.lib.optional (usePreCommit) pre-commit;

  # using the nix rust toolchain
  USE_NIX_RUST = "${toString (!norust)}";
  # copy the rust toolchain to a writable directory, see: https://github.com/rust-lang/cargo/issues/10096
  # the whole toolchain is copied to allow the src to be retrievable through "rustc --print sysroot"
  RUST_TOOLCHAIN = ".rust-toolchain/${rust.version}";
  RUST_TOOLCHAIN_NIX = pkgs.lib.optional (!norust) "${rust}";

  shellHook = ''
    ./scripts/nix/git-submodule-init.sh
    if [ "${toString usePreCommit}" = "1" ]; then
      echo
      pre-commit install
      pre-commit install --hook commit-msg
    fi
    # Looks like vscode with the nix plugin is running commands in different shells without setting the temp variables
    # Since nix-shell by default nests the temp variables, we need to set them here
    if [ "$VSCODE_CLI" = "1" ] || [ -n "$VSCODE_CWD" ]; then
      export TMPDIR=/tmp
      export TMP=/tmp
    fi

    ${pkgs.lib.optionalString (norust) "cowsay ${norust_moth}"}
    ${pkgs.lib.optionalString (norust) "echo"}

    rust_version="${rust.version}" rustup_channel="${lib.strings.concatMapStringsSep "-" (x: x) (lib.lists.drop 1 (lib.strings.splitString "-" rust.version))}" \
    dev_rustup="${toString (devrustup)}" devrustup_moth="${devrustup_moth}" . ./scripts/rust/env-setup.sh

    export WORKSPACE_ROOT=`pwd`
    [ ! -z "${io-engine}" ] && cowsay "${io-engine-moth}"
    [ ! -z "${io-engine}" ] && export IO_ENGINE_BIN="${io-engine-moth}"
    export PATH="$(pwd)/target/debug:$PATH"
    export SUDO=$(which sudo 2>/dev/null || echo /run/wrappers/bin/sudo)

    DOCKER_CONFIG=~/.docker/config.json
    if [ -f "$DOCKER_CONFIG" ]; then
      DOCKER_TOKEN=$(cat ~/.docker/config.json | jq '.auths."https://index.docker.io/v1/".auth // empty' | tr -d '"' | base64 -d)
      export DOCKER_USER=$(echo $DOCKER_TOKEN | cut -d':' -f1)
      export DOCKER_PASS=$(echo $DOCKER_TOKEN | cut -d':' -f2)
    fi
  '';
}
