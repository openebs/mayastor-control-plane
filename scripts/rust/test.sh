#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$0")"

ARGS=""
OPTS=""
DO_ARGS=
while [ "$#" -gt 0 ]; do
  case $1 in
    --)
      DO_ARGS="y"
      shift;;
    *)
      if [ "$DO_ARGS" == "y" ]; then
        ARGS="$ARGS $1"
      else
        OPTS="$OPTS $1"
      fi
      shift;;
  esac
done

cleanup_handler() {
  ERROR=$?
  RUST_LOG="error" "$SCRIPT_DIR"/deployer-cleanup.sh || true
  if [ $ERROR != 0 ]; then exit $ERROR; fi
}

cleanup_handler >/dev/null
trap cleanup_handler INT QUIT TERM HUP EXIT

set -euxo pipefail

# build test dependencies
cargo build --bins

cargo_test="cargo test"
for package in deployer-cluster grpc agents rest io-engine-tests shutdown csi-driver; do
    cargo_test="$cargo_test -p $package"
done

$cargo_test ${OPTS} -- ${ARGS} --test-threads=1
