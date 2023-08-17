#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$0")"
ARGS=${1}

cleanup_handler() {
  ERROR=$?
  "$SCRIPT_DIR"/deployer-cleanup.sh || true
  if [ $ERROR != 0 ]; then exit $ERROR; fi
}

cleanup_handler >/dev/null
trap cleanup_handler INT QUIT TERM HUP EXIT

set -euxo pipefail
# test dependencies
cargo build --bins
cargo_test="cargo test"
for test in deployer-cluster grpc agents rest io-engine-tests shutdown csi-driver; do
    cargo_test="$cargo_test -p $test"
done
$cargo_test ${ARGS} -- --test-threads=1
