#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$0")"

cleanup_handler() {
  "$SCRIPT_DIR"/rust/deployer-cleanup.sh || true
}

trap cleanup_handler ERR INT QUIT TERM HUP EXIT
cleanup_handler

set -euxo pipefail
# test dependencies
cargo build --bins
for test in deployer-cluster grpc agents rest io-engine-tests shutdown csi-driver; do
    cargo test -p ${test} -- --test-threads=1
done
