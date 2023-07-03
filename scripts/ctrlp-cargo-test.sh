#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$0")"

cleanup_handler() {
  ERROR=$?
  "$SCRIPT_DIR"/deployer-cleanup.sh || true
  if [ $ERROR != 0 ]; then exit $ERROR; fi
}

cleanup_handler >/dev/null
trap cleanup_handler INT QUIT TERM HUP EXIT

set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
# test dependencies
cargo build --bins
for test in composer agents rest ctrlp-tests kubectl-plugin; do
    cargo test -p ${test} -- --test-threads=1
done
