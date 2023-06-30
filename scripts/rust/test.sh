#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$0")"
export ROOT_DIR="$SCRIPT_DIR/../.."

cleanup_handler() {
  sudo nvme disconnect-all || true
  "$ROOT_DIR"/target/debug/deployer stop || true

  for c in $(docker ps -a --filter "label=io.composer.test.name" --format '{{.ID}}') ; do
    docker kill "$c" || true
    docker rm -v "$c" || true
  done

  for n in $(docker network ls --filter "label=io.composer.test.name" --format '{{.ID}}') ; do
    docker network rm "$n" || ( sudo systemctl restart docker && docker network rm "$n" )
  done
}

trap cleanup_handler ERR INT QUIT TERM HUP
cleanup_handler

set -euxo pipefail
# test dependencies
cargo build --bins
for test in deployer-cluster grpc agents rest io-engine-tests shutdown csi-driver; do
    cargo test -p ${test} -- --test-threads=1
done
cleanup_handler
