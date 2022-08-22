#!/usr/bin/env bash

cleanup_handler() {
  for c in $(docker ps -a --filter "label=io.composer.test.name" --format '{{.ID}}') ; do
    docker kill "$c" || true
    docker rm -v "$c" || true
  done

  for n in $(docker network ls --filter "label=io.composer.test.name" --format '{{.ID}}') ; do
    docker network rm "$n" || true
  done
}

trap cleanup_handler ERR INT QUIT TERM HUP

set -euxo pipefail
# test dependencies
cargo build --bins
for test in deployer-cluster grpc agents rest io-engine-tests kubectl-plugin shutdown csi-driver; do
    cargo test -p ${test} -- --test-threads=1
done
cleanup_handler
