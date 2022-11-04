#!/usr/bin/env bash

# Runs the python bdd tests
# With no arguments the whole bdd feature set will be tested
# To test with specific arguments, simply provide them, eg:
# scripts/python/test.sh tests/bdd/features/volume/create/test_feature.py -k test_sufficient_suitable_pools
# For faster test cycle, the env variable FAST can be set to anything, which will skip building certain dependencies.
# Before using it, make sure they are already built!
# Eg: FAST=1 scripts/python/test.sh tests/bdd/features/volume/create/test_feature.py -k test_sufficient_suitable_pools

set -e

SCRIPT_DIR="$(dirname "$0")"
export ROOT_DIR="$SCRIPT_DIR/../.."

cleanup_handler() {
  "$ROOT_DIR"/scripts/python/test_residue_cleanup.sh
}

# FAST mode to avoid rebuilding certain dependencies
if [ -n "$FAST" ]; then
  echo "FAST enabled - will not rebuild the csi&openapi clients nor the deployer. (Make sure they are built already)"
fi

# shellcheck source=/dev/null
. "$ROOT_DIR"/tests/bdd/setup.sh

trap cleanup_handler ERR INT QUIT TERM HUP

# Extra arguments will be provided directly to pytest, otherwise the bdd folder will be tested with default arguments
if [ $# -eq 0 ]; then
  pytest "$BDD_TEST_DIR"
else
  pytest "$@"
fi
