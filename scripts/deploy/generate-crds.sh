#!/usr/bin/env bash

set -e

SCRIPTDIR=$(dirname "$0")
ROOTDIR="$SCRIPTDIR"/../../
TEMPLATES="$ROOTDIR/chart/templates"
POOL="$ROOTDIR/control-plane/msp-operator"
POOL_CRD="mayastorpoolcrd.yaml"

# Regenerate the bindings only if the rest src changed
check="no"

case "$1" in
    --changes)
        check="yes"
        ;;
esac

if [[ $check = "yes" ]]; then
    git diff --cached --exit-code "$POOL" 1>/dev/null && exit 0
fi

( cd "$TEMPLATES" && cargo run --bin msp-operator -- --write-crd "$POOL_CRD" )

# If the openapi bindings were modified then fail the check
git diff --exit-code "$TEMPLATES/$POOL_CRD"
