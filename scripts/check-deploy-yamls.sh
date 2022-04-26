#!/usr/bin/env bash

set -e

SCRIPTDIR=$(dirname "$0")
ROOTDIR="$SCRIPTDIR"/../
DEPLOYDIR="$ROOTDIR"/deploy

PROFILE=release
TAG=release-1.0.2

"$SCRIPTDIR"/generate-deploy-yamls.sh -t "$TAG" "$PROFILE"

git diff --exit-code "$DEPLOYDIR" 1>/dev/null && exit 0
