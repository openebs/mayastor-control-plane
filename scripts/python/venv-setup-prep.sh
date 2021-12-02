#!/usr/bin/env bash

# You do not need to call this script directly, it's used as a helper for other scripts

set -e

SCRIPTDIR=$(dirname "$0")
ROOTDIR="$SCRIPTDIR"/../../

CSI_PROTO=
CSI_OUT=
VENV_DIR=
VENV_PTH=
BUILD_RS_BINS=
BUILD_PY_OPENAPI=

missing_arg() {
  echo "Missing argument: $1"
  exit 1
}

help() {
  cat <<EOF

Usage: $0 [OPTIONS]

Common options:
  -h/--help                      Display the help message and exit.
  --csi <proto-dir> <out-dir>    Generate the csi proto bindings
          <proto-dir>  directory where the csi.proto file is located.
          <out-dir>    directory to store the generated grpc python files.
  --venv-pth <venv-dir> <paths>  Add path configuration files to the specified venv (similar to extending PYTHONPATH)
                                   hint: <paths> is a ':' separated string list.
  --build-bins                   Builds all the cargo binaries from the workspace.
  --build-openapi                Generate the openapi python client.


EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --csi)
      CSI_PROTO="$2"
      shift || missing_arg "<proto-dir>"
      CSI_OUT="$2"
      shift 2 || missing_arg "<out-dir>"
      ;;
    --venv-pth)
      VENV_DIR="$2"
      shift 2 || missing_arg "<venv-dir>"
      VENV_PTH="$1"
      shift || missing_arg "<paths>"
      ;;
    --build-bins)
      BUILD_RS_BINS="yes"
      shift
      ;;
    --build-openapi)
      BUILD_PY_OPENAPI="yes"
      shift
      ;;
    --help)
      help
      exit 0
      ;;
    *)
      echo "Invalid option '$1'"
      help
      exit 1
      ;;
  esac
done

if [ -n "$VENV_DIR" ] && [ -z "$VENV_PTH" ]; then
  echo "--venv-pth <venv-dir> <paths> is empty"
  help
  exit 1
fi

# generate the openapi python client
if [ -n "$BUILD_PY_OPENAPI" ]; then
  "$ROOTDIR"/scripts/python/generate-openapi-bindings.sh
fi

# generate the csi python client
if [ -n "$CSI_PROTO" ]; then
  if [ -z "$CSI_OUT" ]; then
    echo "--csi-out <dir> is empty"
    help
    exit 1
  fi
  mkdir -p "$CSI_OUT"
  python -m grpc_tools.protoc --proto_path="$CSI_PROTO" --grpc_python_out="$CSI_OUT" --python_out="$CSI_OUT" csi.proto
fi

# Setup the python config files (similar to extending the PYTHONPATH env, but within venv)
if [ -n "$VENV_PTH" ]; then
  for python_version in "$ROOTDIR"/tests/bdd/venv/lib/*; do
    rm "$python_version/site-packages/bdd.pth" 2>/dev/null || true
    for dir in $(echo "$VENV_PTH" | tr ':' '\n'); do
      echo "$dir" >> "$python_version/site-packages/bdd.pth"
    done
  done
fi

# compile the binaries used by the deployer
if [ -n "$BUILD_RS_BINS" ]; then
  ( cd "$ROOTDIR" && cargo build --bins )
fi
