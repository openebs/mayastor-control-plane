#!/usr/bin/env bash

set -e

DIR_NAME="$(dirname "$(realpath "${BASH_SOURCE[0]:-"$0"}")")"
export ROOT_DIR="$DIR_NAME/../.."
TESTS_DIR="$ROOT_DIR"/tests
BDD_DIR="$TESTS_DIR"/bdd
VENV_DIR="$BDD_DIR/venv"
CSI_OUT="$DIR_NAME/autogen"
CSI_PROTO="$DIR_NAME"/../../rpc/api/protobuf/
HA_OUT="$DIR_NAME/autogen"
HA_PROTO="$DIR_NAME"/../../control-plane/grpc/proto/v1/ha

virtualenv --no-setuptools "$VENV_DIR"

VENV_PTH="$CSI_OUT:$BDD_DIR"
SETUP_ARGS=(--venv-pth "$VENV_DIR" "$VENV_PTH")

# if FAST is set then we do not regenerate the python csi, the python openapi and the rust component binaries
if [ -z "$FAST" ]; then
  SETUP_ARGS=("${SETUP_ARGS[@]}" --csi "$CSI_PROTO" "$CSI_OUT" --build-bins --build-openapi --ha "$HA_PROTO" "$HA_OUT")
fi

sh "$ROOT_DIR"/scripts/python/venv-setup-prep.sh "${SETUP_ARGS[@]}"

set +e

# shellcheck source=/dev/null
source "$VENV_DIR"/bin/activate

pip install -r "$BDD_DIR"/requirements.txt
