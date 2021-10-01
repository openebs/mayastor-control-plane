#!/usr/bin/env bash

DIR_NAME="$(dirname "$(pwd)/${BASH_SOURCE[0]}")"

virtualenv --no-setuptools "$DIR_NAME"/venv

# Generate gRPC protobuf stubs.
python -m grpc_tools.protoc --proto_path=../../rpc/mayastor-api/protobuf/ --grpc_python_out=. --python_out=. csi.proto

# shellcheck disable=SC1091
source "$DIR_NAME"/venv/bin/activate

pip install -r "$DIR_NAME"/requirements.txt

export PYTHONPATH=$PYTHONPATH:$DIR_NAME/openapi
export ROOT_DIR="$DIR_NAME/../.."
