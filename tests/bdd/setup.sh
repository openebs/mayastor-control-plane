#!/usr/bin/env bash

DIR_NAME="$(dirname "$0")"

virtualenv --no-setuptools "$DIR_NAME"/venv

shellcheck disable=SC1091
source "$DIR_NAME"/venv/bin/activate

pip install -r "$DIR_NAME"/requirements.txt
export PYTHONPATH=$PYTHONPATH:$DIR_NAME/openapi
export ROOT_DIR="$DIR_NAME/../.."
