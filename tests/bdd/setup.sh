#!/usr/bin/env bash

set -euxo pipefail

if [ "${SRCDIR:-unset}" = unset ]
then
  echo "SRCDIR must be set to the root of your working tree" 2>&1
  exit 1
fi

cd "$SRCDIR"

virtualenv --no-setuptools tests/bdd/venv
source ./tests/bdd/venv/bin/activate
pip install -r tests/bdd/requirements.txt
export PYTHONPATH=$PYTHONPATH:$SRCDIR/tests/bdd/openapi
