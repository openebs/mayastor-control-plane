#!/usr/bin/env bash

set -euo pipefail

DIRNAME="$(dirname "$0")"
curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" -H "kbn-xsrf: true" --form file=@"$DIRNAME"/kibana_jaeger.ndjson
