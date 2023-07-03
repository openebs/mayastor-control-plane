#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$0")"
export ROOT_DIR="$SCRIPT_DIR/../.."

sudo nvme disconnect-all
"$ROOT_DIR"/target/debug/deployer stop

for c in $(docker ps -a --filter "label=io.composer.test.name" --format '{{.ID}}') ; do
  docker kill "$c"
  docker rm -v "$c"
done

for n in $(docker network ls --filter "label=io.composer.test.name" --format '{{.ID}}') ; do
  docker network rm "$n" || ( sudo systemctl restart docker && docker network rm "$n" )
done

exit 0