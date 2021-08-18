#!/usr/bin/env bash

echo "Waiting for $ES_HOST $ES_PORT..."

while 'true'; do
  if nc -vz "$ES_HOST" "$ES_PORT"; then
    echo "OK!"
    exec "$@"
  fi

  echo -n "+";
  sleep 0.2
done
