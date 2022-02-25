#!/usr/bin/env bash

set -euo pipefail

FORCE=
while [ "$#" -gt 0 ]; do
  case $1 in
    -f|--force)
      FORCE="true"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

for mod in `git config --file .gitmodules --get-regexp path | awk '{ print $2 }'`; do
    if [ -n "$FORCE" ] || [ ! -f $mod/.git ]; then
       git submodule update --init --recursive $mod
    fi
done

