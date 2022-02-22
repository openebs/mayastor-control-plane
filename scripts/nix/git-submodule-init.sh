#!/usr/bin/env bash

set -euo pipefail

for mod in `git config --file .gitmodules --get-regexp path | awk '{ print $2 }'`; do
    if [ ! -f $mod/.git ]; then
       git submodule update --init $mod
    fi
done

