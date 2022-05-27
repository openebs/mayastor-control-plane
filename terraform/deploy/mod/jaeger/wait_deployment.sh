#!/usr/bin/env bash

tries=20
while [[ $tries -gt 0 ]]; do
    if [[ $(kubectl -n $1 get deployments jaeger) ]]; then
        kubectl -n $1 wait --timeout=30s --for=condition=Available deployment/jaeger
        exit 0
    fi
    ((tries--))
    sleep 1
done
