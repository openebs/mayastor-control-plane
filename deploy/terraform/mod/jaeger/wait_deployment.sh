#!/usr/bin/env bash

tries=20
while [[ $tries -gt 0 ]]; do
    if [[ $(kubectl -n mayastor get deployments mayastor-jaeger) ]]; then
        kubectl -n mayastor wait --timeout=30s --for=condition=Available deployment/mayastor-jaeger
        exit 0
    fi
    ((tries--))
    sleep 1
done
