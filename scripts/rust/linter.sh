#!/usr/bin/env bash

cargo fmt --version
cargo fmt --all

cargo clippy --version
cargo clippy --all --all-targets $1 -- -D warnings
