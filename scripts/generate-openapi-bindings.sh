#!/usr/bin/env bash

set -e

SCRIPTDIR=$(dirname "$0")
TARGET="$SCRIPTDIR/../openapi"
VERSION_FILE="$SCRIPTDIR/../openapi/version.txt"
RUST_FMT="$SCRIPTDIR/../.rustfmt.toml"
CARGO_TOML="$TARGET/Cargo.toml"
CARGO_LOCK="$TARGET/../Cargo.lock"
SPEC="$SCRIPTDIR/../control-plane/rest/openapi-specs/v0_api_spec.yaml"

# Regenerate the bindings only if the rest src changed
check_spec="no"
# Use the Cargo.toml from the openapi-generator
default_toml="no"
# skip git diff at the end
skip_git_diff="no"

while [ "$#" -gt 0 ]; do
  case "$1" in
      --changes)
          check_spec="yes"
          shift
          ;;
      --default-toml)
          default_toml="yes"
          shift
          ;;
      --if-rev-changed)
          if [[ -f "$VERSION_FILE" ]]; then
            version=$(cat "$VERSION_FILE")
            bin_version=$(which openapi-generator-cli)
            [[ "$version" = "$bin_version" ]] && exit 0
          fi
          skip_git_diff="yes"
          shift
          ;;
  esac
done

if [[ $check_spec = "yes" ]]; then
    git diff --cached --exit-code "$SPEC" 1>/dev/null && exit 0
fi

tmpd=$(mktemp -d /tmp/openapi-gen-XXXXXXX)

# Generate a new openapi crate
openapi-generator-cli generate -i "$SPEC" -g rust-mayastor -o "$tmpd" --additional-properties actixWeb4Beta="True"

if [[ $default_toml = "no" ]]; then
  cp "$CARGO_TOML" "$tmpd"
fi

# Format the files
# Note, must be formatted on the tmp directory as we've ignored the autogenerated code within the workspace
if [ -f "$RUST_FMT" ]; then
  cp "$RUST_FMT" "$tmpd"
  cp "$CARGO_LOCK" "$tmpd"
  ( cd "$tmpd" && cargo fmt --all )
  # Cargo.lock is no longer generated when running cargo fmt
  ( cd "$tmpd"; rm Cargo.lock || true; rm "$(basename "$RUST_FMT")" )
fi

# Cleanup the existing autogenerated code
git clean -f -X "$TARGET"

rm -rf "$tmpd"/api
mv "$tmpd"/* "$TARGET"/
rm -rf "$tmpd"

which openapi-generator-cli > "$VERSION_FILE"

# If the openapi bindings were modified then fail the check
if [[ "$skip_git_diff" = "no" ]]; then
  git diff --exit-code "$TARGET"
fi
