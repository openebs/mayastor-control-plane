#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]:-"$0"}")")"
ROOT_DIR="$SCRIPT_DIR/../.."
TEST_ROOT_DIR=${TEST_ROOT_DIR:-"$ROOT_DIR"}

# Imports
source "$ROOT_DIR"/scripts/utils/log.sh

set -e

fmt() {
  local -r paths_glob=$1

  bash -c "
  black --quiet $paths_glob &&
  isort --profile=black --quiet $paths_glob &&
  autoflake --quiet -r -i --remove-unused-variables --remove-all-unused-imports --expand-star-imports $paths_glob"
}

fmt_diff() {
  local -r paths_glob=($(bash -c "echo $1"))

  for path in "${paths_glob[@]}"; do
    find "$path" -type f -name "*.py" -exec bash -c '
      OUTPUT=$(diff -u --color=always <(cat {}) <(black -c "$(cat {})" |
        isort --profile=black -d - |
        autoflake --remove-unused-variables --remove-all-unused-imports --expand-star-imports -s -)
      );
      if [ -n "$OUTPUT" ]; then
        echo -e "Diff for file {}\n===================================================\n$OUTPUT\n" | cat
      fi
      ' \;
  done
}

fmt_check() {
  local -r paths_glob=$1

  local error=
  bash -c "
    black --quiet $paths_glob --check &&
    isort --profile=black --quiet $paths_glob --check 2> /dev/null &&
    autoflake --quiet -r --remove-unused-variables --remove-all-unused-imports --expand-star-imports $paths_glob --check > /dev/null
    " || error=$?

  if [ -n "$error" ]; then
    exit $error
  fi
  exit 0
}

CHECK=
DIFF=
PATHS_GLOB=
DEFAULT_PATHS_GLOB=$(realpath "$TEST_ROOT_DIR"/tests/bdd)/{common,features}

# Print usage options for this script.
print_help() {
  cat <<EOF
Usage: $(basename "${0}") [OPTIONS]

Options:
  -h, --help              Display this text.
  --check                 Don't write the files back, just return the status.
                          Return code 0 means nothing would change. Return code
                          1 means some files would be changed.
  --diff                  Don't write the files back, just output a diff to
                          indicate what changes would've been made.
  --paths-glob <glob>     Input a paths glob of directories and/or files which
                          would be parsed recursively. (default: "$DEFAULT_PATHS_GLOB")

Examples:
  $(basename "${0}") --check
EOF
}

# Parse args.
while test $# -gt 0; do
  arg="$1"
  case "$arg" in
  --check)
    CHECK=1
    ;;
  --diff)
    DIFF=1
    ;;
  --paths-glob*)
    if [ "$arg" = "--paths-glob" ]; then
      test $# -lt 2 && log_fatal "Missing value for the optional argument '$arg'."
      PATHS_GLOB="$2"
      shift
    else
      PATHS_GLOB="${arg#*=}"
    fi
    ;;
  -h* | --help*)
    print_help
    exit 0
    ;;
  *)
    print_help
    log_fatal "unexpected argument '$arg'" 1
    ;;
  esac
  shift
done

if [ -z "$PATHS_GLOB" ]; then
  PATHS_GLOB=$DEFAULT_PATHS_GLOB
fi

if [ -n "$CHECK" ]; then
  fmt_check "$PATHS_GLOB"
fi

if [ -n "$DIFF" ]; then
  fmt_diff "$PATHS_GLOB"
fi

if [[ -z "$CHECK" ]] && [[ -z "$DIFF" ]]; then
  fmt "$PATHS_GLOB"
fi
