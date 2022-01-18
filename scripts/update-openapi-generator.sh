#!/usr/bin/env bash

set -eu -o pipefail

SCRIPTDIR=$(dirname "$0")
owner="openebs";
repo="openapi-generator";
branch="rust_mayastor";

github_rev() {
  curl -sSf "https://api.github.com/repos/$owner/$repo/branches/$branch" | \
    jq '.commit.sha' | \
    sed 's/"//g'
}

github_sha256() {
  nix-prefetch-url \
     --unpack \
     --type sha256 \
     "https://github.com/$owner/$repo/archive/$branch.tar.gz" 2>&1 | \
     tail -1
}

echo "=== ${owner}/${repo}@${branch} ==="

echo -n "Looking up latest revision ... "
rev=$(github_rev "${owner}" "${repo}" "${branch}");
echo "revision is \`$rev\`."

sha256=$(github_sha256 "${owner}" "${repo}" "$rev");
echo "sha256 is \`$sha256\`."

if [ "$sha256" == "" ]; then
  echo "sha256 is not valid!"
  exit 2
fi
source_file="$SCRIPTDIR/../nix/pkgs/openapi-generator/source.json"

echo "Previous Content of source file (``$source_file``):"
cat "$source_file"
echo "New Content of source file (``$source_file``) written."
cat <<REPO | tee "$source_file"
{
  "owner": "${owner}",
  "repo": "${repo}",
  "rev": "$rev",
  "sha256": "$sha256"
}
REPO
echo
