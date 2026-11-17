#!/usr/bin/env bash
#
# Checks that the version in pyproject.toml differs from (and is greater than)
# the version on the remote master branch.

set -euo pipefail

FILE="pyproject.toml"
REMOTE="origin"
BRANCH="master"

# Extract `version = "x.y.z"` from the [tool.poetry] section only,
# so a "version" key in other sections (e.g. dependencies) is never picked up.
get_version() {
  awk '
    /^\[/ { in_poetry = ($0 == "[tool.poetry]") }
    in_poetry && /^[[:space:]]*version[[:space:]]*=/ {
      match($0, /"[^"]*"|'"'"'[^'"'"']*'"'"'/)
      print substr($0, RSTART + 1, RLENGTH - 2)
      exit
    }
  '
}

if [[ ! -f "$FILE" ]]; then
  echo "ERROR: $FILE not found" >&2
  exit 2
fi

# Path of the file relative to the repo root (needed for `git show`)
REPO_ROOT="$(git -C "$(dirname "$FILE")" rev-parse --show-toplevel)"
ABS_FILE="$(cd "$(dirname "$FILE")" && pwd)/$(basename "$FILE")"
REL_FILE="${ABS_FILE#"$REPO_ROOT"/}"

# Local version
NEW_VERSION="$(get_version < "$FILE")"
if [[ -z "$NEW_VERSION" ]]; then
  echo "ERROR: could not find a version in [tool.poetry] of $FILE" >&2
  exit 2
fi

# Make sure the version in the working tree is the one that is staged,
# i.e. there is no unstaged change to the version line.
if ! INDEX_CONTENT="$(git -C "$REPO_ROOT" show ":$REL_FILE" 2>/dev/null)"; then
  echo "ERROR: $REL_FILE is not tracked/staged in git" >&2
  exit 2
fi
INDEX_VERSION="$(get_version <<< "$INDEX_CONTENT")"
if [[ "$INDEX_VERSION" != "$NEW_VERSION" ]]; then
  echo "ERROR: unstaged change to the version in $FILE" >&2
  echo "       staged: $INDEX_VERSION, working tree: $NEW_VERSION" >&2
  echo "       run: git add $FILE" >&2
  exit 1
fi

# Remote version: fetch the branch from GitHub (does not touch the local
# master branch or working tree; the result lands in FETCH_HEAD).
git -C "$REPO_ROOT" fetch --quiet --depth=1 "$REMOTE" "$BRANCH"
MASTER_VERSION="$(git -C "$REPO_ROOT" show "FETCH_HEAD:$REL_FILE" | get_version)"
if [[ -z "$MASTER_VERSION" ]]; then
  echo "ERROR: could not find a version in $REMOTE/$BRANCH:$REL_FILE" >&2
  exit 2
fi

# 1. Must be different
if [[ "$NEW_VERSION" == "$MASTER_VERSION" ]]; then
  echo "Version on $REMOTE/$BRANCH: $MASTER_VERSION"
  echo "Version in your $FILE: $NEW_VERSION"
  echo "ERROR: version was not bumped" >&2
  exit 1
fi

# 2. Must be greater (version-aware sort; the highest of the two sorts last)
HIGHEST="$(printf '%s\n%s\n' "$MASTER_VERSION" "$NEW_VERSION" | sort -V | tail -n1)"
if [[ "$HIGHEST" != "$NEW_VERSION" ]]; then
  echo "Version on $REMOTE/$BRANCH: $MASTER_VERSION"
  echo "Version in your $FILE: $NEW_VERSION"
  echo "ERROR: version went down" >&2
  exit 1
fi
