#!/usr/bin/env bash
set -euo pipefail

tag="${1:-}"
range="${2:-}"

if [ -z "${tag}" ]; then
  echo "usage: scripts/release-notes.sh v0.1.0 [git-range]" >&2
  exit 2
fi

if [ -z "${range}" ]; then
  previous="$(git describe --tags --abbrev=0 2>/dev/null || true)"
  if [ -n "${previous}" ]; then
    range="${previous}..HEAD"
  else
    range="HEAD"
  fi
fi

version="${tag#v}"

section() {
  local title="$1"
  local pattern="$2"
  local lines
  lines="$(git log --format='%s' "${range}" | grep -Ei "${pattern}" || true)"
  if [ -z "${lines}" ]; then
    return
  fi
  printf '\n### %s\n\n' "${title}"
  printf '%s\n' "${lines}" | sed -E 's/^[a-z]+(\([^)]+\))?!?:[[:space:]]*/- /I'
}

cat <<EOF
## notcrawl v${version}

local-first Notion crawling into SQLite, normalized Markdown, and git-backed snapshots.
EOF

section "Features" '^(feat|feature|enhancement)(\([^)]+\))?!?:'
section "Fixes" '^(fix|bug)(\([^)]+\))?!?:'
section "Maintenance" '^(chore|ci|build|docs|refactor|test|perf|dep|deps|dependencies)(\([^)]+\))?!?:'
