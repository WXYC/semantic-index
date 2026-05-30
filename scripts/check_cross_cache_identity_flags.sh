#!/usr/bin/env bash
# Asserts that the semantic-index-owned cross-cache-identity feature flags
# listed in docs/deployment.md match the §4.2 locked inventory.
#
# Tier 1 (this PR, BS#667): doc-vs-expected-list.
# Tier 2 (E1 hook-loader PR for SI consumers): adds a check that flag names
# referenced in code match the doc.
#
# Plan reference: WXYC/wiki plans/library-hook-canonicalization-plan.md §4.2.
# Canonical: WXYC/Backend-Service/CLAUDE.md "Cross-cache-identity feature flags (canonical inventory)".

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOC_PATH="$REPO_ROOT/docs/deployment.md"

if [[ ! -f "$DOC_PATH" ]]; then
  echo "FAIL: $DOC_PATH not found." >&2
  exit 1
fi

# Locked SI-owned flag names per §4.2.
expected=(
  "SI_USE_NEW_HOOK_DISCOGS"
  "SI_USE_NEW_HOOK_MUSICBRAINZ"
  "SI_USE_NEW_HOOK_WIKIDATA"
)

# Extract SI_USE_NEW_HOOK_* names from docs/deployment.md (any occurrence; the
# doc has only one canonical mention of each). Backtick-quoted in the table.
documented=$(
  grep -oE '`(SI_USE_NEW_HOOK_[A-Z_]+)`' "$DOC_PATH" | tr -d '`' | sort -u
)

if [[ -z "$documented" ]]; then
  echo "FAIL: no SI_USE_NEW_HOOK_* flags found in docs/deployment.md." >&2
  exit 1
fi

expected_sorted=$(printf '%s\n' "${expected[@]}" | sort -u)

missing=$(comm -23 <(printf '%s\n' "$expected_sorted") <(printf '%s\n' "$documented"))
extra=$(comm -13 <(printf '%s\n' "$expected_sorted") <(printf '%s\n' "$documented"))

failed=0
if [[ -n "$missing" ]]; then
  echo "FAIL: §4.2 SI-owned flags missing from docs/deployment.md:" >&2
  echo "$missing" | sed 's/^/  - /' >&2
  failed=1
fi
if [[ -n "$extra" ]]; then
  echo "FAIL: docs/deployment.md lists SI_USE_NEW_HOOK_* flags not in §4.2:" >&2
  echo "$extra" | sed 's/^/  - /' >&2
  echo "  If a new SI cross-cache-identity flag is being introduced, update §4.2 first." >&2
  failed=1
fi

if [[ "$failed" -ne 0 ]]; then
  echo "" >&2
  echo "Canonical inventory: WXYC/Backend-Service/CLAUDE.md 'Cross-cache-identity feature flags (canonical inventory)'." >&2
  exit 1
fi

count=$(printf '%s\n' "$expected_sorted" | wc -l | tr -d ' ')
echo "PASS: $count semantic-index-owned cross-cache-identity flag(s) consistent with §4.2."
