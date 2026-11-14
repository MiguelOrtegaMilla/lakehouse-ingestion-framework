#!/usr/bin/env bash
# Fails if an image or a GitHub Action the build uses is not pinned (ADR-0013 §5, RNF-16):
#   - every `image:` in docker/ compose files:          name:tag@sha256:<digest>
#   - every image in Testcontainers' configuration:     name:tag@sha256:<digest>
#   - every `uses:` in .github/ workflows and actions:  owner/repo@<40-hex commit SHA> # <release>
# Testcontainers images in code come from these two places only: the postgres image is read from
# the compose file (BootstrappedPostgres) and Ryuk from testcontainers.properties.
# Usage: scripts/check-pins.sh [repository root]
set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$root"

digest='[a-z0-9._/-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}'
testcontainers_properties='modules/integration-tests/src/test/resources/testcontainers.properties'
failures=0

fail() {
  echo "UNPINNED $1"
  failures=$((failures + 1))
}

# Compose files.
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  value="$(sed -E 's/^[^:]+:[0-9]+:[[:space:]]*image:[[:space:]]*//; s/[[:space:]]+(#.*)?$//; s/^"(.*)"$/\1/' <<<"$line")"
  [[ "$value" =~ ^${digest}$ ]] || fail "$line"
done < <(grep -rnE --include='*.yml' --include='*.yaml' '^[[:space:]]*image:' docker/ || true)

# Testcontainers configuration: Ryuk must be configured, and every image pinned.
if ! grep -qE '^ryuk\.container\.image=' "$testcontainers_properties" 2>/dev/null; then
  fail "$testcontainers_properties: no ryuk.container.image (Testcontainers would pull Ryuk by tag)"
fi
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  value="${line#*=}"
  [[ "$value" =~ ^${digest}$ ]] || fail "$testcontainers_properties: $line"
done < <(grep -E '^[a-z.]+\.container\.image=' "$testcontainers_properties" 2>/dev/null || true)

# GitHub Actions: local actions (./) are part of this repository; everything else by commit SHA,
# with the release it corresponds to as a comment (Dependabot updates both).
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  value="$(sed -E 's/^[^:]+:[0-9]+:[[:space:]]*(-[[:space:]]*)?uses:[[:space:]]*//' <<<"$line")"
  case "$value" in
    ./*) ;;
    docker://*) [[ "$value" =~ ^docker://${digest}([[:space:]]|$) ]] || fail "$line" ;;
    *) [[ "$value" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}[[:space:]]+#[[:space:]]*v[0-9] ]] || fail "$line" ;;
  esac
done < <(grep -rnE --include='*.yml' --include='*.yaml' '^[[:space:]]*(-[[:space:]]*)?uses:' .github/ 2>/dev/null || true)

if ((failures > 0)); then
  echo "$failures unpinned reference(s). Pin images as name:tag@sha256:<digest> and actions as owner/repo@<sha> # <release>."
  exit 1
fi
echo "All images and actions are pinned."
