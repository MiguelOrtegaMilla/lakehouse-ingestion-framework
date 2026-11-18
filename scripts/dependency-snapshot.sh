#!/usr/bin/env bash
# Writes the dependency snapshot that CI submits to GitHub's dependency graph (ADR-0013 §4).
#
# The graph is resolved by the Scala Center's sbt-github-dependency-submission plugin, the same one
# the scalacenter/sbt-dependency-submission action runs, and is added to the build only for this
# run, as the action does. Its per-project manifests are then merged into one
# (merge_dependency_manifests.py explains why).
#
# Usage: scripts/dependency-snapshot.sh <output.json>
# Environment:
#   SNAPSHOT_SHA         commit the snapshot describes (on a pull request: its head, not the merge)
#   SNAPSHOT_REF         ref the snapshot is submitted for
#   SNAPSHOT_CORRELATOR  identifies this snapshot's source: a new submission with the same
#                        correlator replaces the previous one instead of adding to it
set -euo pipefail

plugin_version='3.2.2'

out="${1:?usage: scripts/dependency-snapshot.sh <output.json>}"
: "${SNAPSHOT_SHA:?}" "${SNAPSHOT_REF:?}" "${SNAPSHOT_CORRELATOR:?}"
[[ "$SNAPSHOT_CORRELATOR" =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "invalid SNAPSHOT_CORRELATOR" >&2; exit 2; }
out="$(realpath -m "$out")"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

plugin_file='project/zz-dependency-submission.sbt'
outputs="$(mktemp)"
raw=''
cleanup() { rm -f "$plugin_file" "$outputs" ${raw:+"$raw"}; }
trap cleanup EXIT

printf 'addSbtPlugin("ch.epfl.scala" %% "sbt-github-dependency-submission" %% "%s")\n' "$plugin_version" >"$plugin_file"

# The plugin takes the commit and ref from GITHUB_SHA and GITHUB_REF, and reports the path of the
# snapshot it writes through GITHUB_OUTPUT. These are set for sbt only.
#
# The task resolves the build and writes a file, so it can be run again: it is, if Maven Central
# answers 429 (retry-on-429.sh).
GITHUB_SHA="$SNAPSHOT_SHA" GITHUB_REF="$SNAPSHOT_REF" GITHUB_WORKSPACE="$root" GITHUB_OUTPUT="$outputs" \
  scripts/retry-on-429.sh sbt -batch "githubGenerateSnapshot {\"ignoredModules\":[],\"ignoredConfigs\":[],\"onResolveFailure\":\"error\",\"correlator\":\"$SNAPSHOT_CORRELATOR\"}"

# The last one it reported: an attempt that was cut short may have reported one too.
raw="$(sed -n 's/^snapshot-json-path=//p' "$outputs" | tail -n 1)"
[[ -f "$raw" ]] || { echo "the plugin did not report a snapshot" >&2; exit 1; }

python3 -I scripts/merge_dependency_manifests.py "$raw" >"$out"
echo "Dependency snapshot written to $out"
