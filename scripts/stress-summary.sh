#!/usr/bin/env bash
# Prints a Markdown table of the stress reports in target/stress: one row for each run, with
# what its verifier found. It is the job summary of the stress workflow; run by hand, it shows
# the same of the local runs. With a file as argument, only the reports newer than that file.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# The value of one row of a report's result table.
field() {
  { grep -m1 -F "| $2 |" "$1" || true; } | awk -F'|' '{ gsub(/^ +| +$/, "", $3); print $3 }'
}

echo "## LogStore stress runs"
echo
echo "Runs named \`red\` are on Delta's default LogStore, where a lost commit is the expected result. Runs named \`green\` are on \`PostgresLogStore\`, and must lose none."
echo
echo "| Run | Acknowledged | Lost | Verifier |"
echo "|---|---|---|---|"

found=0
while IFS= read -r report; do
  found=1
  run="$(basename "$(dirname "$report")")"
  verdict="$({ grep -m1 -E '^\*\*(No anomalies|[0-9]+ anomalies)\.\*\*$' "$report" || true; } | tr -d '*')"
  acknowledged="$(field "$report" 'Commits acknowledged by writers')"
  lost="$(field "$report" 'Lost commits (acknowledged, not in the log)')"
  echo "| \`$run\` | ${acknowledged:-?} | ${lost:-?} | ${verdict:-?} |"
done < <(find target/stress -mindepth 2 -maxdepth 2 -name report.md ${1:+-newer "$1"} 2>/dev/null | sort)

if ((found == 0)); then
  echo "| (no report) | | | |"
fi
