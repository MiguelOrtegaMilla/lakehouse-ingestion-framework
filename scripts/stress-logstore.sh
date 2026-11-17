#!/usr/bin/env bash
# Runs the LogStore stress spikes (ADR-0004 §7.3, `sbt stress`) and lists the reports this
# invocation wrote. Each run keeps its table, journals, logs and probes in target/stress/<run>/,
# next to its report.md. Needs about 3 GB of free memory: the test JVM plus three writer JVMs.
# The runs on PostgresLogStore (spike GREEN) start PostgreSQL with Testcontainers: they need Docker.
# The whole takes about 80 minutes, most of it in the ten natural runs of the GREEN spike;
# STRESS_NATURAL_RUNS sets another number of them, and 0 leaves them out (about 25 minutes).
# Extra arguments go to sbt before the alias, e.g. -J-Xmx2g.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# Reports newer than this marker were written by this invocation.
marker="$(mktemp)"
trap 'rm -f "$marker"' EXIT

status=0
sbt -batch "$@" stress || status=$?

echo
echo "Reports written by this run:"
find target/stress -name report.md -newer "$marker" 2>/dev/null | sort | sed 's/^/  /'
exit "$status"
