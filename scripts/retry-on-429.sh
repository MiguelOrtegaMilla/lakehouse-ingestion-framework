#!/usr/bin/env bash
# Runs a command that downloads from a repository of libraries, and runs it again, after a wait,
# if it failed because the repository answered 429 (Too Many Requests).
#
# Maven Central limits how much one address may ask for, and the runners of CI share theirs. A
# 429 says nothing about the build: the same command a little later does what it was asked. Any
# other failure is the command's own, and is not tried again: a library that does not exist is
# as missing the fourth time as the first.
#
# It is for commands that only resolve and download, which can be run again without harm. Not
# for the tests: a test that fails must fail the job.
#
# Usage: scripts/retry-on-429.sh <command> [argument...]
# The command's output, both streams, goes to standard output; what this script says, to
# standard error. The exit code is that of the last attempt.
# Environment:
#   RETRY_WAITS  the seconds to wait before each further attempt, which is also how many
#                there are (default "30 60 120": four attempts, three and a half minutes)
set -uo pipefail

if (($# == 0)); then
  echo "usage: scripts/retry-on-429.sh <command> [argument...]" >&2
  exit 2
fi

read -r -a waits <<<"${RETRY_WAITS-30 60 120}"
for wait in "${waits[@]}"; do
  [[ "$wait" =~ ^[0-9]+$ ]] || { echo "retry-on-429: RETRY_WAITS must be whole seconds, not '$wait'" >&2; exit 2; }
done

# How a 429 is said by what downloads: the JDK ("Server returned HTTP response code: 429 for
# URL"), which is what coursier and the sbt launcher report, and the status line itself. A bare
# 429 is not enough: it is also a duration in milliseconds, and a part of a version.
too_many='response code:? ?429|status( code)?:? ?429|HTTP( error)?:? ?429|429 Too Many Requests|Too Many Requests'

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

attempt=1
while true; do
  "$@" 2>&1 | tee "$log"
  status="${PIPESTATUS[0]}"
  ((status == 0)) && exit 0

  if ! grep -qiE "$too_many" "$log"; then
    exit "$status"
  fi
  if ((attempt > ${#waits[@]})); then
    echo "retry-on-429: the repository still answers 429 after $attempt attempts; giving up" >&2
    exit "$status"
  fi

  wait="${waits[attempt - 1]}"
  message="the repository answered 429 (attempt $attempt of $((${#waits[@]} + 1))); waiting ${wait} s before the next"
  echo "retry-on-429: $message" >&2
  # In the summary of the run, so that a job that passed after waiting says that it waited.
  [[ "${GITHUB_ACTIONS:-}" == "true" ]] && echo "::warning title=Rate limited by the repository::$message"
  sleep "$wait"
  attempt=$((attempt + 1))
done
