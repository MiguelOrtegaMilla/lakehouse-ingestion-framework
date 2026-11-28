#!/usr/bin/env python3
"""The time that each suite of tests took, from the JUnit reports that sbt writes.

CI runs the tests of integration-tests in parts of their own, each a group of suites that takes
about as long as the others (.github/workflows/ci.yml). This script says how long each suite
took, so that the groups can be made again when they drift:

  - as a table in the summary of the job, when GITHUB_STEP_SUMMARY is set;
  - as one notice on the job, which the API of the checks gives to whoever reads it, with no
    token for a public repository, where the log of a job needs one;
  - on its standard output, a line for each suite.

It reads every TEST-*.xml under the directories that it is given. A suite with no report did not
run, and a directory with none is not an error: the job that called it may have failed before
its tests.

Usage: scripts/suite_times.py <directory>...
"""

import glob
import os
import sys
import xml.etree.ElementTree as ET

TITLE = "Time of each suite"


def suites(directories):
    """(seconds, suite, tests) of each suite reported under the directories, the longest first."""
    found = []
    for directory in directories:
        for path in sorted(glob.glob(os.path.join(directory, "TEST-*.xml"))):
            root = ET.parse(path).getroot()
            for suite in [root] if root.tag == "testsuite" else root.iter("testsuite"):
                found.append(
                    (
                        float(suite.get("time", "0")),
                        suite.get("name") or os.path.basename(path),
                        int(suite.get("tests", "0")),
                    )
                )
    return sorted(found, key=lambda suite: (-suite[0], suite[1]))


def lines(found):
    """A line for each suite, and one for them all."""
    total = sum(seconds for seconds, _, _ in found)
    return [f"{seconds:8.1f} s  {name} ({tests})" for seconds, name, tests in found] + [
        f"{total:8.1f} s  {len(found)} suites"
    ]


def table(found):
    """The summary of the job: a table of Markdown."""
    rows = [f"| {name} | {tests} | {seconds:.1f} |" for seconds, name, tests in found]
    total = sum(seconds for seconds, _, _ in found)
    return "\n".join(
        [f"### {TITLE}", "", "| Suite | Tests | Seconds |", "|---|---:|---:|"]
        + rows
        + [f"| {len(found)} suites | | {total:.1f} |", ""]
    )


def escaped(text):
    """Text as the message of a command of a workflow takes it: one line."""
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def main(directories):
    found = suites(directories)
    if not found:
        print("no report of a suite under " + ", ".join(directories))
        return 0
    said = lines(found)
    print("\n".join(said))
    print(f"::notice title={TITLE}::" + escaped("\n".join(said)))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write(table(found) + "\n")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1:]))
