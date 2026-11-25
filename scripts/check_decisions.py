#!/usr/bin/env python3
"""Holds the table of decisions of the discovery to the repository.

The table (discovery, section 22) has one row for each decision that the code is to enforce:
where production enforces it, and which test shows it. A table like that goes stale without a
word: a class is renamed, a test is deleted, and the row still reads well. This script fails
when a row names something that is not there any more:

  - a name in backquotes in the column of production is found in a file of production: every
    part of it (``LeasedRun.commit`` is ``LeasedRun`` and ``commit``) as a word of one file;
  - a name in backquotes in the column of the tests is found the same way in a file of the
    tests, and a text in double quotes after it is in a file of the tests that the row names:
    the title of a test, or a part of it;
  - a name that is the path of a file of the repository, or the name of one, is that file;
  - a row with nothing in one of the two columns says so, as ``**None**``, with the phase that
    is to give it one: that is a finding, and is counted.

It reads what git tracks, so that a file that was not added does not make it pass. It does not
read what a test asserts: that a test shows what its row says is for whoever writes the row.
"""

import re
import subprocess
import sys
from pathlib import Path

DOCUMENT = "docs/architecture/00-architecture-discovery.md"
BEGIN = "<!-- decisions:begin -->"
END = "<!-- decisions:end -->"

PRODUCTION = re.compile(
    r"^(modules/[^/]+/src/main/|project/[^/]+$|build\.sbt$|\.github/|docker/|conf/|scripts/(?!test_))"
)
TESTS = re.compile(r"^(modules/[^/]+/src/test/|scripts/test_|\.github/workflows/)")

NONE = re.compile(r"^\*\*None\*\*")
PHASE = re.compile(r"\b(P\d+[ab]?|E\d+)\b")
NAME = re.compile(r"`([^`]+)`")
QUOTED = re.compile(r"\"([^\"]+)\"")


def rows(text):
    """The rows of the table between the two marks: (id, decision, source, production, tests)."""
    if BEGIN not in text or END not in text:
        raise ValueError(f"{DOCUMENT} has no table between {BEGIN} and {END}")
    table = text.split(BEGIN, 1)[1].split(END, 1)[0]
    found = []
    for line in table.splitlines():
        line = line.strip()
        if not line.startswith("|") or set(line) <= set("|- "):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells[0] in ("#", ""):
            continue
        if len(cells) != 5:
            raise ValueError(f"a row of the table has {len(cells)} cells, and not 5: {line[:80]}")
        found.append(tuple(cells))
    return found


def parts(name):
    """The words of a name: ``LeasedRun.commit`` is ``LeasedRun`` and ``commit``."""
    return [part for part in re.split(r"[^A-Za-z0-9_$]+", name) if part]


def has_word(content, word):
    return re.search(rf"(?<![A-Za-z0-9_]){re.escape(word)}(?![A-Za-z0-9_])", content) is not None


def is_file(name, tracked):
    """Whether a name is the path of a tracked file, or the name of one."""
    return name in tracked or any(path.endswith("/" + name) for path in tracked)


def files_with(name, files):
    """The files that have every word of the name."""
    words = parts(name)
    return [path for path, content in files.items() if all(has_word(content, w) for w in words)]


def check(table, production, tests, tracked):
    """What is wrong with the rows, and the rows that say that they lack a column."""
    problems, findings, seen = [], [], set()
    for row, _decision, _source, enforced, shown in table:
        if row in seen:
            problems.append(f"{row}: there are two rows of this id")
        seen.add(row)
        for column, cell, files in (("production", enforced, production), ("tests", shown, tests)):
            if NONE.match(cell):
                if PHASE.search(cell):
                    findings.append(f"{row}: nothing in {column}: {cell}")
                else:
                    problems.append(f"{row}: nothing in {column}, and no phase that is to give it")
                continue
            names = NAME.findall(cell)
            if not names:
                problems.append(f"{row}: the column of {column} names nothing, and does not say None")
            named = []
            for name in names:
                if is_file(name, tracked):
                    named.extend(path for path in files if path == name or path.endswith("/" + name))
                    continue
                having = files_with(name, files)
                if not having:
                    problems.append(f"{row}: `{name}` is in no file of {column}")
                named.extend(having)
            for text in QUOTED.findall(cell) if column == "tests" else []:
                if not any(text in files[path] for path in named):
                    problems.append(f'{row}: no test of the row has "{text}"')
    return problems, findings


def tracked_files(root):
    listed = subprocess.run(
        ["git", "ls-files"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.splitlines()
    return set(listed)


def read(root, paths):
    contents = {}
    for path in paths:
        try:
            contents[path] = (root / path).read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError):
            continue
    return contents


def main():
    root = Path(__file__).resolve().parent.parent
    tracked = tracked_files(root)
    production = read(root, [path for path in tracked if PRODUCTION.match(path)])
    tests = read(root, [path for path in tracked if TESTS.match(path)])
    table = rows((root / DOCUMENT).read_text(encoding="utf-8"))
    problems, findings = check(table, production, tests, tracked)
    for finding in findings:
        print(f"finding: {finding}")
    for problem in problems:
        print(f"error: {problem}", file=sys.stderr)
    if problems:
        print(f"{len(problems)} of the names of the table of decisions are not there.", file=sys.stderr)
        return 1
    print(f"{len(table)} decisions, each with what enforces it and what shows it, "
          f"but for {len(findings)} that say what they lack.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
