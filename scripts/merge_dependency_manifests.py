#!/usr/bin/env python3
"""Merges the per-project manifests of an sbt dependency snapshot into one (ADR-0013 §4).

sbt-github-dependency-submission emits one manifest per sbt project, and every one of them points
at the same file, build.sbt. A library used by several projects is then listed several times under
that file, each time with the scope it has in that project: runtime in a project that ships it,
development in a project that only tests with it. GitHub does not keep those entries apart, and
which scope it records is not stable between two submissions of the same graph. Dependency review
then reported each such library as removed with one scope and added with the other, and blocked
pull requests on known vulnerabilities in dependencies they did not touch.

One manifest with one entry per package removes the ambiguity:
  - scope:        runtime if any project uses the package at runtime, development otherwise;
  - relationship: direct if any project declares the package, indirect otherwise;
  - dependencies: the union over the projects.

Usage: merge_dependency_manifests.py <snapshot.json>   (the merged snapshot goes to stdout)
"""

import json
import sys


class MergeError(Exception):
    pass


def merge(snapshot):
    manifests = snapshot["manifests"].values()
    locations = {m["file"]["source_location"] for m in manifests}
    if len(locations) != 1:
        raise MergeError(f"expected one build file, found {sorted(locations)}")
    location = locations.pop()

    nodes_by_key = {}
    for manifest in manifests:
        for key, node in manifest["resolved"].items():
            nodes_by_key.setdefault(key, []).append(node)

    resolved = {}
    for key in sorted(nodes_by_key):
        nodes = nodes_by_key[key]
        package_urls = {n["package_url"] for n in nodes}
        if len(package_urls) != 1:
            raise MergeError(f"{key} has several package URLs: {sorted(package_urls)}")
        resolved[key] = {
            "package_url": package_urls.pop(),
            "relationship": "direct" if any(n.get("relationship") == "direct" for n in nodes) else "indirect",
            "scope": "runtime" if any(n.get("scope") == "runtime" for n in nodes) else "development",
            "dependencies": sorted({d for n in nodes for d in n.get("dependencies", [])}),
        }

    return {
        **snapshot,
        "manifests": {location: {"name": location, "file": {"source_location": location}, "resolved": resolved}},
    }


def main(argv):
    if len(argv) != 2:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as f:
        snapshot = json.load(f)
    try:
        merged = merge(snapshot)
    except MergeError as e:
        print(f"merge_dependency_manifests: {e}", file=sys.stderr)
        return 1
    json.dump(merged, sys.stdout, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
