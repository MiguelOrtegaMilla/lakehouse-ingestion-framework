"""Tests for merge_dependency_manifests.py. Run: python3 -m unittest discover -s scripts"""

import unittest

from merge_dependency_manifests import MergeError, merge

BCPROV = "org.bouncycastle:bcprov-jdk18on:1.80"
BCPROV_PURL = "pkg:maven/org.bouncycastle/bcprov-jdk18on@1.80"
JUNIT = "junit:junit:4.13.2"


def node(purl, scope, relationship="indirect", dependencies=()):
    return {
        "package_url": purl,
        "scope": scope,
        "relationship": relationship,
        "dependencies": list(dependencies),
        "metadata": {"config": "compile"},
    }


def manifest(name, resolved, location="build.sbt"):
    return {"name": name, "file": {"source_location": location}, "metadata": {}, "resolved": resolved}


def snapshot(*manifests):
    return {"version": 0, "sha": "abc", "ref": "refs/heads/main", "manifests": {m["name"]: m for m in manifests}}


class MergeTest(unittest.TestCase):
    # The case that broke dependency review: one library, runtime in the project that ships it and
    # development in the test-only project, both under build.sbt.
    def test_a_package_in_several_projects_becomes_one_entry_with_the_runtime_scope(self):
        merged = merge(
            snapshot(
                manifest("engine", {BCPROV: node(BCPROV_PURL, "runtime")}),
                manifest("integration-tests", {BCPROV: node(BCPROV_PURL, "development", "direct")}),
            )
        )
        self.assertEqual(list(merged["manifests"]), ["build.sbt"])
        self.assertEqual(
            merged["manifests"]["build.sbt"]["resolved"],
            {BCPROV: {"package_url": BCPROV_PURL, "scope": "runtime", "relationship": "direct", "dependencies": []}},
        )

    def test_a_package_used_only_in_development_stays_development(self):
        merged = merge(snapshot(manifest("a", {JUNIT: node("pkg:maven/junit/junit@4.13.2", "development")})))
        self.assertEqual(merged["manifests"]["build.sbt"]["resolved"][JUNIT]["scope"], "development")

    def test_dependencies_are_the_union_over_the_projects(self):
        merged = merge(
            snapshot(
                manifest("a", {BCPROV: node(BCPROV_PURL, "runtime", dependencies=["x:y:1"])}),
                manifest("b", {BCPROV: node(BCPROV_PURL, "runtime", dependencies=["x:y:1", "p:q:2"])}),
            )
        )
        self.assertEqual(merged["manifests"]["build.sbt"]["resolved"][BCPROV]["dependencies"], ["p:q:2", "x:y:1"])

    def test_the_snapshot_fields_are_kept(self):
        merged = merge(snapshot(manifest("a", {})))
        self.assertEqual((merged["sha"], merged["ref"]), ("abc", "refs/heads/main"))

    def test_the_result_does_not_depend_on_the_order_of_the_manifests(self):
        a = manifest("a", {BCPROV: node(BCPROV_PURL, "runtime")})
        b = manifest("b", {BCPROV: node(BCPROV_PURL, "development")})
        self.assertEqual(merge(snapshot(a, b)), merge(snapshot(b, a)))

    def test_one_module_with_two_package_urls_is_refused(self):
        with self.assertRaisesRegex(MergeError, "several package URLs"):
            merge(
                snapshot(
                    manifest("a", {BCPROV: node(BCPROV_PURL, "runtime")}),
                    manifest("b", {BCPROV: node(BCPROV_PURL + "?packaging=tests", "runtime")}),
                )
            )

    def test_manifests_from_different_build_files_are_refused(self):
        with self.assertRaisesRegex(MergeError, "one build file"):
            merge(snapshot(manifest("a", {}), manifest("b", {}, location="other/build.sbt")))


if __name__ == "__main__":
    unittest.main()
