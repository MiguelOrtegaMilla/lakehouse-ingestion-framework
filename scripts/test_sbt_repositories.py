"""Tests of the repositories that sbt is given in CI: python3 -m unittest discover -s scripts"""

import os
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIRROR = "google-mirror: https://maven-central.storage-download.googleapis.com/maven2/"


def repositories(name):
    """The entries of a repositories file of the sbt launcher, in their order."""
    with open(os.path.join(ROOT, ".github", "sbt", name)) as file:
        lines = [line.strip() for line in file if line.strip() and not line.startswith("#")]
    assert lines[0] == "[repositories]", lines[0]
    return lines[1:]


class SbtRepositoriesTest(unittest.TestCase):
    def test_central_is_asked_first_and_its_mirror_after_it(self):
        self.assertEqual(repositories("repositories"), ["local", "maven-central", MIRROR])

    def test_after_a_429_the_mirror_is_asked_first_and_central_after_it(self):
        self.assertEqual(
            repositories("repositories-mirror-first"), ["local", MIRROR, "maven-central"]
        )

    def test_the_two_have_the_same_repositories_and_differ_in_their_order_alone(self):
        first, again = repositories("repositories"), repositories("repositories-mirror-first")
        self.assertEqual(sorted(first), sorted(again))
        self.assertNotEqual(first, again)

    def test_everything_is_downloaded_over_tls(self):
        for name in ("repositories", "repositories-mirror-first"):
            for entry in repositories(name):
                self.assertNotIn("http://", entry)
                self.assertNotIn("allowInsecureProtocol", entry)

    def test_the_action_gives_sbt_both_files(self):
        with open(os.path.join(ROOT, ".github", "actions", "setup-build", "action.yml")) as file:
            action = file.read()
        self.assertIn(
            'echo "SBT_OPTS=-Dsbt.override.build.repos=true '
            '-Dsbt.repository.config=$repositories/repositories"',
            action,
        )
        self.assertIn(
            'echo "RETRY_SBT_OPTS=-Dsbt.override.build.repos=true '
            '-Dsbt.repository.config=$repositories/repositories-mirror-first"',
            action,
        )


if __name__ == "__main__":
    unittest.main()
