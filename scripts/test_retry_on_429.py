"""Tests of retry-on-429.sh: python3 -m unittest discover -s scripts"""

import os
import subprocess
import tempfile
import textwrap
import unittest

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "retry-on-429.sh")

# What coursier and the sbt launcher print when Maven Central limits them: the JDK's own words.
JDK_429 = (
    "download error: Caught java.io.IOException (Server returned HTTP response code: 429 for "
    "URL: https://repo1.maven.org/maven2/org/scala-sbt/sbt/1.13.1/sbt-1.13.1.pom)"
)


class RetryOn429Test(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.count = os.path.join(self.work.name, "count")

    def command(self, failures, says, exit_code=1):
        """A command that fails `failures` times, saying `says`, and then succeeds."""
        path = os.path.join(self.work.name, "command.sh")
        with open(path, "w") as script:
            script.write(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    runs=$(($(cat "$COUNT" 2>/dev/null || echo 0) + 1))
                    echo "$runs" >"$COUNT"
                    echo "attempt $runs with $*"
                    if ((runs <= FAILURES)); then
                      echo "$SAYS" >&2
                      exit "$EXIT_CODE"
                    fi
                    echo "done"
                    """
                )
            )
        os.chmod(path, 0o755)
        self.environment = {
            "COUNT": self.count,
            "FAILURES": str(failures),
            "SAYS": says,
            "EXIT_CODE": str(exit_code),
        }
        return path

    def run_script(self, *args, waits="0 0 0", **environment):
        env = {"PATH": os.environ["PATH"], "RETRY_WAITS": waits}
        env.update(getattr(self, "environment", {}))
        env.update(environment)
        return subprocess.run(
            [SCRIPT, *args], env=env, capture_output=True, text=True, timeout=60
        )

    def runs(self):
        with open(self.count) as count:
            return int(count.read())

    def test_a_command_that_succeeds_runs_once(self):
        ran = self.run_script(self.command(0, ""), "update", "two words")

        self.assertEqual(ran.returncode, 0)
        self.assertEqual(self.runs(), 1)
        # Its arguments reach it as they were given, and its output is passed on.
        self.assertEqual(ran.stdout, "attempt 1 with update two words\ndone\n")
        self.assertEqual(ran.stderr, "")

    def test_a_429_is_waited_out_and_the_command_run_again(self):
        ran = self.run_script(self.command(2, JDK_429))

        self.assertEqual(ran.returncode, 0)
        self.assertEqual(self.runs(), 3)
        self.assertIn("attempt 3", ran.stdout)
        self.assertEqual(
            ran.stderr.splitlines(),
            [
                "retry-on-429: the repository answered 429 (attempt 1 of 4); waiting 0 s before the next",
                "retry-on-429: the repository answered 429 (attempt 2 of 4); waiting 0 s before the next",
            ],
        )

    def test_a_429_that_does_not_pass_fails_with_the_code_of_the_command(self):
        ran = self.run_script(self.command(99, JDK_429, exit_code=7), waits="0 0")

        self.assertEqual(ran.returncode, 7)
        # The first attempt, and one for each wait.
        self.assertEqual(self.runs(), 3)
        self.assertIn("still answers 429 after 3 attempts; giving up", ran.stderr)

    def test_any_other_failure_is_not_tried_again(self):
        ran = self.run_script(
            self.command(99, "not found: https://repo1.maven.org/maven2/x/y/1.0/y-1.0.pom", exit_code=5)
        )

        self.assertEqual(ran.returncode, 5)
        self.assertEqual(self.runs(), 1)
        self.assertEqual(ran.stderr, "")

    def test_a_429_that_is_not_a_status_is_not_tried_again(self):
        for says in ("resolved in 429 ms", "found version 1.4.29", "[error] 429 tests failed"):
            with self.subTest(says=says):
                if os.path.exists(self.count):
                    os.remove(self.count)
                ran = self.run_script(self.command(99, says))
                self.assertEqual(ran.returncode, 1)
                self.assertEqual(self.runs(), 1)

    def test_the_ways_a_429_is_said(self):
        for says in (
            JDK_429,
            "HTTP/1.1 429 Too Many Requests",
            "unexpected status code: 429",
            "error: HTTP 429",
            "too many requests",
        ):
            with self.subTest(says=says):
                if os.path.exists(self.count):
                    os.remove(self.count)
                ran = self.run_script(self.command(1, says))
                self.assertEqual(ran.returncode, 0)
                self.assertEqual(self.runs(), 2)

    def test_the_waits_are_how_long_and_how_many(self):
        ran = self.run_script(self.command(99, JDK_429), waits="1")

        self.assertEqual(self.runs(), 2)
        self.assertIn("(attempt 1 of 2); waiting 1 s", ran.stderr)

        os.remove(self.count)
        none = self.run_script(self.command(99, JDK_429), waits="")
        self.assertEqual(none.returncode, 1)
        self.assertEqual(self.runs(), 1)

    def test_in_a_workflow_a_wait_is_a_warning_of_the_run(self):
        ran = self.run_script(self.command(1, JDK_429), GITHUB_ACTIONS="true")

        self.assertEqual(ran.returncode, 0)
        self.assertIn("::warning title=Rate limited by the repository::", ran.stdout)

    def test_what_is_not_a_use_of_it_is_refused(self):
        self.assertEqual(self.run_script().returncode, 2)
        bad = self.run_script(self.command(0, ""), waits="30s")
        self.assertEqual(bad.returncode, 2)
        self.assertIn("RETRY_WAITS must be whole seconds", bad.stderr)
        self.assertFalse(os.path.exists(self.count))


if __name__ == "__main__":
    unittest.main()
