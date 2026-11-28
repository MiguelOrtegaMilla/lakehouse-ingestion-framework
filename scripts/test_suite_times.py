"""Tests of suite_times.py: python3 -m unittest discover -s scripts"""

import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

import suite_times


def report(directory, name, seconds, tests=1, root="testsuite"):
    """A report of one suite, as sbt writes it, or inside a list of suites."""
    suite = f'<testsuite name="{name}" tests="{tests}" time="{seconds}"><testcase name="t"/></testsuite>'
    text = suite if root == "testsuite" else f"<testsuites>{suite}</testsuites>"
    with open(os.path.join(directory, f"TEST-{name}.xml"), "w", encoding="utf-8") as out:
        out.write("<?xml version='1.0' encoding='UTF-8'?>\n" + text)


class SuiteTimesTest(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.one = os.path.join(self.work.name, "one")
        self.two = os.path.join(self.work.name, "two")
        os.makedirs(self.one)
        os.makedirs(self.two)

    def run_main(self, *directories, summary=None):
        env = {"GITHUB_STEP_SUMMARY": summary} if summary else {}
        out = io.StringIO()
        with mock.patch.dict(os.environ, env, clear=True), redirect_stdout(out):
            code = suite_times.main(list(directories))
        return code, out.getvalue()

    def test_the_suites_of_every_directory_the_longest_first(self):
        report(self.one, "a.FastSpec", 1.5, tests=3)
        report(self.two, "b.SlowSpec", 120.25, tests=40)
        report(self.two, "c.MiddleSpec", 30, root="testsuites")
        self.assertEqual(
            suite_times.suites([self.one, self.two]),
            [(120.25, "b.SlowSpec", 40), (30.0, "c.MiddleSpec", 1), (1.5, "a.FastSpec", 3)],
        )

    def test_a_line_for_each_suite_and_one_notice_with_them_all(self):
        report(self.one, "a.FastSpec", 1.5)
        report(self.one, "b.SlowSpec", 120.25)
        code, out = self.run_main(self.one)
        self.assertEqual(code, 0)
        printed = out.splitlines()
        self.assertEqual(printed[0], "   120.2 s  b.SlowSpec (1)")
        self.assertEqual(printed[2], "   121.8 s  2 suites")
        # One command of the workflow: one line, its ends of lines escaped, the total first.
        notice = printed[3]
        self.assertTrue(notice.startswith("::notice title=Time of each suite::2 suites, 121.8 s%0A"))
        self.assertIn("  120.2 s  SlowSpec%0A", notice)
        self.assertEqual(len(printed), 4)

    def test_a_notice_keeps_within_what_github_keeps_and_says_what_it_left_out(self):
        found = [(float(100 - i), f"lakehouse.ingestion.it.part.Suite{i:03d}Spec", 1) for i in range(400)]
        message = suite_times.notice(found)
        self.assertLessEqual(len(message), suite_times.NOTICE_LIMIT)
        lines = message.splitlines()
        self.assertEqual(lines[0], f"400 suites, {sum(s for s, _, _ in found):.1f} s")
        self.assertEqual(lines[1], "  100.0 s  Suite000Spec")
        left_out = 400 - (len(lines) - 2)
        self.assertTrue(lines[-1].startswith(f"and {left_out} more, "))
        self.assertGreater(left_out, 0)

    def test_a_notice_of_suites_that_fit_leaves_none_out(self):
        found = [(2.0, "a.BSpec", 1), (1.0, "c.DSpec", 1)]
        self.assertEqual(suite_times.notice(found), "2 suites, 3.0 s\n    2.0 s  BSpec\n    1.0 s  DSpec")

    def test_a_percent_in_a_name_is_escaped_in_the_notice(self):
        report(self.one, "a%Spec", 1)
        _, out = self.run_main(self.one)
        self.assertIn("a%25Spec", out.splitlines()[-1])

    def test_the_summary_of_the_job_has_a_table(self):
        report(self.one, "a.FastSpec", 1.5, tests=3)
        summary = os.path.join(self.work.name, "summary.md")
        self.run_main(self.one, summary=summary)
        with open(summary, encoding="utf-8") as written:
            text = written.read()
        self.assertIn("| Suite | Tests | Seconds |", text)
        self.assertIn("| a.FastSpec | 3 | 1.5 |", text)
        self.assertIn("| 1 suites | | 1.5 |", text)

    def test_no_report_is_said_and_is_no_failure(self):
        code, out = self.run_main(self.one, os.path.join(self.work.name, "not-there"))
        self.assertEqual(code, 0)
        self.assertIn("no report of a suite", out)
        self.assertNotIn("::notice", out)


if __name__ == "__main__":
    unittest.main()
