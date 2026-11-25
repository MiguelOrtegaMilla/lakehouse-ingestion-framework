"""Tests of check_decisions.py: what a row of the table of decisions is held to."""

import unittest

import check_decisions as decisions

PRODUCTION = {
    "modules/engine/src/main/scala/LeasedRun.scala": "final class LeasedRun { def commit(): Unit }",
    "modules/engine/src/main/resources/V1__control_schema.sql": "CREATE FUNCTION refuse_change()",
}
TESTS = {
    "modules/it/src/test/scala/BatchesSpec.scala": 'class BatchesSpec { test("a batch is committed once") }',
    ".github/workflows/ci.yml": "run: sbt checkBoundaries",
}
TRACKED = set(PRODUCTION) | set(TESTS)


def check(*rows):
    return decisions.check(list(rows), PRODUCTION, TESTS, TRACKED)


class RowsTest(unittest.TestCase):
    def test_the_rows_are_those_between_the_marks(self):
        text = "\n".join(
            [
                "| D-000 | outside | x | `Nothing` | `Nothing` |",
                decisions.BEGIN,
                "| # | Decision | From | Production | Tests |",
                "|---|---|---|---|---|",
                "| D-001 | one | ADR | `LeasedRun` | `BatchesSpec` |",
                decisions.END,
            ]
        )
        self.assertEqual(
            decisions.rows(text), [("D-001", "one", "ADR", "`LeasedRun`", "`BatchesSpec`")]
        )

    def test_a_document_without_its_marks_is_refused(self):
        with self.assertRaises(ValueError):
            decisions.rows("| D-001 | one | ADR | `LeasedRun` | `BatchesSpec` |")

    def test_a_row_with_another_number_of_cells_is_refused(self):
        with self.assertRaises(ValueError):
            decisions.rows(f"{decisions.BEGIN}\n| D-001 | one | `LeasedRun` |\n{decisions.END}")


class CheckTest(unittest.TestCase):
    def test_a_row_whose_names_are_there_has_no_problem(self):
        problems, findings = check(
            ("D-001", "one", "ADR", "`LeasedRun.commit`", '`BatchesSpec` "a batch is committed once"')
        )
        self.assertEqual((problems, findings), ([], []))

    def test_a_name_whose_words_are_in_no_one_file_is_said(self):
        problems, _ = check(("D-001", "one", "ADR", "`LeasedRun.abandon`", "`BatchesSpec`"))
        self.assertEqual(problems, ["D-001: `LeasedRun.abandon` is in no file of production"])

    def test_a_name_of_production_does_not_stand_for_a_test(self):
        problems, _ = check(("D-001", "one", "ADR", "`LeasedRun`", "`LeasedRun`"))
        self.assertEqual(problems, ["D-001: `LeasedRun` is in no file of tests"])

    def test_a_part_of_a_longer_word_is_not_the_word(self):
        problems, _ = check(("D-001", "one", "ADR", "`Leased`", "`BatchesSpec`"))
        self.assertEqual(problems, ["D-001: `Leased` is in no file of production"])

    def test_a_title_that_no_test_of_the_row_has_is_said(self):
        problems, _ = check(
            ("D-001", "one", "ADR", "`LeasedRun`", '`BatchesSpec` "a batch is committed twice"')
        )
        self.assertEqual(problems, ['D-001: no test of the row has "a batch is committed twice"'])

    def test_a_name_that_is_a_tracked_file_is_that_file(self):
        problems, _ = check(
            ("D-001", "one", "ADR", "`V1__control_schema.sql`", "`ci.yml` (`checkBoundaries`)")
        )
        self.assertEqual(problems, [])

    def test_a_column_that_names_nothing_is_said(self):
        problems, _ = check(("D-001", "one", "ADR", "the engine", "`BatchesSpec`"))
        self.assertEqual(
            problems, ["D-001: the column of production names nothing, and does not say None"]
        )

    def test_a_column_that_says_none_with_its_phase_is_a_finding(self):
        problems, findings = check(
            ("D-001", "one", "ADR", "**None** (P8): no command reaches it", "`BatchesSpec`")
        )
        self.assertEqual(problems, [])
        self.assertEqual(len(findings), 1)

    def test_a_column_that_says_none_with_no_phase_is_a_problem(self):
        problems, findings = check(("D-001", "one", "ADR", "`LeasedRun`", "**None**"))
        self.assertEqual(problems, ["D-001: nothing in tests, and no phase that is to give it"])
        self.assertEqual(findings, [])

    def test_an_id_is_of_one_row(self):
        row = ("D-001", "one", "ADR", "`LeasedRun`", "`BatchesSpec`")
        problems, _ = check(row, row)
        self.assertEqual(problems, ["D-001: there are two rows of this id"])


if __name__ == "__main__":
    unittest.main()
