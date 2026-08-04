"""Part 2 contract tests; they deliberately do not require Spark or AWS."""

from pathlib import Path
import unittest


RULES = Path(__file__).parents[1] / "config" / "preprocessing_rules.yaml"


class PreprocessingBoundaryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rules = RULES.read_text(encoding="utf-8")

    def test_duration_boundaries_are_explicit(self):
        self.assertIn("exclusive_minimum: 0", self.rules)
        self.assertIn("inclusive_maximum: 240", self.rules)

    def test_distance_boundaries_preserve_the_approved_labels(self):
        for label in (
            '"Very short: >0 to <2"', '"Short: 2 to <5"',
            '"Medium: 5 to <10"', '"Long: 10 to <20"', '"Very long: 20+"',
        ):
            self.assertIn(label, self.rules)

    def test_missing_and_zero_passengers_remain_categories(self):
        self.assertIn("passenger_groups: [Missing, \"0\"", self.rules)
        self.assertIn("missing: Missing", self.rules)
        self.assertIn('zero: "0"', self.rules)

    def test_missing_optional_fees_and_unmatched_pickups_are_preserved(self):
        self.assertEqual(self.rules.count("absent_value: null"), 2)
        self.assertIn("preserve_unmatched_pickup_rows: true", self.rules)


if __name__ == "__main__":
    unittest.main()
