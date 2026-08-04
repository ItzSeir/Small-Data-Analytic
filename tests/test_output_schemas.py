"""Part 2 output-schema contract tests; no Spark implementation is invoked."""

from pathlib import Path
import unittest


RULES = Path(__file__).parents[1] / "config" / "preprocessing_rules.yaml"


class OutputSchemaContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rules = RULES.read_text(encoding="utf-8")

    def test_processed_output_is_partitioned_by_pickup_period(self):
        self.assertIn("format: parquet", self.rules)
        self.assertIn("partition_columns: [pickup_year, pickup_month]", self.rules)

    def test_required_enrichment_contract_is_declared(self):
        for field in ("pickup_borough", "pickup_zone", "pickup_service_zone"):
            self.assertIn(field, self.rules)

    def test_monthly_quality_schema_contains_each_required_count(self):
        expected = (
            "raw_rows", "structurally_valid_rows", "missing_timestamp_rows",
            "invalid_time_order_rows", "out_of_month_rows", "missing_location_rows",
            "non_positive_distance_rows", "non_positive_fare_rows",
            "missing_passenger_rows", "zero_passenger_rows",
            "unmatched_pickup_zone_rows",
        )
        for field in expected:
            self.assertIn(f"- {field}", self.rules)


if __name__ == "__main__":
    unittest.main()
