import unittest

from app.services.pivot_core import build_pivot_view
from app.services.pivot_streaming import StreamingPivotAggregator


class PivotStreamingTests(unittest.TestCase):
    def test_streaming_matches_single_chunk(self) -> None:
        records = [
            {"cls": "A", "year": 2024, "value": 10, "count": 1},
            {"cls": "A", "year": 2024, "value": 20, "count": 2},
            {"cls": "B", "year": 2024, "value": 5, "count": 3},
            {"cls": "B", "year": 2023, "value": 15, "count": 4},
        ]
        snapshot = {
            "pivot": {"rows": ["cls"], "columns": ["year"], "filters": []},
            "metrics": [
                {"key": "value__sum", "sourceKey": "value", "op": "sum"},
                {"key": "count__sum", "sourceKey": "count", "op": "sum"},
                {
                    "key": "value_avg_formula",
                    "type": "formula",
                    "expression": "value__sum / count__sum",
                },
            ],
        }

        aggregator = StreamingPivotAggregator(snapshot)
        aggregator.update(records)
        single_result = aggregator.finalize()

        chunked = StreamingPivotAggregator(snapshot)
        chunked.update(records[:2])
        chunked.update(records[2:])
        chunked_result = chunked.finalize()

        expected = build_pivot_view(records, snapshot)
        self.assertEqual(single_result, chunked_result)
        self.assertEqual(single_result, expected)


if __name__ == "__main__":
    unittest.main()
