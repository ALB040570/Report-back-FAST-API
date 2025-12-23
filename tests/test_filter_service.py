import unittest

from app.services.filter_service import apply_filters


class FilterServiceTests(unittest.TestCase):
    def test_values_filter_string(self) -> None:
        records = [
            {"name": "A"},
            {"name": "B"},
        ]
        snapshot = {
            "fieldMeta": {},
        }
        filters = {
            "globalFilters": {"name": {"values": ["A"]}},
            "containerFilters": {},
        }
        filtered, debug = apply_filters(records, snapshot, filters)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["name"], "A")
        self.assertIn("name", debug.get("appliedKeys", {}).get("values", []))

    def test_range_filter_iso_date(self) -> None:
        records = [
            {"createdAt": "2025-12-09"},
            {"createdAt": "2025-11-30"},
        ]
        snapshot = {
            "fieldMeta": {"createdAt": {"type": "date"}},
            "filterRanges": {
                "createdAt": {"start": "2025-12-01", "end": "2025-12-31"},
            },
        }
        filtered, _ = apply_filters(records, snapshot, None)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["createdAt"], "2025-12-09")

    def test_range_filter_ms(self) -> None:
        records = [
            {"createdAt": 1_750_000_000_000},
            {"createdAt": 1_650_000_000_000},
        ]
        snapshot = {
            "fieldMeta": {"createdAt": {"type": "date"}},
            "filterRanges": {
                "createdAt": {
                    "start": 1_700_000_000_000,
                    "end": 1_800_000_000_000,
                },
            },
        }
        filtered, _ = apply_filters(records, snapshot, None)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["createdAt"], 1_750_000_000_000)

    def test_values_filter_flat_key(self) -> None:
        records = [
            {"OBJ.nameSection": "A"},
            {"OBJ.nameSection": "B"},
        ]
        snapshot = {"fieldMeta": {}}
        filters = {
            "globalFilters": {},
            "containerFilters": {"OBJ.nameSection": {"values": ["A"]}},
        }
        filtered, _ = apply_filters(records, snapshot, filters)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["OBJ.nameSection"], "A")

    def test_values_filter_date_part_key(self) -> None:
        records = [
            {"createdAt": "2025-06-01"},
            {"createdAt": "2024-01-01"},
        ]
        snapshot = {"fieldMeta": {}}
        filters = {
            "globalFilters": {"createdAt__date_part__year": {"values": ["2025"]}},
            "containerFilters": {},
        }
        filtered, _ = apply_filters(records, snapshot, filters)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["createdAt"], "2025-06-01")


if __name__ == "__main__":
    unittest.main()
