import unittest

from app.services.computed_fields import ComputedFieldsEngine


class ComputedFieldsTests(unittest.TestCase):
    def test_computed_field_expression(self) -> None:
        engine = ComputedFieldsEngine(
            [
                {
                    "id": "field-1",
                    "fieldKey": "is_equal",
                    "expression": "(date({{FactDateEnd}}) == date({{UpdatedAt}})) ? 1 : 0",
                    "resultType": "number",
                }
            ]
        )
        records = [
            {"FactDateEnd": "2024-01-01", "UpdatedAt": "2024-01-01T12:00:00Z"},
            {"FactDateEnd": "2024-01-02", "UpdatedAt": "2024-01-01T12:00:00Z"},
        ]
        engine.apply(records)
        self.assertEqual(records[0]["is_equal"], 1.0)
        self.assertEqual(records[1]["is_equal"], 0.0)
        self.assertEqual(engine.warnings, [])

    def test_computed_field_parse_error(self) -> None:
        engine = ComputedFieldsEngine(
            [
                {
                    "id": "bad",
                    "fieldKey": "bad_field",
                    "expression": "{{value}} +",
                    "resultType": "number",
                }
            ]
        )
        records = [{"value": 1}]
        engine.apply(records)
        self.assertIsNone(records[0]["bad_field"])
        self.assertTrue(engine.warnings)


if __name__ == "__main__":
    unittest.main()
