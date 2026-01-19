import unittest

from app.services.data_source_client import build_request_payloads


class DataSourceClientTests(unittest.TestCase):
    def test_build_request_payloads_params_list(self) -> None:
        body = {
            "method": "data/loadPlan",
            "params": [
                {"date": "2025-01-01", "periodType": 11},
                {"date": "2026-01-01", "periodType": 12},
            ],
            "extra": "value",
        }
        payloads = build_request_payloads(body)
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0].body["method"], "data/loadPlan")
        self.assertEqual(payloads[0].body["extra"], "value")
        self.assertEqual(payloads[0].body["params"], {"date": "2025-01-01", "periodType": 11})
        self.assertEqual(payloads[0].params, {"date": "2025-01-01", "periodType": 11})

    def test_build_request_payloads_requests_list(self) -> None:
        body = {
            "method": "data/loadPlan",
            "requests": [
                {"params": {"date": "2025-01-01", "periodType": 11}},
                {
                    "params": {"date": "2026-01-01", "periodType": 12},
                    "body": {"method": "data/loadFact", "extra": True},
                },
            ],
        }
        payloads = build_request_payloads(body)
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0].body["method"], "data/loadPlan")
        self.assertEqual(payloads[0].body["params"], {"date": "2025-01-01", "periodType": 11})
        self.assertEqual(payloads[0].params, {"date": "2025-01-01", "periodType": 11})
        self.assertEqual(payloads[1].body["method"], "data/loadFact")
        self.assertEqual(payloads[1].body["extra"], True)
        self.assertEqual(payloads[1].body["params"], {"date": "2026-01-01", "periodType": 12})
        self.assertEqual(payloads[1].params, {"date": "2026-01-01", "periodType": 12})

    def test_build_request_payloads_params_list_scalar(self) -> None:
        body = {"method": "data/loadEquipment", "params": [0]}
        payloads = build_request_payloads(body)
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0].body["method"], "data/loadEquipment")
        self.assertEqual(payloads[0].body["params"], [0])
        self.assertIsNone(payloads[0].params)


if __name__ == "__main__":
    unittest.main()
