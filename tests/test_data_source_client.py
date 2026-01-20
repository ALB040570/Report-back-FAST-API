import asyncio
import os
import unittest

from app.models.remote_source import RemoteSource
from app.services.data_source_client import async_load_records, build_request_payloads


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

    def test_async_load_records_from_batch_results(self) -> None:
        remote_source = RemoteSource(
            url="mock://batch",
            method="POST",
            body={
                "results": [
                    {
                        "ok": True,
                        "params": {"date": "2025-01-01"},
                        "data": {"result": {"records": [{"value": 1}]}},
                    }
                ]
            },
        )
        records = asyncio.run(async_load_records(remote_source))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["value"], 1)
        self.assertEqual(records[0]["requestDate"], "2025-01-01")

    def test_async_load_records_blocks_private_without_allowlist(self) -> None:
        allowlist = os.environ.pop("REPORT_REMOTE_ALLOWLIST", None)
        upstream_allowlist = os.environ.pop("UPSTREAM_ALLOWLIST", None)
        try:
            remote_source = RemoteSource(
                url="http://localhost:8080/private",
                method="POST",
                body={},
            )
            with self.assertRaises(ValueError):
                asyncio.run(async_load_records(remote_source))
        finally:
            if allowlist is not None:
                os.environ["REPORT_REMOTE_ALLOWLIST"] = allowlist
            if upstream_allowlist is not None:
                os.environ["UPSTREAM_ALLOWLIST"] = upstream_allowlist


if __name__ == "__main__":
    unittest.main()
