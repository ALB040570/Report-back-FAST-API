import asyncio
import json
import os
import unittest

import httpx
import respx

from app.models.remote_source import RemoteSource
from app.services.data_source_client import async_iter_records, async_load_records, build_request_payloads


class DataSourceClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self._remote_allowlist = os.environ.get("REPORT_REMOTE_ALLOWLIST")
        os.environ["REPORT_REMOTE_ALLOWLIST"] = "example.com"

    def tearDown(self) -> None:
        if self._remote_allowlist is None:
            os.environ.pop("REPORT_REMOTE_ALLOWLIST", None)
        else:
            os.environ["REPORT_REMOTE_ALLOWLIST"] = self._remote_allowlist

    def test_build_request_payloads_params_list(self) -> None:
        body = {
            "method": "data/loadPlan",
            "params": [
                {"date": "2025-01-01", "periodType": 11},
                {"date": "2026-01-01", "periodType": 12},
            ],
            "extra": "value",
            "splitParams": True,
        }
        payloads = build_request_payloads(body)
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0].body["method"], "data/loadPlan")
        self.assertEqual(payloads[0].body["extra"], "value")
        self.assertEqual(payloads[0].body["params"], [{"date": "2025-01-01", "periodType": 11}])
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
            "splitParams": True,
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

    def test_build_request_payloads_params_list_no_split(self) -> None:
        body = {
            "method": "data/loadPlan",
            "params": [
                {"date": "2025-01-01", "periodType": 11},
                {"date": "2026-01-01", "periodType": 12},
            ],
            "splitParams": False,
        }
        payloads = build_request_payloads(body)
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0].body["params"], body["params"])
        self.assertIsNone(payloads[0].params)
        self.assertNotIn("splitParams", payloads[0].body)

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

    def test_async_load_records_split_params_metadata(self) -> None:
        remote_source = RemoteSource(
            url="mock://split",
            method="POST",
            body={
                "method": "data/loadPlan",
                "params": [{"date": "2025-01-01", "periodType": 11}],
                "splitParams": True,
            },
        )
        records = asyncio.run(async_load_records(remote_source))
        self.assertTrue(records)
        self.assertEqual(records[0]["requestDate"], "2025-01-01")
        self.assertEqual(records[0]["requestPeriodType"], 11)

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

    def test_async_iter_records_paging_requests_multiple_pages(self) -> None:
        remote_source = RemoteSource(
            url="https://example.com/data",
            method="POST",
            body={"paging": {"limit": 2, "offset": 0}},
        )
        router = respx.mock(assert_all_called=True)
        router.__enter__()

        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8")) if request.content else {}
            paging = payload.get("paging") or {}
            offset = paging.get("offset", 0)
            if offset == 0:
                records = [{"id": 1}, {"id": 2}]
            elif offset == 2:
                records = [{"id": 3}, {"id": 4}]
            else:
                records = []
            return httpx.Response(200, json={"result": {"records": records}})

        route = router.post("https://example.com/data").mock(side_effect=handler)
        try:
            async def run() -> list[list[dict]]:
                chunks: list[list[dict]] = []
                async for chunk in async_iter_records(
                    remote_source,
                    chunk_size=2,
                    paging_allowlist="example.com",
                    paging_max_pages=10,
                ):
                    chunks.append(chunk)
                return chunks

            chunks = asyncio.run(run())
            self.assertEqual(len(route.calls), 3)
            self.assertEqual(len(chunks), 2)
            self.assertEqual(len(chunks[0]), 2)
            self.assertEqual(len(chunks[1]), 2)
        finally:
            router.__exit__(None, None, None)

    def test_async_iter_records_paging_disabled_without_allowlist(self) -> None:
        remote_source = RemoteSource(
            url="https://example.com/data",
            method="POST",
            body={"paging": {"limit": 2, "offset": 0}},
        )
        router = respx.mock(assert_all_called=True)
        router.__enter__()
        route = router.post("https://example.com/data").mock(
            return_value=httpx.Response(
                200,
                json={"result": {"records": [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]}},
            )
        )
        try:
            async def run() -> list[list[dict]]:
                chunks: list[list[dict]] = []
                async for chunk in async_iter_records(remote_source, chunk_size=2):
                    chunks.append(chunk)
                return chunks

            chunks = asyncio.run(run())
            self.assertEqual(len(route.calls), 1)
            self.assertEqual(len(chunks), 2)
        finally:
            router.__exit__(None, None, None)


if __name__ == "__main__":
    unittest.main()
