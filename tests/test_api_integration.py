import os
import unittest

import asyncio

import httpx
import respx

from app.main import app
from app.services import record_cache


class ApiIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._allowlist = os.environ.get("REPORT_REMOTE_ALLOWLIST")
        self._redis_url = os.environ.get("REDIS_URL")
        self._upstream_base = os.environ.get("UPSTREAM_BASE_URL")
        self._max_records = os.environ.get("REPORT_MAX_RECORDS")
        self._async_reports = os.environ.get("ASYNC_REPORTS")
        os.environ["REPORT_REMOTE_ALLOWLIST"] = "example.com"
        os.environ.pop("REDIS_URL", None)
        os.environ["UPSTREAM_BASE_URL"] = "http://example.com"
        os.environ.pop("REPORT_MAX_RECORDS", None)
        os.environ["ASYNC_REPORTS"] = "0"
        record_cache._STORE.clear()

    def tearDown(self) -> None:
        if self._allowlist is None:
            os.environ.pop("REPORT_REMOTE_ALLOWLIST", None)
        else:
            os.environ["REPORT_REMOTE_ALLOWLIST"] = self._allowlist
        if self._redis_url is None:
            os.environ.pop("REDIS_URL", None)
        else:
            os.environ["REDIS_URL"] = self._redis_url
        if self._upstream_base is None:
            os.environ.pop("UPSTREAM_BASE_URL", None)
        else:
            os.environ["UPSTREAM_BASE_URL"] = self._upstream_base
        if self._max_records is None:
            os.environ.pop("REPORT_MAX_RECORDS", None)
        else:
            os.environ["REPORT_MAX_RECORDS"] = self._max_records
        if self._async_reports is None:
            os.environ.pop("ASYNC_REPORTS", None)
        else:
            os.environ["ASYNC_REPORTS"] = self._async_reports

    def _base_payload(self) -> dict:
        return {
            "templateId": "test-template",
            "remoteSource": {
                "url": "https://example.com/dtj/api/report",
                "method": "POST",
                "body": {"params": {"from": "test"}},
            },
            "snapshot": {
                "pivot": {
                    "rows": ["cls"],
                    "columns": ["year"],
                    "filters": ["cls"],
                },
                "metrics": [
                    {"key": "value__sum", "sourceKey": "value", "op": "sum"},
                ],
                "fieldMeta": {},
            },
            "filters": {"globalFilters": {}, "containerFilters": {}},
        }

    def _mock_upstream(self) -> respx.MockRouter:
        router = respx.mock(assert_all_called=True)
        router.__enter__()
        router.post("https://example.com/dtj/api/report").mock(
            return_value=httpx.Response(
                200,
                json={
                    "result": {
                        "records": [
                            {"cls": "A", "year": 2024, "value": 10, "count": 1},
                            {"cls": "B", "year": 2024, "value": 20, "count": 2},
                        ]
                    }
                },
            )
        )
        return router

    async def _post(self, path: str, payload: dict) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.post(path, json=payload)

    async def _get(self, path: str) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.get(path)

    def test_report_view_shape(self) -> None:
        router = self._mock_upstream()
        try:
            payload = self._base_payload()
            response = asyncio.run(self._post("/api/report/view", payload))
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("view", data)
            self.assertIn("chart", data)
            self.assertIsInstance(data["view"], dict)
            self.assertIsInstance(data["chart"], dict)
            self.assertIn("type", data["chart"])
            self.assertIn("data", data["chart"])
        finally:
            router.__exit__(None, None, None)

    def test_report_filters_shape(self) -> None:
        router = self._mock_upstream()
        try:
            payload = self._base_payload()
            response = asyncio.run(self._post("/api/report/filters", payload))
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("options", data)
            self.assertIn("meta", data)
            self.assertIn("truncated", data)
            self.assertIsInstance(data["options"], dict)
            self.assertIsInstance(data["meta"], dict)
            self.assertIsInstance(data["truncated"], dict)
        finally:
            router.__exit__(None, None, None)

    def test_report_details_paging(self) -> None:
        router = self._mock_upstream()
        try:
            payload = self._base_payload()
            payload.update(
                {
                    "detailFields": ["cls", "year", "value"],
                    "limit": 1,
                    "offset": 1,
                }
            )
            response = asyncio.run(self._post("/api/report/details", payload))
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("entries", data)
            self.assertIn("total", data)
            self.assertEqual(data["limit"], 1)
            self.assertEqual(data["offset"], 1)
            self.assertIsInstance(data["entries"], list)
            self.assertIsInstance(data["total"], int)
            self.assertEqual(len(data["entries"]), 1)
        finally:
            router.__exit__(None, None, None)

    def test_batch_smoke(self) -> None:
        response = asyncio.run(
            self._post(
                "/batch",
                {
                    "endpoint": "/dtj/api/plan",
                    "method": "POST",
                    "params": [{"date": "2025-01-01", "periodType": 11}],
                },
            )
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("job_id", data)
        status = asyncio.run(self._get(f"/batch/{data['job_id']}"))
        self.assertEqual(status.status_code, 200)
        status_payload = status.json()
        self.assertIn("status", status_payload)
        self.assertIn("progress", status_payload)


if __name__ == "__main__":
    unittest.main()
