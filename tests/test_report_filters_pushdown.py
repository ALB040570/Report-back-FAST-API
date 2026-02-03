import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.main import app
from app.services import record_cache


class ReportFiltersPushdownTests(unittest.TestCase):
    def setUp(self) -> None:
        self._redis_url = os.environ.pop("REDIS_URL", None)
        record_cache._REDIS_CLIENT = None
        record_cache._REDIS_URL = None
        record_cache._STORE.clear()

    def tearDown(self) -> None:
        if self._redis_url is not None:
            os.environ["REDIS_URL"] = self._redis_url
        record_cache._REDIS_CLIENT = None
        record_cache._REDIS_URL = None
        record_cache._STORE.clear()

    def _base_payload(self) -> dict:
        return {
            "templateId": "filters-pushdown",
            "remoteSource": {
                "url": "https://example.com/dtj/api/report",
                "method": "POST",
                "body": {"params": {"from": "test"}},
            },
            "snapshot": {
                "pivot": {"rows": ["cls"], "columns": ["year"], "filters": ["cls"]},
                "metrics": [{"key": "value__sum", "sourceKey": "value", "op": "sum"}],
                "fieldMeta": {},
            },
            "filters": {"globalFilters": {}, "containerFilters": {}},
        }

    async def _post(self, path: str, payload: dict) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.post(path, json=payload)

    def test_filters_disable_pushdown(self) -> None:
        payload = self._base_payload()
        mock_records = [
            {"cls": "A", "year": 2024, "value": 10, "count": 1},
            {"cls": "B", "year": 2024, "value": 20, "count": 2},
        ]
        mock_load = AsyncMock(return_value=mock_records)
        with patch("app.main.async_load_records", mock_load):
            response = asyncio.run(self._post("/api/report/filters", payload))

        self.assertEqual(response.status_code, 200)
        mock_load.assert_awaited()
        kwargs = mock_load.call_args.kwargs
        self.assertIsNone(kwargs.get("payload_filters"))
        self.assertIs(kwargs.get("pushdown_enabled"), False)


if __name__ == "__main__":
    unittest.main()
