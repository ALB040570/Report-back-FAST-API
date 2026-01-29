import json
import os
import unittest

import asyncio

import httpx
import respx

from app.main import app
from app.services import record_cache
from app.storage.job_store import get_job_store


class ApiIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._allowlist = os.environ.get("REPORT_REMOTE_ALLOWLIST")
        self._redis_url = os.environ.get("REDIS_URL")
        self._upstream_base = os.environ.get("UPSTREAM_BASE_URL")
        self._max_records = os.environ.get("REPORT_MAX_RECORDS")
        self._async_reports = os.environ.get("ASYNC_REPORTS")
        self._report_streaming = os.environ.get("REPORT_STREAMING")
        self._report_chunk_size = os.environ.get("REPORT_CHUNK_SIZE")
        self._report_streaming_max_groups = os.environ.get("REPORT_STREAMING_MAX_GROUPS")
        self._report_streaming_max_unique = os.environ.get("REPORT_STREAMING_MAX_UNIQUE_VALUES_PER_DIM")
        self._report_join_lookup_max_keys = os.environ.get("REPORT_JOIN_LOOKUP_MAX_KEYS")
        self._report_paging_allowlist = os.environ.get("REPORT_PAGING_ALLOWLIST")
        self._report_paging_max_pages = os.environ.get("REPORT_PAGING_MAX_PAGES")
        self._report_upstream_paging = os.environ.get("REPORT_UPSTREAM_PAGING")
        self._pushdown_enabled = os.environ.get("REPORT_UPSTREAM_PUSHDOWN")
        self._pushdown_allowlist = os.environ.get("REPORT_PUSHDOWN_ALLOWLIST")
        self._pushdown_max_filters = os.environ.get("REPORT_PUSHDOWN_MAX_FILTERS")
        self._pushdown_max_in_values = os.environ.get("REPORT_PUSHDOWN_MAX_IN_VALUES")
        self._pushdown_safe_only = os.environ.get("REPORT_PUSHDOWN_SAFE_ONLY")
        self._pushdown_override = os.environ.get("REPORT_PUSHDOWN_OVERRIDE")
        os.environ["REPORT_REMOTE_ALLOWLIST"] = "example.com"
        os.environ.pop("REDIS_URL", None)
        os.environ["UPSTREAM_BASE_URL"] = "http://example.com"
        os.environ.pop("REPORT_MAX_RECORDS", None)
        os.environ["ASYNC_REPORTS"] = "0"
        os.environ["REPORT_STREAMING"] = "0"
        os.environ["REPORT_STREAMING_MAX_GROUPS"] = "200000"
        os.environ["REPORT_STREAMING_MAX_UNIQUE_VALUES_PER_DIM"] = "0"
        os.environ["REPORT_JOIN_LOOKUP_MAX_KEYS"] = "2000000"
        os.environ.pop("REPORT_PAGING_ALLOWLIST", None)
        os.environ.pop("REPORT_PAGING_MAX_PAGES", None)
        os.environ.pop("REPORT_UPSTREAM_PAGING", None)
        os.environ["REPORT_UPSTREAM_PUSHDOWN"] = "0"
        os.environ.pop("REPORT_PUSHDOWN_ALLOWLIST", None)
        os.environ.pop("REPORT_PUSHDOWN_MAX_FILTERS", None)
        os.environ.pop("REPORT_PUSHDOWN_MAX_IN_VALUES", None)
        os.environ.pop("REPORT_PUSHDOWN_SAFE_ONLY", None)
        os.environ.pop("REPORT_PUSHDOWN_OVERRIDE", None)
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
        if self._report_streaming is None:
            os.environ.pop("REPORT_STREAMING", None)
        else:
            os.environ["REPORT_STREAMING"] = self._report_streaming
        if self._report_chunk_size is None:
            os.environ.pop("REPORT_CHUNK_SIZE", None)
        else:
            os.environ["REPORT_CHUNK_SIZE"] = self._report_chunk_size
        if self._report_streaming_max_groups is None:
            os.environ.pop("REPORT_STREAMING_MAX_GROUPS", None)
        else:
            os.environ["REPORT_STREAMING_MAX_GROUPS"] = self._report_streaming_max_groups
        if self._report_streaming_max_unique is None:
            os.environ.pop("REPORT_STREAMING_MAX_UNIQUE_VALUES_PER_DIM", None)
        else:
            os.environ["REPORT_STREAMING_MAX_UNIQUE_VALUES_PER_DIM"] = self._report_streaming_max_unique
        if self._report_join_lookup_max_keys is None:
            os.environ.pop("REPORT_JOIN_LOOKUP_MAX_KEYS", None)
        else:
            os.environ["REPORT_JOIN_LOOKUP_MAX_KEYS"] = self._report_join_lookup_max_keys
        if self._report_paging_allowlist is None:
            os.environ.pop("REPORT_PAGING_ALLOWLIST", None)
        else:
            os.environ["REPORT_PAGING_ALLOWLIST"] = self._report_paging_allowlist
        if self._report_paging_max_pages is None:
            os.environ.pop("REPORT_PAGING_MAX_PAGES", None)
        else:
            os.environ["REPORT_PAGING_MAX_PAGES"] = self._report_paging_max_pages
        if self._report_upstream_paging is None:
            os.environ.pop("REPORT_UPSTREAM_PAGING", None)
        else:
            os.environ["REPORT_UPSTREAM_PAGING"] = self._report_upstream_paging
        if self._pushdown_enabled is None:
            os.environ.pop("REPORT_UPSTREAM_PUSHDOWN", None)
        else:
            os.environ["REPORT_UPSTREAM_PUSHDOWN"] = self._pushdown_enabled
        if self._pushdown_allowlist is None:
            os.environ.pop("REPORT_PUSHDOWN_ALLOWLIST", None)
        else:
            os.environ["REPORT_PUSHDOWN_ALLOWLIST"] = self._pushdown_allowlist
        if self._pushdown_max_filters is None:
            os.environ.pop("REPORT_PUSHDOWN_MAX_FILTERS", None)
        else:
            os.environ["REPORT_PUSHDOWN_MAX_FILTERS"] = self._pushdown_max_filters
        if self._pushdown_max_in_values is None:
            os.environ.pop("REPORT_PUSHDOWN_MAX_IN_VALUES", None)
        else:
            os.environ["REPORT_PUSHDOWN_MAX_IN_VALUES"] = self._pushdown_max_in_values
        if self._pushdown_safe_only is None:
            os.environ.pop("REPORT_PUSHDOWN_SAFE_ONLY", None)
        else:
            os.environ["REPORT_PUSHDOWN_SAFE_ONLY"] = self._pushdown_safe_only
        if self._pushdown_override is None:
            os.environ.pop("REPORT_PUSHDOWN_OVERRIDE", None)
        else:
            os.environ["REPORT_PUSHDOWN_OVERRIDE"] = self._pushdown_override

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

    def test_report_view_streaming_matches_sync(self) -> None:
        records = [
            {"cls": "A", "year": 2024, "value": 10, "count": 1},
            {"cls": "B", "year": 2024, "value": 20, "count": 2},
            {"cls": "B", "year": 2023, "value": 5, "count": 1},
        ]
        router = respx.mock(assert_all_called=True)
        router.__enter__()

        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8")) if request.content else {}
            paging = payload.get("paging")
            if not paging:
                return httpx.Response(200, json={"result": {"records": records}})
            offset = int(paging.get("offset", 0))
            limit = int(paging.get("limit", len(records)))
            page = records[offset : offset + limit]
            return httpx.Response(200, json={"result": {"records": page}})

        router.post("https://example.com/dtj/api/report").mock(side_effect=handler)
        try:
            payload = self._base_payload()
            os.environ["REPORT_STREAMING"] = "0"
            sync_response = asyncio.run(self._post("/api/report/view", payload))
            self.assertEqual(sync_response.status_code, 200)
            os.environ["REPORT_STREAMING"] = "1"
            os.environ["REPORT_CHUNK_SIZE"] = "1"
            os.environ["REPORT_PAGING_ALLOWLIST"] = "example.com"
            paged_payload = dict(payload)
            paged_payload["remoteSource"] = dict(payload["remoteSource"])
            paged_payload["remoteSource"]["body"] = dict(payload["remoteSource"]["body"])
            paged_payload["remoteSource"]["body"]["paging"] = {"limit": 1, "offset": 0}
            streaming_response = asyncio.run(self._post("/api/report/view", paged_payload))
            self.assertEqual(streaming_response.status_code, 200)
            self.assertEqual(sync_response.json(), streaming_response.json())
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

    def test_report_filters_computed_warnings(self) -> None:
        router = self._mock_upstream()
        try:
            payload = self._base_payload()
            payload["remoteSource"] = dict(payload["remoteSource"])
            payload["remoteSource"]["computedFields"] = [
                {
                    "id": "bad",
                    "fieldKey": "bad_field",
                    "expression": "{{value}} +",
                    "resultType": "number",
                }
            ]
            response = asyncio.run(self._post("/api/report/filters", payload))
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("computedWarnings", data)
            self.assertIsInstance(data["computedWarnings"], list)
            self.assertTrue(data["computedWarnings"])
            self.assertEqual(data["computedWarnings"][0].get("fieldKey"), "bad_field")
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

    def test_report_details_computed_warnings(self) -> None:
        router = self._mock_upstream()
        try:
            payload = self._base_payload()
            payload["remoteSource"] = dict(payload["remoteSource"])
            payload["remoteSource"]["computedFields"] = [
                {
                    "id": "bad",
                    "fieldKey": "bad_field",
                    "expression": "{{value}} +",
                    "resultType": "number",
                }
            ]
            payload.update(
                {
                    "detailFields": ["cls", "year", "value"],
                }
            )
            response = asyncio.run(self._post("/api/report/details", payload))
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("computedWarnings", data)
            self.assertIsInstance(data["computedWarnings"], list)
            self.assertTrue(data["computedWarnings"])
            self.assertEqual(data["computedWarnings"][0].get("fieldKey"), "bad_field")
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

    def test_batch_results_paged_inline(self) -> None:
        store = get_job_store()
        job_id = "job-inline"
        job = {
            "job_id": job_id,
            "status": "done",
            "total": 3,
            "done": 3,
            "results": [
                {"ok": True, "data": {"id": 1}},
                {"ok": True, "data": {"id": 2}},
                {"ok": True, "data": {"id": 3}},
            ],
            "resultsSummary": {"ok": 3, "failed": 0, "total": 3},
            "resultsFileRef": None,
        }
        asyncio.run(store.set_job(job_id, job))
        response = asyncio.run(self._get(f"/batch/{job_id}/results?offset=1&limit=1"))
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 3)
        self.assertEqual(payload["offset"], 1)
        self.assertEqual(payload["limit"], 1)
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(payload["results"][0]["data"]["id"], 2)

    def test_batch_results_paged_file(self) -> None:
        store = get_job_store()
        job_id = "job-file"
        results_dir = os.path.join(os.getcwd(), "batch_results")
        os.makedirs(results_dir, exist_ok=True)
        results_path = os.path.join(results_dir, f"{job_id}.json")
        results_payload = [
            {"ok": True, "data": {"id": 1}},
            {"ok": True, "data": {"id": 2}},
            {"ok": True, "data": {"id": 3}},
        ]
        with open(results_path, "w", encoding="utf-8") as handle:
            json.dump(results_payload, handle)
        try:
            job = {
                "job_id": job_id,
                "status": "done",
                "total": 3,
                "done": 3,
                "results": None,
                "resultsSummary": {"ok": 3, "failed": 0, "total": 3},
                "resultsFileRef": results_path,
            }
            asyncio.run(store.set_job(job_id, job))
            response = asyncio.run(self._get(f"/batch/{job_id}/results?offset=1&limit=2"))
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["total"], 3)
            self.assertEqual(len(payload["results"]), 2)
            self.assertEqual(payload["results"][0]["data"]["id"], 2)
        finally:
            try:
                os.remove(results_path)
            except OSError:
                pass

    def test_batch_results_available_via(self) -> None:
        store = get_job_store()
        job_id = "job-paged"
        job = {
            "job_id": job_id,
            "status": "done",
            "createdAt": "2025-01-01T00:00:00Z",
            "total": 1,
            "done": 1,
            "results": [{"ok": True, "params": {}}],
            "resultsSummary": {"ok": 1, "failed": 0, "total": 1},
            "resultsFileRef": None,
        }
        asyncio.run(store.set_job(job_id, job))
        response = asyncio.run(self._get(f"/batch/{job_id}"))
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload.get("resultsAvailableVia"), "paged")


if __name__ == "__main__":
    unittest.main()
