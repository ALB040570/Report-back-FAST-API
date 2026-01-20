import asyncio
import os
import unittest

import httpx
import respx

from app.main import app


class ReportAsyncModeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = {
            "ASYNC_REPORTS": os.environ.get("ASYNC_REPORTS"),
            "REPORT_REMOTE_ALLOWLIST": os.environ.get("REPORT_REMOTE_ALLOWLIST"),
            "REDIS_URL": os.environ.get("REDIS_URL"),
            "UPSTREAM_BASE_URL": os.environ.get("UPSTREAM_BASE_URL"),
            "REPORT_JOB_MAX_CONCURRENCY": os.environ.get("REPORT_JOB_MAX_CONCURRENCY"),
            "REPORT_JOB_QUEUE_MAX_SIZE": os.environ.get("REPORT_JOB_QUEUE_MAX_SIZE"),
        }
        os.environ["REPORT_REMOTE_ALLOWLIST"] = "example.com"
        os.environ["UPSTREAM_BASE_URL"] = "http://example.com"
        os.environ.pop("REDIS_URL", None)

    def tearDown(self) -> None:
        for key, value in self._env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _payload(self) -> dict:
        return {
            "templateId": "async-template",
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

    def _mock_upstream(self, assert_all_called: bool = True) -> respx.MockRouter:
        router = respx.mock(assert_all_called=assert_all_called)
        router.__enter__()
        router.post("https://example.com/dtj/api/report").mock(
            return_value=httpx.Response(
                200,
                json={
                    "result": {
                        "records": [
                            {"cls": "A", "year": 2024, "value": 10},
                            {"cls": "B", "year": 2024, "value": 20},
                        ]
                    }
                },
            )
        )
        return router

    async def _client_post(self, path: str, payload: dict) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.post(path, json=payload)

    async def _client_get(self, path: str) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.get(path)

    def test_sync_mode_default(self) -> None:
        os.environ["ASYNC_REPORTS"] = "0"
        router = self._mock_upstream()
        try:
            response = asyncio.run(self._client_post("/api/report/view", self._payload()))
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("view", data)
            self.assertIn("chart", data)
        finally:
            router.__exit__(None, None, None)

    def test_async_mode_returns_job(self) -> None:
        os.environ["ASYNC_REPORTS"] = "1"
        os.environ["REPORT_JOB_MAX_CONCURRENCY"] = "1"
        router = self._mock_upstream()
        try:
            async def run() -> None:
                response = await self._client_post("/api/report/view", self._payload())
                self.assertEqual(response.status_code, 202)
                data = response.json()
                self.assertIn("job_id", data)
                self.assertEqual(data.get("status"), "queued")
                job_id = data.get("job_id")
                for _ in range(10):
                    status = await self._client_get(f"/api/report/jobs/{job_id}")
                    if status.json().get("status") == "done":
                        return
                    await asyncio.sleep(0.05)

            asyncio.run(run())
        finally:
            router.__exit__(None, None, None)

    def test_async_mode_job_result(self) -> None:
        os.environ["ASYNC_REPORTS"] = "1"
        os.environ["REPORT_JOB_MAX_CONCURRENCY"] = "1"
        router = self._mock_upstream()
        try:
            async def run() -> None:
                response = await self._client_post("/api/report/view", self._payload())
                self.assertEqual(response.status_code, 202)
                job_id = response.json().get("job_id")
                self.assertTrue(job_id)
                for _ in range(30):
                    status = await self._client_get(f"/api/report/jobs/{job_id}")
                    self.assertEqual(status.status_code, 200)
                    payload = status.json()
                    if payload.get("status") == "done":
                        result = payload.get("result")
                        self.assertIsInstance(result, dict)
                        self.assertIn("view", result)
                        self.assertIn("chart", result)
                        return
                    await asyncio.sleep(0.05)
                self.fail("Job did not finish in time")

            asyncio.run(run())
        finally:
            router.__exit__(None, None, None)

    def test_async_mode_force_sync(self) -> None:
        os.environ["ASYNC_REPORTS"] = "1"
        router = self._mock_upstream()
        try:
            async def run() -> None:
                response = await self._client_post("/api/report/view?sync=1", self._payload())
                self.assertEqual(response.status_code, 200)
                data = response.json()
                self.assertIn("view", data)
                self.assertIn("chart", data)

            asyncio.run(run())
        finally:
            router.__exit__(None, None, None)


if __name__ == "__main__":
    unittest.main()
