import asyncio
import json
import os
import unittest

import httpx
import respx

from app.models.remote_source import RemoteSource
from app.services.data_source_client import async_iter_records


class PushdownIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._pushdown_enabled = os.environ.get("REPORT_UPSTREAM_PUSHDOWN")
        self._pushdown_allowlist = os.environ.get("REPORT_PUSHDOWN_ALLOWLIST")
        self._paging_allowlist = os.environ.get("REPORT_PAGING_ALLOWLIST")
        self._pushdown_safe_only = os.environ.get("REPORT_PUSHDOWN_SAFE_ONLY")

    def tearDown(self) -> None:
        if self._pushdown_enabled is None:
            os.environ.pop("REPORT_UPSTREAM_PUSHDOWN", None)
        else:
            os.environ["REPORT_UPSTREAM_PUSHDOWN"] = self._pushdown_enabled
        if self._pushdown_allowlist is None:
            os.environ.pop("REPORT_PUSHDOWN_ALLOWLIST", None)
        else:
            os.environ["REPORT_PUSHDOWN_ALLOWLIST"] = self._pushdown_allowlist
        if self._paging_allowlist is None:
            os.environ.pop("REPORT_PAGING_ALLOWLIST", None)
        else:
            os.environ["REPORT_PAGING_ALLOWLIST"] = self._paging_allowlist
        if self._pushdown_safe_only is None:
            os.environ.pop("REPORT_PUSHDOWN_SAFE_ONLY", None)
        else:
            os.environ["REPORT_PUSHDOWN_SAFE_ONLY"] = self._pushdown_safe_only

    def test_pushdown_applied_with_paging(self) -> None:
        os.environ["REPORT_UPSTREAM_PUSHDOWN"] = "1"
        os.environ["REPORT_PUSHDOWN_ALLOWLIST"] = "example.com"
        os.environ["REPORT_PAGING_ALLOWLIST"] = "example.com"
        os.environ["REPORT_PUSHDOWN_SAFE_ONLY"] = "1"

        remote_source = RemoteSource(
            url="https://example.com/dtj/api/report",
            method="POST",
            body={"params": [{}]},
            pushdown={
                "enabled": True,
                "mode": "jsonrpc_params",
                "paging": {
                    "strategy": "offset",
                    "limitPath": "body.params.0.limit",
                    "offsetPath": "body.params.0.offset",
                },
                "filters": [
                    {
                        "filterKey": "objLocation",
                        "op": "eq",
                        "targetPath": "body.params.0.objLocation",
                    }
                ],
            },
        )
        filters = {"globalFilters": {"objLocation": {"values": [1069]}}}
        records = [
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ]
        seen_params: list[dict] = []

        router = respx.mock(assert_all_called=True)
        router.__enter__()

        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            params = payload["params"][0]
            seen_params.append(params)
            offset = int(params.get("offset", 0))
            limit = int(params.get("limit", 2))
            page = records[offset : offset + limit]
            return httpx.Response(200, json={"result": {"records": page}})

        router.post("https://example.com/dtj/api/report").mock(side_effect=handler)
        try:
            chunks = []
            async def _run() -> None:
                async for chunk in async_iter_records(
                    remote_source,
                    chunk_size=2,
                    payload_filters=filters,
                ):
                    chunks.append(chunk)

            asyncio.run(_run())
            self.assertGreaterEqual(len(seen_params), 2)
            self.assertTrue(all("objLocation" in item for item in seen_params))
            self.assertTrue(all("limit" in item and "offset" in item for item in seen_params))
            flat = [item for chunk in chunks for item in chunk]
            self.assertEqual(len(flat), len(records))
        finally:
            router.__exit__(None, None, None)

    def test_pushdown_disabled(self) -> None:
        os.environ["REPORT_UPSTREAM_PUSHDOWN"] = "0"
        os.environ.pop("REPORT_PUSHDOWN_ALLOWLIST", None)
        os.environ["REPORT_PAGING_ALLOWLIST"] = "example.com"

        remote_source = RemoteSource(
            url="https://example.com/dtj/api/report",
            method="POST",
            body={"params": [{}]},
            pushdown={
                "enabled": True,
                "mode": "jsonrpc_params",
                "filters": [
                    {
                        "filterKey": "objLocation",
                        "op": "eq",
                        "targetPath": "body.params.0.objLocation",
                    }
                ],
            },
        )
        filters = {"globalFilters": {"objLocation": {"values": [1069]}}}
        seen_payloads: list[dict] = []

        router = respx.mock(assert_all_called=True)
        router.__enter__()

        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            seen_payloads.append(payload)
            return httpx.Response(200, json={"result": {"records": []}})

        router.post("https://example.com/dtj/api/report").mock(side_effect=handler)
        try:
            async def _run() -> None:
                async for _ in async_iter_records(
                    remote_source,
                    chunk_size=2,
                    payload_filters=filters,
                ):
                    pass

            asyncio.run(_run())
            self.assertEqual(len(seen_payloads), 1)
            self.assertNotIn("objLocation", seen_payloads[0]["params"][0])
        finally:
            router.__exit__(None, None, None)

    def test_pushdown_fallback_on_error(self) -> None:
        os.environ["REPORT_UPSTREAM_PUSHDOWN"] = "1"
        os.environ["REPORT_PUSHDOWN_ALLOWLIST"] = "example.com"

        remote_source = RemoteSource(
            url="https://example.com/dtj/api/report",
            method="POST",
            body={"params": [{}]},
            pushdown={
                "enabled": True,
                "mode": "jsonrpc_params",
                "filters": [
                    {
                        "filterKey": "objLocation",
                        "op": "eq",
                        "targetPath": "body.params.foo.objLocation",
                    }
                ],
            },
        )
        filters = {"globalFilters": {"objLocation": {"values": [1069]}}}
        seen_payloads: list[dict] = []

        router = respx.mock(assert_all_called=True)
        router.__enter__()

        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            seen_payloads.append(payload)
            return httpx.Response(200, json={"result": {"records": []}})

        router.post("https://example.com/dtj/api/report").mock(side_effect=handler)
        try:
            async def _run() -> None:
                async for _ in async_iter_records(
                    remote_source,
                    chunk_size=2,
                    payload_filters=filters,
                ):
                    pass

            asyncio.run(_run())
            self.assertEqual(len(seen_payloads), 1)
            self.assertNotIn("objLocation", seen_payloads[0]["params"][0])
        finally:
            router.__exit__(None, None, None)
