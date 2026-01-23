import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

import httpx
import respx

from app.models.remote_source import RemoteSource
from app.services.join_service import (
    PreparedJoin,
    apply_prepared_join_lookups,
    apply_prepared_joins,
    prepare_joins_streaming,
)
from app.services.source_registry import SourceConfig


class JoinStreamingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._remote_allowlist = os.environ.get("REPORT_REMOTE_ALLOWLIST")
        os.environ["REPORT_REMOTE_ALLOWLIST"] = "example.com"

    def tearDown(self) -> None:
        if self._remote_allowlist is None:
            os.environ.pop("REPORT_REMOTE_ALLOWLIST", None)
        else:
            os.environ["REPORT_REMOTE_ALLOWLIST"] = self._remote_allowlist

    def test_prepare_joins_streaming_uses_paging(self) -> None:
        join_rows = [
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"},
            {"id": 3, "name": "C"},
        ]
        join = {
            "id": "join-1",
            "targetSourceId": "join-source",
            "primaryKey": "obj",
            "foreignKey": "id",
            "joinType": "left",
            "resultPrefix": "J",
            "fields": ["name"],
        }
        remote_source = RemoteSource(
            url="https://example.com/base",
            method="POST",
            body={},
            joins=[join],
        )
        config = SourceConfig(
            source_id="join-source",
            url="https://example.com/join",
            method="POST",
            body={"paging": {"limit": 2, "offset": 0}},
            raw_body=None,
            headers={"Content-Type": "application/json"},
        )

        router = respx.mock(assert_all_called=True)
        router.__enter__()

        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8")) if request.content else {}
            paging = payload.get("paging") or {}
            offset = int(paging.get("offset", 0))
            limit = int(paging.get("limit", 2))
            page = join_rows[offset : offset + limit]
            return httpx.Response(200, json={"result": {"records": page}})

        route = router.post("https://example.com/join").mock(side_effect=handler)
        try:
            with patch(
                "app.services.join_service.get_source_config",
                new=AsyncMock(return_value=config),
            ), patch(
                "app.services.join_service.async_load_records",
                new=AsyncMock(side_effect=AssertionError("async_load_records should not be called")),
            ):
                prepared = asyncio.run(
                    prepare_joins_streaming(
                        remote_source,
                        chunk_size=2,
                        paging_allowlist="example.com",
                        paging_max_pages=10,
                    )
                )
            base_rows = [{"obj": 1}, {"obj": 2}, {"obj": 4}]
            expected_rows, _ = apply_prepared_joins(
                base_rows,
                [PreparedJoin(join=join, rows=join_rows, target_source_id="join-source")],
            )
            actual_rows, _ = apply_prepared_join_lookups(base_rows, prepared)
            self.assertEqual(expected_rows, actual_rows)
            self.assertGreaterEqual(len(route.calls), 2)
        finally:
            router.__exit__(None, None, None)


if __name__ == "__main__":
    unittest.main()
