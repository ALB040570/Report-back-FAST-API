import asyncio
import os
import unittest

from app.services import record_cache
from app.models.remote_source import RemoteSource


class RecordCacheTests(unittest.TestCase):
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

    def test_in_memory_cache_roundtrip(self) -> None:
        asyncio.run(record_cache.set_cached_records("cache-key", [{"value": 1}]))
        value = asyncio.run(record_cache.get_cached_records("cache-key"))
        self.assertEqual(value, [{"value": 1}])

    def test_cache_key_includes_filters(self) -> None:
        remote_source = RemoteSource(
            url="https://example.com/dtj/api/report",
            method="POST",
            body={"params": {"from": "test"}},
        )
        joins = []
        filters_a = {"globalFilters": {"cls": {"values": ["A"]}}, "containerFilters": {}}
        filters_b = {"globalFilters": {"cls": {"values": ["B"]}}, "containerFilters": {}}

        key_a = record_cache.build_records_cache_key("template", remote_source, joins, filters_a)
        key_b = record_cache.build_records_cache_key("template", remote_source, joins, filters_b)
        self.assertNotEqual(key_a, key_b)

    def test_cache_key_ignores_empty_filters(self) -> None:
        remote_source = RemoteSource(
            url="https://example.com/dtj/api/report",
            method="POST",
            body={"params": {"from": "test"}},
        )
        joins = []
        empty_filters = {"globalFilters": {}, "containerFilters": {}}

        key_no_filters = record_cache.build_records_cache_key("template", remote_source, joins, None)
        key_empty_filters = record_cache.build_records_cache_key("template", remote_source, joins, empty_filters)
        self.assertEqual(key_no_filters, key_empty_filters)


if __name__ == "__main__":
    unittest.main()
