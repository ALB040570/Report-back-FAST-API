import asyncio
import os
import unittest

from app.services import record_cache


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


if __name__ == "__main__":
    unittest.main()
