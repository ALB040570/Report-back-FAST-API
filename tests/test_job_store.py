import asyncio
import unittest

from app.storage.job_store import InMemoryJobStore


class JobStoreTests(unittest.TestCase):
    def test_in_memory_store_basic(self) -> None:
        async def run() -> None:
            store = InMemoryJobStore()
            await store.set_job("job-1", {"status": "queued"}, ttl_seconds=1)
            job = await store.get_job("job-1")
            self.assertIsNotNone(job)
            self.assertEqual(job.get("status"), "queued")

            await store.update_job("job-1", {"status": "running"})
            job = await store.get_job("job-1")
            self.assertEqual(job.get("status"), "running")

            await store.enqueue_job("job-1")
            dequeued = await store.dequeue_job(timeout_seconds=1)
            self.assertEqual(dequeued, "job-1")

        asyncio.run(run())

    def test_in_memory_store_expiry(self) -> None:
        async def run() -> None:
            store = InMemoryJobStore()
            await store.set_job("job-expire", {"status": "done"}, ttl_seconds=0)
            job = await store.get_job("job-expire")
            self.assertIsNone(job)

            await store.set_job("job-expire-2", {"status": "done"}, ttl_seconds=1)
            await asyncio.sleep(1.1)
            expired = await store.get_job("job-expire-2")
            self.assertIsNone(expired)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
