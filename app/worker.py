import asyncio
import logging

from app.services.batch_service import process_job
from app.storage.job_store import get_job_store


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def _worker_loop() -> None:
    store = get_job_store()
    logger.info("Batch worker started")
    while True:
        job_id = await store.dequeue_job(timeout_seconds=5)
        if not job_id:
            await asyncio.sleep(0.1)
            continue
        await process_job(job_id, store)


def main() -> None:
    asyncio.run(_worker_loop())


if __name__ == "__main__":
    main()
