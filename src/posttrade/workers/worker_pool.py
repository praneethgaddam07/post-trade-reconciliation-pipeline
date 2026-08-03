import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

from posttrade.config import settings
from posttrade.workers.worker import run_worker

logger = logging.getLogger(__name__)


def run_worker_pool(
    size: int | None = None,
    max_messages_per_worker: int | None = None,
    max_idle_reads: int | None = None,
    stream_name: str | None = None,
    group_name: str | None = None,
) -> dict[str, int]:
    """Spawns `size` OS-level worker processes against the same consumer
    group and waits for them to finish. Returns {consumer_name: processed_count}
    so callers can verify work was actually split across workers."""
    size = size or settings.worker_pool_size
    names = [f"worker-{i}" for i in range(size)]
    results: dict[str, int] = {}
    with ProcessPoolExecutor(max_workers=size) as executor:
        futures = {
            executor.submit(
                run_worker, name, max_messages_per_worker, max_idle_reads, stream_name, group_name
            ): name
            for name in names
        }
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    outcome = run_worker_pool(max_idle_reads=3)
    logger.info("pool results: %s", outcome)
