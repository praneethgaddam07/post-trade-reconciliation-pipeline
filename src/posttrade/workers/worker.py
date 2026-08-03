import logging
import os
import signal
from types import FrameType

from posttrade.config import settings
from posttrade.queue.redis_stream import StreamConsumer
from posttrade.storage.timeseries_store import get_tick_store

logger = logging.getLogger(__name__)


def run_worker(
    consumer_name: str,
    max_messages: int | None = None,
    max_idle_reads: int | None = None,
    stream_name: str | None = None,
    group_name: str | None = None,
) -> int:
    """Runs one consumer loop against the shared consumer group.

    Meant to be the entry point of a separate OS process (see
    `worker_pool.run_worker_pool`) — several of these run concurrently
    against the *same* Redis consumer group, so each message is delivered
    to exactly one of them. That's what makes this genuinely distributed
    rather than one script pretending to be several.

    `max_messages` / `max_idle_reads` exist so a run can terminate
    deterministically for load tests and demos instead of blocking forever.
    `stream_name` / `group_name` default to settings but are overridable so
    the load harness can point workers at an isolated test stream.
    """
    stream_name = stream_name or settings.redis_stream_name
    group_name = group_name or settings.redis_consumer_group
    logging.basicConfig(
        level=logging.INFO, format=f"%(asctime)s [{consumer_name}] %(levelname)s %(message)s", force=True
    )
    consumer = StreamConsumer(settings.redis_url, stream_name, group_name, consumer_name)
    store = get_tick_store()
    processed = 0
    idle_reads = 0
    pid = os.getpid()
    logger.info("worker %s (pid=%d) starting", consumer_name, pid)

    running = True

    def _stop(signum: int, frame: FrameType | None) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    try:
        while running:
            messages = consumer.read(count=50, block_ms=2000)
            if not messages:
                idle_reads += 1
                if max_idle_reads is not None and idle_reads >= max_idle_reads:
                    logger.info("worker %s (pid=%d) idle for %d reads, stopping", consumer_name, pid, idle_reads)
                    break
                continue
            idle_reads = 0
            for msg in messages:
                store.write_tick(msg.tick, worker=consumer_name)
                consumer.ack(msg.message_id)
                processed += 1
                logger.info(
                    "worker=%s pid=%d handled %s seq=%d symbol=%s",
                    consumer_name,
                    pid,
                    msg.message_id,
                    msg.tick.sequence,
                    msg.tick.symbol,
                )
                if max_messages is not None and processed >= max_messages:
                    running = False
                    break
    finally:
        consumer.close()
        store.close()
        logger.info("worker %s (pid=%d) processed %d messages total", consumer_name, pid, processed)
    return processed
