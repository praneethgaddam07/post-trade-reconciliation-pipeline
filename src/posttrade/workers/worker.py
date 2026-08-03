import logging
import os
import signal
from types import FrameType

from posttrade.config import settings
from posttrade.observability.logging_config import configure_structured_logging
from posttrade.observability.metrics import (
    batch_write_seconds,
    consumer_lag,
    start_metrics_server,
    ticks_processed_total,
)
from posttrade.queue.redis_stream import StreamConsumer
from posttrade.storage.timeseries_store import get_tick_store

logger = logging.getLogger(__name__)


def run_worker(
    consumer_name: str,
    max_messages: int | None = None,
    max_idle_reads: int | None = None,
    stream_name: str | None = None,
    group_name: str | None = None,
    read_batch_size: int = 500,
    metrics_port: int | None = None,
) -> int:
    """Runs one consumer loop against the shared consumer group.

    Meant to be the entry point of a separate OS process (see
    `worker_pool.run_worker_pool`) — several of these run concurrently
    against the *same* Redis consumer group, so each message is delivered
    to exactly one of them. That's what makes this genuinely distributed
    rather than one script pretending to be several.

    Each XREADGROUP batch is written to the store in a single COPY call and
    acked in a single XACK call — one round-trip per batch instead of one
    per message, which is what "batched writes" means here.

    `max_messages` / `max_idle_reads` exist so a run can terminate
    deterministically for load tests and demos instead of blocking forever.
    `stream_name` / `group_name` default to settings but are overridable so
    the load harness can point workers at an isolated test stream.
    `metrics_port`, if given, exposes this worker's counters on its own
    Prometheus scrape endpoint — each worker gets a distinct port so Grafana
    can chart throughput per worker, not just an aggregate.
    """
    stream_name = stream_name or settings.redis_stream_name
    group_name = group_name or settings.redis_consumer_group
    configure_structured_logging()
    if settings.metrics_enabled and metrics_port is not None:
        start_metrics_server(metrics_port)

    consumer = StreamConsumer(settings.redis_url, stream_name, group_name, consumer_name)
    store = get_tick_store()
    processed = 0
    idle_reads = 0
    pid = os.getpid()
    logger.info("worker starting", extra={"worker": consumer_name, "pid": pid})

    running = True

    def _stop(signum: int, frame: FrameType | None) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    try:
        while running:
            messages = consumer.read(count=read_batch_size, block_ms=2000)
            if not messages:
                idle_reads += 1
                if max_idle_reads is not None and idle_reads >= max_idle_reads:
                    logger.info(
                        "worker idle, stopping", extra={"worker": consumer_name, "pid": pid, "idle_reads": idle_reads}
                    )
                    break
                continue
            idle_reads = 0

            batch = messages
            if max_messages is not None and processed + len(batch) > max_messages:
                batch = batch[: max_messages - processed]

            ticks = [msg.tick for msg in batch]
            with batch_write_seconds.labels(worker=consumer_name).time():
                store.write_ticks_batch(ticks, worker=consumer_name)
            consumer.ack_batch([msg.message_id for msg in batch])
            processed += len(batch)

            ticks_processed_total.labels(worker=consumer_name).inc(len(batch))
            consumer_lag.labels(group=group_name).set(consumer.pending_count())

            logger.info(
                "batch-wrote ticks",
                extra={
                    "worker": consumer_name,
                    "pid": pid,
                    "batch_size": len(batch),
                    "seq_start": batch[0].tick.sequence,
                    "seq_end": batch[-1].tick.sequence,
                    "symbol": batch[-1].tick.symbol,
                },
            )

            if max_messages is not None and processed >= max_messages:
                running = False
    finally:
        consumer.close()
        store.close()
        logger.info("worker stopped", extra={"worker": consumer_name, "pid": pid, "processed_total": processed})
    return processed
