import logging
import time
import uuid
from datetime import UTC, datetime

from dataeng.bronze.s3_client import ensure_bucket, get_s3_client
from dataeng.config import settings

logger = logging.getLogger(__name__)


class BronzeWriter:
    """Lands raw exchange JSON messages as newline-delimited JSON objects in
    the bronze bucket, partitioned by symbol/date/hour (Hive-style keys) —
    the immutable log of record. This is what makes a Kappa-style batch
    replay possible later: it replays the exact bytes the exchange sent,
    not a reconstruction from already-normalized data.

    Buffers per partition and flushes on a size or time threshold rather
    than writing one object per message — a bronze lander doing per-message
    PUTs would be both slow and a real S3 anti-pattern (too many small
    objects hurts both write throughput and later scan performance).
    """

    def __init__(
        self,
        bucket: str | None = None,
        flush_max_lines: int = 200,
        flush_max_seconds: float = 10.0,
    ) -> None:
        self.bucket = bucket or settings.bronze_bucket
        self.flush_max_lines = flush_max_lines
        self.flush_max_seconds = flush_max_seconds
        self._client = get_s3_client()
        ensure_bucket(self._client, self.bucket)
        self._buffers: dict[str, list[str]] = {}
        self._buffer_started_at: dict[str, float] = {}
        self.total_lines_flushed = 0
        self.total_objects_flushed = 0

    def _partition_key(self, symbol: str, received_at: datetime) -> str:
        return f"ticks/symbol={symbol}/date={received_at:%Y-%m-%d}/hour={received_at:%H}"

    def append(self, symbol: str, raw_message: str, received_at: datetime | None = None) -> None:
        received_at = received_at or datetime.now(UTC)
        partition = self._partition_key(symbol, received_at)
        buf = self._buffers.setdefault(partition, [])
        if not buf:
            self._buffer_started_at[partition] = time.monotonic()
        buf.append(raw_message)
        if len(buf) >= self.flush_max_lines:
            self.flush(partition)

    def flush_due(self) -> None:
        """Call periodically (e.g. from the ingestor's own loop) to flush
        partitions aged past flush_max_seconds even if they never hit the
        line-count threshold — otherwise a low-volume symbol could sit
        buffered in memory indefinitely."""
        now = time.monotonic()
        due = [
            p
            for p, started in self._buffer_started_at.items()
            if now - started >= self.flush_max_seconds and self._buffers.get(p)
        ]
        for partition in due:
            self.flush(partition)

    def flush(self, partition: str | None = None) -> None:
        partitions = [partition] if partition else list(self._buffers.keys())
        for p in partitions:
            lines = self._buffers.get(p)
            if not lines:
                continue
            body = ("\n".join(lines) + "\n").encode("utf-8")
            key = f"{p}/{uuid.uuid4().hex}.ndjson"
            self._client.put_object(Bucket=self.bucket, Key=key, Body=body)
            self.total_lines_flushed += len(lines)
            self.total_objects_flushed += 1
            logger.info("bronze flush", extra={"bucket": self.bucket, "key": key, "lines": len(lines)})
            self._buffers[p] = []
            self._buffer_started_at.pop(p, None)

    def flush_all(self) -> None:
        self.flush(None)
