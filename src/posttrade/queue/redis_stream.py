import logging
from dataclasses import dataclass
from typing import Any, cast

import redis
import redis.asyncio as aioredis

from posttrade.models import Tick

# redis-py's stubs type stream-read responses as a broad `ResponseT` union
# (covers every possible command reply), not the nested structure XREADGROUP
# / XAUTOCLAIM actually return. These narrow it to what's actually iterated
# below — keys may be bytes or str depending on the client's
# decode_responses setting, which is why field lookups below check both.
_StreamEntries = list[tuple[Any, dict[Any, Any]]]
_StreamReadResponse = list[tuple[Any, _StreamEntries]]
_AutoclaimResponse = tuple[Any, _StreamEntries, list[Any]]

logger = logging.getLogger(__name__)

_FIELD = "data"


def _create_group_sync(client: redis.Redis, stream_name: str, group_name: str) -> None:
    try:
        client.xgroup_create(name=stream_name, groupname=group_name, id="0", mkstream=True)
        logger.info("created consumer group %s on stream %s", group_name, stream_name)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _create_group_async(client: aioredis.Redis, stream_name: str, group_name: str) -> None:
    try:
        await client.xgroup_create(name=stream_name, groupname=group_name, id="0", mkstream=True)
        logger.info("created consumer group %s on stream %s", group_name, stream_name)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


@dataclass
class ConsumedMessage:
    message_id: str
    tick: Tick


def _parse_entries(entries: _StreamEntries) -> list[ConsumedMessage]:
    messages: list[ConsumedMessage] = []
    for msg_id, fields in entries:
        raw = fields.get(b"data") or fields.get(_FIELD)
        if raw is None:
            continue
        tick = Tick.model_validate_json(raw)
        mid = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        messages.append(ConsumedMessage(message_id=mid, tick=tick))
    return messages


class StreamPublisher:
    """Async publish side of the tick stream. Used by the ingestor — nothing
    above this module should call `.xadd` on a raw Redis client directly."""

    def __init__(
        self,
        redis_url: str,
        stream_name: str,
        group_name: str | None = None,
        client: aioredis.Redis | None = None,
    ) -> None:
        self.stream_name = stream_name
        self.group_name = group_name
        self._redis = client if client is not None else aioredis.from_url(redis_url)

    async def ensure_group(self) -> None:
        if self.group_name:
            await _create_group_async(self._redis, self.stream_name, self.group_name)

    async def publish(self, tick: Tick) -> str:
        msg_id = await self._redis.xadd(self.stream_name, {_FIELD: tick.model_dump_json()})
        return msg_id.decode() if isinstance(msg_id, bytes) else msg_id

    async def publish_batch(self, ticks: list[Tick]) -> list[str]:
        """Pipelines N XADD calls into a single network round-trip instead of
        awaiting each one sequentially — this is what lets the load harness's
        publisher keep up with high target rates instead of being limited by
        per-call round-trip latency."""
        if not ticks:
            return []
        pipe = self._redis.pipeline()
        for tick in ticks:
            pipe.xadd(self.stream_name, {_FIELD: tick.model_dump_json()})
        results = await pipe.execute()
        return [r.decode() if isinstance(r, bytes) else r for r in results]

    async def close(self) -> None:
        await self._redis.aclose()


class StreamConsumer:
    """Sync consume side of the tick stream, meant to run inside one worker
    process at a time (Phase 3 spawns several of these against the same
    consumer group so the OS — not asyncio — does the parallelism)."""

    def __init__(
        self,
        redis_url: str,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        client: redis.Redis | None = None,
    ) -> None:
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self._redis = client if client is not None else redis.Redis.from_url(redis_url)
        _create_group_sync(self._redis, self.stream_name, self.group_name)

    def read(self, count: int = 10, block_ms: int = 5000) -> list[ConsumedMessage]:
        raw_response = self._redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={self.stream_name: ">"},
            count=count,
            block=block_ms,
        )
        if not raw_response:
            return []
        response = cast(_StreamReadResponse, raw_response)
        messages: list[ConsumedMessage] = []
        for _stream_name, entries in response:
            messages.extend(_parse_entries(entries))
        return messages

    def claim_stale(self, min_idle_ms: int = 0, count: int = 100) -> list[ConsumedMessage]:
        """Reclaims PENDING entries idle for at least `min_idle_ms` under
        this consumer's name — this is the recovery path when a worker
        reads a batch and crashes before acking it: the messages aren't
        lost, they sit in the group's PEL until another consumer calls this
        (via XAUTOCLAIM) to take ownership and finish the work. Redis's
        consumer-group PEL is what makes that recovery possible at all."""
        raw_response = self._redis.xautoclaim(
            self.stream_name,
            self.group_name,
            self.consumer_name,
            min_idle_time=min_idle_ms,
            count=count,
        )
        _next_cursor, entries, _deleted = cast(_AutoclaimResponse, raw_response)
        return _parse_entries(entries)

    def ack(self, message_id: str) -> None:
        self._redis.xack(self.stream_name, self.group_name, message_id)

    def ack_batch(self, message_ids: list[str]) -> None:
        if not message_ids:
            return
        self._redis.xack(self.stream_name, self.group_name, *message_ids)

    def pending_count(self) -> int:
        summary = self._redis.xpending(self.stream_name, self.group_name)
        return int(summary["pending"]) if summary else 0

    def close(self) -> None:
        self._redis.close()
