import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import fakeredis
import pytest

from posttrade.models import Exchange, Tick
from posttrade.queue.redis_stream import StreamConsumer, StreamPublisher
from posttrade.storage.timeseries_store import TimescaleTickStore, get_tick_store


@pytest.mark.asyncio
async def test_ingest_to_queue_to_worker_to_store_end_to_end(postgres_required):
    """Spins up an ephemeral in-memory Redis (fakeredis) and exercises the
    real pipeline components against it: publish (as the ingestor does),
    consume + ack via a consumer group (as a worker does), and write to the
    real local TimescaleDB store — verifying the wiring between every layer
    without depending on a live exchange connection."""
    stream_name = f"test-ticks-{uuid.uuid4().hex[:8]}"
    group_name = f"test-group-{uuid.uuid4().hex[:8]}"
    symbol = f"TEST-{uuid.uuid4().hex[:8]}"

    server = fakeredis.FakeServer()
    async_client = fakeredis.aioredis.FakeRedis(server=server)
    sync_client = fakeredis.FakeRedis(server=server)

    publisher = StreamPublisher(redis_url="redis://fake", stream_name=stream_name, client=async_client)
    consumer = StreamConsumer(
        redis_url="redis://fake",
        stream_name=stream_name,
        group_name=group_name,
        consumer_name="test-consumer",
        client=sync_client,
    )

    now = datetime.now(UTC)
    published_ticks = [
        Tick(
            symbol=symbol,
            sequence=i,
            price=Decimal(100) + i,
            exchange=Exchange.COINBASE,
            channel="ticker",
            exchange_timestamp=now,
            ingest_timestamp=now,
        )
        for i in range(1, 6)
    ]
    for tick in published_ticks:
        msg_id = await publisher.publish(tick)
        assert msg_id

    store = cast(TimescaleTickStore, get_tick_store())
    try:
        consumed = consumer.read(count=10, block_ms=1000)
        assert len(consumed) == 5

        for msg in consumed:
            store.write_tick(msg.tick, worker="test-worker")
            consumer.ack(msg.message_id)

        assert consumer.pending_count() == 0

        df = store.read_ticks_df(symbol, now.replace(microsecond=0), now)
        assert len(df) == 5
        assert sorted(df["sequence"].tolist()) == [1, 2, 3, 4, 5]
    finally:
        with store._conn.cursor() as cur:
            cur.execute("DELETE FROM ticks WHERE symbol = %s;", [symbol])
        store.close()
        consumer.close()
        await publisher.close()


@pytest.mark.asyncio
async def test_worker_crash_pending_entries_reclaimed_by_another_consumer():
    """A worker can read a batch (XREADGROUP) and die before acking it — a
    process crash, an OOM kill, a bad deploy. Those messages must not be
    lost: Redis keeps them in the consumer group's PEL (pending entries
    list) under the dead consumer's name until another consumer reclaims
    them via XAUTOCLAIM. This proves that recovery path actually works,
    not just the happy-path exactly-once delivery the other tests cover."""
    stream_name = f"test-ticks-crash-{uuid.uuid4().hex[:8]}"
    group_name = f"test-group-crash-{uuid.uuid4().hex[:8]}"
    symbol = f"TEST-CRASH-{uuid.uuid4().hex[:8]}"

    server = fakeredis.FakeServer()
    async_client = fakeredis.aioredis.FakeRedis(server=server)
    sync_client_crashed = fakeredis.FakeRedis(server=server)
    sync_client_recovery = fakeredis.FakeRedis(server=server)

    publisher = StreamPublisher(redis_url="redis://fake", stream_name=stream_name, client=async_client)
    crashed_consumer = StreamConsumer(
        redis_url="redis://fake",
        stream_name=stream_name,
        group_name=group_name,
        consumer_name="worker-crashed",
        client=sync_client_crashed,
    )
    recovery_consumer = StreamConsumer(
        redis_url="redis://fake",
        stream_name=stream_name,
        group_name=group_name,
        consumer_name="worker-recovery",
        client=sync_client_recovery,
    )

    now = datetime.now(UTC)
    published_ticks = [
        Tick(
            symbol=symbol,
            sequence=i,
            price=Decimal(300) + i,
            exchange=Exchange.COINBASE,
            channel="ticker",
            exchange_timestamp=now,
            ingest_timestamp=now,
        )
        for i in range(1, 11)
    ]
    for tick in published_ticks:
        await publisher.publish(tick)

    try:
        # worker-crashed reads the full batch — this creates PEL entries —
        # then "dies": no ack, no further calls on this consumer at all.
        crashed_read = crashed_consumer.read(count=100, block_ms=1000)
        assert len(crashed_read) == 10
        assert recovery_consumer.pending_count() == 10  # group-wide PEL, visible from either consumer

        # worker-recovery reclaims everything idle >= 0ms (crashed_read is
        # already idle by the time this runs) — this is the real recovery
        # mechanism, not a simulation of one.
        reclaimed = recovery_consumer.claim_stale(min_idle_ms=0, count=100)
        assert len(reclaimed) == 10
        assert sorted(m.tick.sequence for m in reclaimed) == list(range(1, 11))

        # worker-recovery finishes the work the crashed worker never did
        recovery_consumer.ack_batch([m.message_id for m in reclaimed])
        assert recovery_consumer.pending_count() == 0

        # PEL is genuinely empty now (not just re-claimable) — acked
        # entries are gone, so a further claim attempt finds nothing.
        third_consumer = StreamConsumer(
            redis_url="redis://fake",
            stream_name=stream_name,
            group_name=group_name,
            consumer_name="worker-recovery-2",
            client=fakeredis.FakeRedis(server=server),
        )
        assert third_consumer.claim_stale(min_idle_ms=0, count=100) == []
        third_consumer.close()
    finally:
        crashed_consumer.close()
        recovery_consumer.close()
        await publisher.close()


@pytest.mark.asyncio
async def test_batched_write_and_ack_matches_individual_path(postgres_required):
    """Workers now batch a full XREADGROUP read into one COPY write and one
    XACK call (see worker.py) instead of one round-trip per message. This
    verifies that batched path end to end: same real store, same ephemeral
    Redis pattern, just exercising write_ticks_batch/ack_batch directly."""
    stream_name = f"test-ticks-batch-{uuid.uuid4().hex[:8]}"
    group_name = f"test-group-batch-{uuid.uuid4().hex[:8]}"
    symbol = f"TEST-BATCH-{uuid.uuid4().hex[:8]}"

    server = fakeredis.FakeServer()
    async_client = fakeredis.aioredis.FakeRedis(server=server)
    sync_client = fakeredis.FakeRedis(server=server)

    publisher = StreamPublisher(redis_url="redis://fake", stream_name=stream_name, client=async_client)
    consumer = StreamConsumer(
        redis_url="redis://fake",
        stream_name=stream_name,
        group_name=group_name,
        consumer_name="test-batch-consumer",
        client=sync_client,
    )

    now = datetime.now(UTC)
    published_ticks = [
        Tick(
            symbol=symbol,
            sequence=i,
            price=Decimal(200) + i,
            exchange=Exchange.COINBASE,
            channel="ticker",
            exchange_timestamp=now,
            ingest_timestamp=now,
        )
        for i in range(1, 21)
    ]
    for tick in published_ticks:
        await publisher.publish(tick)

    store = cast(TimescaleTickStore, get_tick_store())
    try:
        consumed = consumer.read(count=100, block_ms=1000)
        assert len(consumed) == 20

        written = store.write_ticks_batch([msg.tick for msg in consumed], worker="test-batch-worker")
        consumer.ack_batch([msg.message_id for msg in consumed])
        assert written == 20

        assert consumer.pending_count() == 0

        df = store.read_ticks_df(symbol, now.replace(microsecond=0), now)
        assert len(df) == 20
        assert sorted(df["sequence"].tolist()) == list(range(1, 21))
        assert (df["worker"] == "test-batch-worker").all()
    finally:
        with store._conn.cursor() as cur:
            cur.execute("DELETE FROM ticks WHERE symbol = %s;", [symbol])
        store.close()
        consumer.close()
        await publisher.close()
