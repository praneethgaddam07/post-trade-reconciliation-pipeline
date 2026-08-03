import uuid
from datetime import datetime, timezone
from decimal import Decimal

import fakeredis
import pytest

from posttrade.models import Exchange, Tick
from posttrade.queue.redis_stream import StreamConsumer, StreamPublisher
from posttrade.storage.timeseries_store import get_tick_store


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

    now = datetime.now(timezone.utc)
    published_ticks = [
        Tick(
            symbol=symbol,
            sequence=i,
            price=Decimal("100") + i,
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

    store = get_tick_store()
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
