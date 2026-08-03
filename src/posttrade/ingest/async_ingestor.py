import asyncio
import itertools
import json
import logging
import time
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import websockets

from posttrade.config import settings
from posttrade.models import Exchange, Side, Tick
from posttrade.observability.metrics import start_metrics_server, ticks_ingested_total
from posttrade.queue.redis_stream import StreamPublisher

logger = logging.getLogger(__name__)

_RELEVANT_TYPES = {"ticker", "match", "last_match"}


def _parse_exchange_timestamp(raw: str) -> datetime:
    return datetime.fromisoformat(raw)


class AsyncIngestor:
    """Connects to a public exchange WebSocket feed, normalizes messages into
    typed `Tick` objects, and publishes them onto a Redis stream via XADD.

    Sequence numbers are assigned here, per symbol, independent of whatever
    sequence field the exchange sends — this is *our* gap-detection axis,
    not theirs.
    """

    def __init__(
        self,
        ws_url: str | None = None,
        symbols: list[str] | None = None,
        redis_url: str | None = None,
        stream_name: str | None = None,
        throughput_log_interval: float = 5.0,
    ) -> None:
        self.ws_url = ws_url or settings.coinbase_ws_url
        self.symbols = symbols or settings.symbols
        self.redis_url = redis_url or settings.redis_url
        self.stream_name = stream_name or settings.redis_stream_name
        self.throughput_log_interval = throughput_log_interval

        self._sequences: dict[str, itertools.count[int]] = defaultdict(lambda: itertools.count(1))
        self._publisher: StreamPublisher | None = None
        self._msg_count = 0
        self._window_start = time.monotonic()
        self.total_published = 0

    async def run(self, max_reconnects: int | None = None) -> None:
        if settings.metrics_enabled:
            start_metrics_server(settings.metrics_port_ingestor)
        self._publisher = StreamPublisher(self.redis_url, self.stream_name)
        backoff = 1.0
        max_backoff = 30.0
        attempts = 0
        try:
            while max_reconnects is None or attempts <= max_reconnects:
                try:
                    async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                        await self._subscribe(ws)
                        backoff = 1.0
                        async for raw in ws:
                            await self._handle_message(raw)
                except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                    attempts += 1
                    logger.warning("ingestor disconnected (%s), reconnecting in %.1fs", exc, backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        finally:
            if self._publisher is not None:
                await self._publisher.close()

    async def _subscribe(self, ws: websockets.ClientConnection) -> None:
        msg = {
            "type": "subscribe",
            "product_ids": self.symbols,
            "channels": ["ticker", "matches", "heartbeat"],
        }
        await ws.send(json.dumps(msg))
        logger.info("subscribed to %s on %s", self.symbols, self.ws_url)

    async def _handle_message(self, raw: str | bytes) -> None:
        data = json.loads(raw)
        msg_type = data.get("type")
        if msg_type not in _RELEVANT_TYPES:
            return
        tick = self._normalize(data, msg_type)
        if tick is None:
            return
        await self._publish(tick)
        self._log_throughput()

    def _normalize(self, data: dict[str, Any], msg_type: str) -> Tick | None:
        symbol = data.get("product_id")
        price_raw = data.get("price")
        if symbol is None or price_raw is None:
            return None
        try:
            price = Decimal(price_raw)
        except InvalidOperation:
            return None

        size_raw = data.get("size") or data.get("last_size")
        size: Decimal | None = None
        if size_raw is not None:
            try:
                size = Decimal(size_raw)
            except InvalidOperation:
                size = None

        side_raw = data.get("side")
        side = Side(side_raw) if side_raw in ("buy", "sell") else None

        exch_ts_raw = data.get("time")
        exchange_timestamp = (
            _parse_exchange_timestamp(exch_ts_raw) if exch_ts_raw else datetime.now(UTC)
        )

        sequence = next(self._sequences[symbol])
        channel = "ticker" if msg_type == "ticker" else "matches"

        return Tick(
            symbol=symbol,
            sequence=sequence,
            price=price,
            size=size,
            side=side,
            exchange=Exchange.COINBASE,
            channel=channel,
            exchange_timestamp=exchange_timestamp,
            ingest_timestamp=datetime.now(UTC),
        )

    async def _publish(self, tick: Tick) -> None:
        assert self._publisher is not None
        await self._publisher.publish(tick)
        self.total_published += 1
        ticks_ingested_total.labels(symbol=tick.symbol, channel=tick.channel).inc()

    def _log_throughput(self) -> None:
        self._msg_count += 1
        elapsed = time.monotonic() - self._window_start
        if elapsed >= self.throughput_log_interval:
            rate = self._msg_count / elapsed
            logger.info(
                "ingest throughput",
                extra={
                    "msg_per_sec": round(rate, 1),
                    "window_msgs": self._msg_count,
                    "window_seconds": round(elapsed, 1),
                    "total_published": self.total_published,
                },
            )
            self._msg_count = 0
            self._window_start = time.monotonic()


if __name__ == "__main__":
    from posttrade.observability.logging_config import configure_structured_logging

    configure_structured_logging()
    asyncio.run(AsyncIngestor().run())
