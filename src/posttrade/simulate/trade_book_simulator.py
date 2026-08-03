import itertools
import logging
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from posttrade.config import settings
from posttrade.models import Fill, Position, Side, Tick
from posttrade.queue.redis_stream import StreamConsumer
from posttrade.storage.book_store import BookStore
from posttrade.storage.timeseries_store import get_tick_store

logger = logging.getLogger(__name__)

ANOMALY_TYPES = ("unmatched_fill", "position_drift", "sequence_gap")


class TradeBookSimulator:
    """Reacts to the live tick stream with a toy momentum strategy and
    generates synthetic Fill records against a simulated book.

    Deliberately plants a known rate of anomalies and logs each one as
    ground truth in `planted_anomalies` — this is what turns Phase 7's
    reconciler tests into a measured precision/recall number instead of a
    guess. All three anomaly types are physically real in the data, not
    just claimed:
      - unmatched_fill: fill timestamp shifted minutes away from any tick
      - position_drift: the recorded book position is nudged away from
        what summing the fills would actually give, and stays drifted
      - sequence_gap: previously-stored tick rows are deleted for real, so
        the reconciler's sequence scan has something genuine to find
    """

    def __init__(
        self,
        consumer_name: str = "simulator",
        group_name: str = "simulator-group",
        trade_probability: float = 0.25,
        anomaly_rate: float = 0.15,
        rng: random.Random | None = None,
    ) -> None:
        self.consumer = StreamConsumer(settings.redis_url, settings.redis_stream_name, group_name, consumer_name)
        self.tick_store = get_tick_store()
        self.book_store = BookStore()
        self.trade_probability = trade_probability
        self.anomaly_rate = anomaly_rate
        self.rng = rng or random.Random()

        self._true_position: dict[str, Decimal] = {}
        self._avg_price: dict[str, Decimal] = {}
        self._last_price: dict[str, Decimal] = {}
        self._recent_sequences: dict[str, list[int]] = {}
        self._fill_sequences: dict[str, itertools.count[int]] = {}
        self._order_ids = itertools.count(1)

        self.fills_emitted = 0
        self.anomalies_planted: dict[str, int] = dict.fromkeys(ANOMALY_TYPES, 0)

    def run(self, max_fills: int | None = None, max_idle_reads: int | None = None) -> int:
        idle = 0
        try:
            while True:
                messages = self.consumer.read(count=20, block_ms=2000)
                if not messages:
                    idle += 1
                    if max_idle_reads is not None and idle >= max_idle_reads:
                        logger.info("simulator idle for %d reads, stopping", idle)
                        break
                    continue
                idle = 0
                for msg in messages:
                    self.consumer.ack(msg.message_id)
                    self._track_sequence(msg.tick)
                    if self.rng.random() < self.trade_probability:
                        self._generate_fill(msg.tick)
                        if max_fills is not None and self.fills_emitted >= max_fills:
                            return self.fills_emitted
        finally:
            self.consumer.close()
            self.tick_store.close()
            self.book_store.close()
        return self.fills_emitted

    def _track_sequence(self, tick: Tick) -> None:
        seqs = self._recent_sequences.setdefault(tick.symbol, [])
        seqs.append(tick.sequence)
        if len(seqs) > 50:
            del seqs[: len(seqs) - 50]

    def _generate_fill(self, tick: Tick) -> None:
        prior_price = self._last_price.get(tick.symbol, tick.price)
        side = Side.BUY if tick.price >= prior_price else Side.SELL
        self._last_price[tick.symbol] = tick.price

        quantity = Decimal(str(round(self.rng.uniform(0.001, 0.05), 6)))
        order_id = f"sim-order-{next(self._order_ids)}"
        fill_id = str(uuid.uuid4())
        sequence = next(self._fill_sequences.setdefault(tick.symbol, itertools.count(1)))

        anomaly_type = self.rng.choice(ANOMALY_TYPES) if self.rng.random() < self.anomaly_rate else None
        timestamp = tick.exchange_timestamp
        if anomaly_type == "unmatched_fill":
            timestamp = tick.exchange_timestamp + timedelta(minutes=self.rng.choice([-15, 15]))

        fill = Fill(
            fill_id=fill_id,
            order_id=order_id,
            symbol=tick.symbol,
            sequence=sequence,
            price=tick.price,
            quantity=quantity,
            side=side,
            timestamp=timestamp,
            is_planted_anomaly=anomaly_type is not None,
            anomaly_type=anomaly_type,
        )
        self.book_store.write_fill(fill)
        self._apply_fill(fill)
        self.fills_emitted += 1

        if anomaly_type == "unmatched_fill":
            self.book_store.log_planted_anomaly(
                anomaly_type,
                tick.symbol,
                tick.exchange_timestamp,
                f"fill {fill_id} timestamped {timestamp.isoformat()}, far from any observed tick "
                f"(nearest real tick at {tick.exchange_timestamp.isoformat()})",
                related_fill_id=fill_id,
            )
            self.anomalies_planted["unmatched_fill"] += 1
            logger.info("planted unmatched_fill: %s", fill_id)

        elif anomaly_type == "position_drift":
            drift = Decimal(str(round(self.rng.uniform(0.01, 0.1), 6))) * self.rng.choice([Decimal(1), Decimal(-1)])
            true_qty = self._true_position[tick.symbol]
            drifted_qty = true_qty + drift
            self.book_store.log_planted_anomaly(
                anomaly_type,
                tick.symbol,
                tick.exchange_timestamp,
                f"recorded position drifted by {drift} after fill {fill_id}; true={true_qty} recorded={drifted_qty}",
                related_fill_id=fill_id,
            )
            self.anomalies_planted["position_drift"] += 1
            logger.info("planted position_drift: %s drift=%s", fill_id, drift)
            self._write_position(tick.symbol, tick.exchange_timestamp, fill_id, quantity_override=drifted_qty)
        else:
            self._write_position(tick.symbol, tick.exchange_timestamp, fill_id)

        if anomaly_type == "sequence_gap":
            self._plant_sequence_gap(tick.symbol, tick.exchange_timestamp)

    def _apply_fill(self, fill: Fill) -> None:
        symbol = fill.symbol
        delta = fill.quantity if fill.side == Side.BUY else -fill.quantity
        self._true_position[symbol] = self._true_position.get(symbol, Decimal(0)) + delta
        self._avg_price[symbol] = fill.price

    def _write_position(
        self, symbol: str, timestamp: datetime, fill_id: str, quantity_override: Decimal | None = None
    ) -> None:
        quantity = quantity_override if quantity_override is not None else self._true_position[symbol]
        position = Position(
            symbol=symbol,
            quantity=quantity,
            avg_price=self._avg_price.get(symbol, Decimal(0)),
            last_updated=timestamp,
            last_fill_id=fill_id,
        )
        self.book_store.write_position(position)

    def _plant_sequence_gap(self, symbol: str, planted_at: datetime) -> None:
        seqs = sorted(self._recent_sequences.get(symbol, []))
        if len(seqs) < 6:
            return
        start_idx = self.rng.randint(0, len(seqs) - 4)
        gap_seqs = seqs[start_idx : start_idx + self.rng.randint(1, 3)]
        deleted = self.tick_store.delete_sequences(symbol, gap_seqs)
        if deleted:
            self.book_store.log_planted_anomaly(
                "sequence_gap",
                symbol,
                planted_at,
                f"deleted stored ticks with sequences {deleted} for {symbol}",
                related_sequence_start=min(deleted),
                related_sequence_end=max(deleted),
            )
            self.anomalies_planted["sequence_gap"] += 1
            logger.info("planted sequence_gap: %s sequences=%s", symbol, deleted)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [simulator] %(levelname)s %(message)s")
    sim = TradeBookSimulator()
    total = sim.run(max_idle_reads=5)
    logger.info("simulator finished: %d fills emitted, anomalies=%s", total, sim.anomalies_planted)
