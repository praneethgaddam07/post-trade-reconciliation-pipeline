from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from posttrade.models import Break, BreakType, Exchange, Fill, Position, Side, Tick


def _now():
    return datetime.now(UTC)


def test_tick_is_immutable():
    tick = Tick(
        symbol="BTC-USD",
        sequence=1,
        price=Decimal("50000.12"),
        exchange=Exchange.COINBASE,
        channel="ticker",
        exchange_timestamp=_now(),
        ingest_timestamp=_now(),
    )
    with pytest.raises(ValidationError):
        tick.sequence = 2


def test_tick_requires_price_and_sequence():
    with pytest.raises(ValidationError):
        Tick(  # type: ignore[call-arg]  # intentionally omitting required fields to test runtime validation
            symbol="BTC-USD",
            exchange=Exchange.COINBASE,
            channel="ticker",
            exchange_timestamp=_now(),
            ingest_timestamp=_now(),
        )


def test_fill_default_not_anomalous():
    fill = Fill(
        fill_id="f1",
        order_id="o1",
        symbol="BTC-USD",
        sequence=1,
        price=Decimal(100),
        quantity=Decimal("0.01"),
        side=Side.BUY,
        timestamp=_now(),
    )
    assert fill.is_planted_anomaly is False
    assert fill.anomaly_type is None


def test_fill_is_immutable():
    fill = Fill(
        fill_id="f1",
        order_id="o1",
        symbol="BTC-USD",
        sequence=1,
        price=Decimal(100),
        quantity=Decimal("0.01"),
        side=Side.BUY,
        timestamp=_now(),
    )
    with pytest.raises(ValidationError):
        fill.quantity = Decimal("0.02")


def test_break_type_enum_values():
    assert BreakType.UNMATCHED_FILL.value == "unmatched_fill"
    assert BreakType.POSITION_DRIFT.value == "position_drift"
    assert BreakType.SEQUENCE_GAP.value == "sequence_gap"


def test_break_construction():
    b = Break(
        break_type=BreakType.SEQUENCE_GAP,
        symbol="ETH-USD",
        detected_at=_now(),
        detail="gap",
        related_sequence=42,
    )
    assert b.related_fill_id is None
    assert b.related_sequence == 42


def test_position_accepts_negative_quantity_for_short():
    pos = Position(symbol="ETH-USD", quantity=Decimal("-1.5"), avg_price=Decimal(1800), last_updated=_now())
    assert pos.quantity == Decimal("-1.5")
