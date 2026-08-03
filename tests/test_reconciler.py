import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from posttrade.models import BreakType, Exchange, Fill, Position, Side, Tick
from posttrade.reconcile.reconciler import Reconciler
from posttrade.storage.book_store import BookStore
from posttrade.storage.timeseries_store import get_tick_store


def _reconciler_stub(tick_match_window=timedelta(seconds=5), position_drift_tolerance=Decimal("0.000001")):
    """A Reconciler that never touches the DB — the `_find_*` methods only
    read self.tick_match_window / self.position_drift_tolerance, so a plain
    namespace is enough to unit test them without live infra."""
    return SimpleNamespace(tick_match_window=tick_match_window, position_drift_tolerance=position_drift_tolerance)


def _ts(offset_seconds=0):
    return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


# --- unmatched_fill -----------------------------------------------------


def test_unmatched_fill_flags_fill_far_from_any_tick():
    r = _reconciler_stub()
    fills_df = pd.DataFrame([{"fill_id": "f1", "time": _ts(1000)}])
    ticks_df = pd.DataFrame([{"time": _ts(0)}, {"time": _ts(1)}])
    breaks = Reconciler._find_unmatched_fills(r, "BTC-USD", fills_df, ticks_df)
    assert len(breaks) == 1
    assert breaks[0].break_type == BreakType.UNMATCHED_FILL
    assert breaks[0].related_fill_id == "f1"


def test_unmatched_fill_not_flagged_when_within_window():
    r = _reconciler_stub()
    fills_df = pd.DataFrame([{"fill_id": "f1", "time": _ts(3)}])
    ticks_df = pd.DataFrame([{"time": _ts(0)}])
    breaks = Reconciler._find_unmatched_fills(r, "BTC-USD", fills_df, ticks_df)
    assert breaks == []


def test_unmatched_fill_with_no_ticks_at_all_is_flagged():
    r = _reconciler_stub()
    fills_df = pd.DataFrame([{"fill_id": "f1", "time": _ts(0)}])
    ticks_df = pd.DataFrame(columns=["time"])
    breaks = Reconciler._find_unmatched_fills(r, "BTC-USD", fills_df, ticks_df)
    assert len(breaks) == 1


def test_unmatched_fill_empty_fills_returns_no_breaks():
    r = _reconciler_stub()
    breaks = Reconciler._find_unmatched_fills(r, "BTC-USD", pd.DataFrame(columns=["fill_id", "time"]), pd.DataFrame())
    assert breaks == []


# --- position_drift -------------------------------------------------------


def test_position_drift_flags_mismatched_recorded_quantity():
    r = _reconciler_stub()
    fills_df = pd.DataFrame(
        [
            {"fill_id": "f1", "sequence": 1, "time": _ts(0), "quantity": "1.0", "side": "buy"},
            {"fill_id": "f2", "sequence": 2, "time": _ts(1), "quantity": "0.5", "side": "sell"},
        ]
    )
    # true position after f2 = 1.0 - 0.5 = 0.5, but recorded says 0.9 -> drift
    positions_df = pd.DataFrame([{"time": _ts(1), "quantity": "0.9", "last_fill_id": "f2"}])
    breaks = Reconciler._find_position_drift(r, "BTC-USD", fills_df, positions_df)
    assert len(breaks) == 1
    assert breaks[0].break_type == BreakType.POSITION_DRIFT
    assert breaks[0].related_fill_id == "f2"


def test_position_drift_not_flagged_when_position_matches_fills():
    r = _reconciler_stub()
    fills_df = pd.DataFrame(
        [
            {"fill_id": "f1", "sequence": 1, "time": _ts(0), "quantity": "1.0", "side": "buy"},
        ]
    )
    positions_df = pd.DataFrame([{"time": _ts(0), "quantity": "1.0", "last_fill_id": "f1"}])
    breaks = Reconciler._find_position_drift(r, "BTC-USD", fills_df, positions_df)
    assert breaks == []


def test_position_drift_sorts_by_sequence_not_corrupted_timestamp():
    """Regression test: an unmatched_fill anomaly deliberately shifts `time`
    far from reality. If position math sorted by `time` instead of the true
    execution-order `sequence`, this fill would desync the cumulative sum
    and create a phantom drift break. It must not."""
    r = _reconciler_stub()
    fills_df = pd.DataFrame(
        [
            {"fill_id": "f1", "sequence": 1, "time": _ts(0), "quantity": "1.0", "side": "buy"},
            # f2 executed second (sequence=2) but its *timestamp* is corrupted to be earliest
            {"fill_id": "f2", "sequence": 2, "time": _ts(-9999), "quantity": "0.5", "side": "buy"},
        ]
    )
    # true cumulative order is f1 then f2: position after f2 = 1.0 + 0.5 = 1.5
    positions_df = pd.DataFrame([{"time": _ts(1), "quantity": "1.5", "last_fill_id": "f2"}])
    breaks = Reconciler._find_position_drift(r, "BTC-USD", fills_df, positions_df)
    assert breaks == []


# --- sequence_gap -----------------------------------------------------------


def test_sequence_gap_detects_missing_sequence():
    r = _reconciler_stub()
    ticks_df = pd.DataFrame([{"sequence": 1, "time": _ts(0)}, {"sequence": 4, "time": _ts(3)}])
    breaks = Reconciler._find_sequence_gaps(r, "BTC-USD", ticks_df)
    assert len(breaks) == 1
    assert breaks[0].break_type == BreakType.SEQUENCE_GAP
    assert breaks[0].related_sequence == 2


def test_sequence_gap_not_flagged_for_consecutive_sequences():
    r = _reconciler_stub()
    ticks_df = pd.DataFrame([{"sequence": i, "time": _ts(i)} for i in range(1, 6)])
    breaks = Reconciler._find_sequence_gaps(r, "BTC-USD", ticks_df)
    assert breaks == []


def test_sequence_gap_empty_ticks_returns_no_breaks():
    r = _reconciler_stub()
    breaks = Reconciler._find_sequence_gaps(r, "BTC-USD", pd.DataFrame(columns=["sequence", "time"]))
    assert breaks == []


# --- against real ground truth (requires local Postgres/Timescale) --------


def test_reconciler_finds_seeded_breaks_with_perfect_precision_and_recall(postgres_required):
    """Seeds one of each anomaly type directly into the real store (no live
    feed, no simulator randomness) and asserts the reconciler finds exactly
    those breaks — a deterministic version of the measurement validated
    manually against the trade book simulator's live-feed output."""
    symbol = f"TEST-{uuid.uuid4().hex[:8]}"
    tick_store = get_tick_store()
    book_store = BookStore()
    base = datetime.now(timezone.utc)

    try:
        # clean ticks 1..5, minus a deliberately deleted sequence 3 (gap)
        for seq in [1, 2, 4, 5]:
            tick_store.write_tick(
                Tick(
                    symbol=symbol,
                    sequence=seq,
                    price=Decimal("100"),
                    exchange=Exchange.COINBASE,
                    channel="ticker",
                    exchange_timestamp=base + timedelta(seconds=seq),
                    ingest_timestamp=base + timedelta(seconds=seq),
                )
            )

        clean_fill = Fill(
            fill_id=f"clean-{uuid.uuid4()}",
            order_id="o1",
            symbol=symbol,
            sequence=1,
            price=Decimal("100"),
            quantity=Decimal("1.0"),
            side=Side.BUY,
            timestamp=base,
        )
        unmatched_fill = Fill(
            fill_id=f"unmatched-{uuid.uuid4()}",
            order_id="o2",
            symbol=symbol,
            sequence=2,
            price=Decimal("100"),
            quantity=Decimal("0.5"),
            side=Side.BUY,
            timestamp=base + timedelta(minutes=20),  # far outside match window
        )
        book_store.write_fill(clean_fill)
        book_store.write_fill(unmatched_fill)

        # true cumulative position after both fills = 1.5, but we record a
        # drifted 1.9 for the second fill
        book_store.write_position(
            Position(symbol=symbol, quantity=Decimal("1.0"), avg_price=Decimal("100"), last_updated=base, last_fill_id=clean_fill.fill_id)
        )
        book_store.write_position(
            Position(
                symbol=symbol,
                quantity=Decimal("1.9"),
                avg_price=Decimal("100"),
                last_updated=base + timedelta(minutes=20),
                last_fill_id=unmatched_fill.fill_id,
            )
        )

        r = Reconciler()
        try:
            breaks = r.reconcile_symbol(symbol, base - timedelta(minutes=1), base + timedelta(minutes=30))
        finally:
            r.close()

        by_type = {}
        for b in breaks:
            by_type.setdefault(b.break_type, []).append(b)

        assert len(by_type.get(BreakType.SEQUENCE_GAP, [])) == 1
        assert by_type[BreakType.SEQUENCE_GAP][0].related_sequence == 3

        assert len(by_type.get(BreakType.UNMATCHED_FILL, [])) == 1
        assert by_type[BreakType.UNMATCHED_FILL][0].related_fill_id == unmatched_fill.fill_id

        assert len(by_type.get(BreakType.POSITION_DRIFT, [])) == 1
        assert by_type[BreakType.POSITION_DRIFT][0].related_fill_id == unmatched_fill.fill_id

        assert len(breaks) == 3, f"expected exactly 3 breaks (one per planted type), got {len(breaks)}: {breaks}"
    finally:
        with tick_store._conn.cursor() as cur:
            cur.execute("DELETE FROM ticks WHERE symbol = %s;", [symbol])
        with book_store._conn.cursor() as cur:
            cur.execute("DELETE FROM fills WHERE symbol = %s;", [symbol])
            cur.execute("DELETE FROM positions WHERE symbol = %s;", [symbol])
        tick_store.close()
        book_store.close()
