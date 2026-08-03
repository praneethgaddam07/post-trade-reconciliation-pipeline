import logging
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import pairwise

import pandas as pd

from posttrade.models import Break, BreakType
from posttrade.storage.book_store import BookStore
from posttrade.storage.timeseries_store import get_tick_store

logger = logging.getLogger(__name__)


class Reconciler:
    """Joins the simulated book (fills, positions) against observed market
    data (ticks) and flags breaks. Independent of Phase 5 — it only sees
    what's actually in storage, the same as it would against a real book."""

    def __init__(
        self,
        tick_match_window: timedelta = timedelta(seconds=5),
        position_drift_tolerance: Decimal = Decimal("0.000001"),
    ) -> None:
        self.tick_store = get_tick_store()
        self.book_store = BookStore()
        self.tick_match_window = tick_match_window
        self.position_drift_tolerance = position_drift_tolerance

    def reconcile_symbol(self, symbol: str, start: datetime, end: datetime) -> list[Break]:
        fills_df = self.book_store.read_fills_df(symbol, start - self.tick_match_window, end + self.tick_match_window)
        ticks_df = self.tick_store.read_ticks_df(symbol, start - self.tick_match_window, end + self.tick_match_window)
        positions_df = self.book_store.read_positions_df(symbol, start, end)

        breaks: list[Break] = []
        breaks += self._find_unmatched_fills(symbol, fills_df, ticks_df)
        breaks += self._find_position_drift(symbol, fills_df, positions_df)
        breaks += self._find_sequence_gaps(symbol, ticks_df)
        return breaks

    def reconcile_all(self, symbols: list[str], start: datetime, end: datetime) -> list[Break]:
        breaks: list[Break] = []
        for symbol in symbols:
            breaks += self.reconcile_symbol(symbol, start, end)
        return breaks

    def _find_unmatched_fills(self, symbol: str, fills_df: pd.DataFrame, ticks_df: pd.DataFrame) -> list[Break]:
        breaks: list[Break] = []
        if fills_df.empty:
            return breaks
        tick_times = ticks_df["time"] if not ticks_df.empty else pd.Series([], dtype="object")
        for _, fill in fills_df.iterrows():
            fill_time = fill["time"]
            if tick_times.empty:
                nearest_delta = None
            else:
                nearest_delta = (tick_times - fill_time).abs().min()
            if nearest_delta is None or nearest_delta > self.tick_match_window:
                breaks.append(
                    Break(
                        break_type=BreakType.UNMATCHED_FILL,
                        symbol=symbol,
                        detected_at=fill_time,
                        detail=(
                            f"fill {fill['fill_id']} at {fill_time} has no observed tick within "
                            f"{self.tick_match_window}"
                        ),
                        related_fill_id=fill["fill_id"],
                    )
                )
        return breaks

    def _find_position_drift(self, symbol: str, fills_df: pd.DataFrame, positions_df: pd.DataFrame) -> list[Break]:
        breaks: list[Break] = []
        if fills_df.empty or positions_df.empty:
            return breaks

        # Sort by execution-order `sequence`, not `time` — the unmatched_fill
        # anomaly deliberately corrupts timestamp, and using it here would
        # desync the cumulative sum from the book's true fill order.
        fills_sorted = fills_df.sort_values("sequence").copy()
        signed_qty = [
            Decimal(str(qty)) if side == "buy" else -Decimal(str(qty))
            for qty, side in zip(fills_sorted["quantity"], fills_sorted["side"], strict=True)
        ]
        fills_sorted["expected_position"] = pd.Series(signed_qty, index=fills_sorted.index).cumsum()
        expected_by_fill = dict(zip(fills_sorted["fill_id"], fills_sorted["expected_position"]))

        for _, pos in positions_df.iterrows():
            fill_id = pos["last_fill_id"]
            expected = expected_by_fill.get(fill_id)
            if expected is None:
                continue
            recorded = Decimal(str(pos["quantity"]))
            diff = abs(recorded - expected)
            if diff > self.position_drift_tolerance:
                breaks.append(
                    Break(
                        break_type=BreakType.POSITION_DRIFT,
                        symbol=symbol,
                        detected_at=pos["time"],
                        detail=(
                            f"recorded position {recorded} differs from fills-derived position {expected} "
                            f"by {diff} (fill {fill_id})"
                        ),
                        related_fill_id=fill_id,
                    )
                )
        return breaks

    def _find_sequence_gaps(self, symbol: str, ticks_df: pd.DataFrame) -> list[Break]:
        breaks: list[Break] = []
        if ticks_df.empty:
            return breaks
        df = ticks_df.sort_values("sequence")
        seq_time = dict(zip(df["sequence"], df["time"]))
        seqs = sorted(seq_time.keys())
        for prev, curr in pairwise(seqs):
            if curr - prev > 1:
                breaks.append(
                    Break(
                        break_type=BreakType.SEQUENCE_GAP,
                        symbol=symbol,
                        detected_at=seq_time[curr],
                        detail=f"sequence gap for {symbol}: {prev} -> {curr} (missing {curr - prev - 1} tick(s))",
                        related_sequence=prev + 1,
                    )
                )
        return breaks

    def close(self) -> None:
        self.tick_store.close()
        self.book_store.close()


def breaks_to_df(breaks: list[Break]) -> pd.DataFrame:
    if not breaks:
        return pd.DataFrame(columns=["break_type", "symbol", "detected_at", "detail", "related_fill_id", "related_sequence"])
    return pd.DataFrame([b.model_dump() for b in breaks])
