from datetime import datetime
from typing import Any

import pandas as pd
import psycopg

from posttrade.config import settings
from posttrade.models import Fill, Position

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    price NUMERIC NOT NULL,
    quantity NUMERIC NOT NULL,
    side TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    is_planted_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_type TEXT
);
CREATE TABLE IF NOT EXISTS positions (
    symbol TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    quantity NUMERIC NOT NULL,
    avg_price NUMERIC NOT NULL,
    last_fill_id TEXT
);
CREATE TABLE IF NOT EXISTS planted_anomalies (
    id SERIAL PRIMARY KEY,
    anomaly_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    planted_at TIMESTAMPTZ NOT NULL,
    detail TEXT NOT NULL,
    related_fill_id TEXT,
    related_sequence_start INTEGER,
    related_sequence_end INTEGER
);
CREATE INDEX IF NOT EXISTS fills_symbol_time_idx ON fills (symbol, time);
CREATE INDEX IF NOT EXISTS positions_symbol_time_idx ON positions (symbol, time);
"""


class BookStore:
    """Persists the trade book simulator's fills, position snapshots, and
    the ground-truth log of anomalies it deliberately planted (Phase 5)."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.timescale_dsn
        self._conn = psycopg.connect(self.dsn, autocommit=True)
        with self._conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)

    def write_fill(self, fill: Fill) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fills (fill_id, order_id, symbol, sequence, price, quantity, side, time,
                                    is_planted_anomaly, anomaly_type)
                VALUES (%(fill_id)s, %(order_id)s, %(symbol)s, %(sequence)s, %(price)s, %(quantity)s, %(side)s,
                        %(timestamp)s, %(is_planted_anomaly)s, %(anomaly_type)s)
                ON CONFLICT (fill_id) DO NOTHING;
                """,
                {
                    "fill_id": fill.fill_id,
                    "order_id": fill.order_id,
                    "symbol": fill.symbol,
                    "sequence": fill.sequence,
                    "price": fill.price,
                    "quantity": fill.quantity,
                    "side": fill.side.value,
                    "timestamp": fill.timestamp,
                    "is_planted_anomaly": fill.is_planted_anomaly,
                    "anomaly_type": fill.anomaly_type,
                },
            )

    def write_position(self, position: Position) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO positions (symbol, time, quantity, avg_price, last_fill_id)
                VALUES (%(symbol)s, %(time)s, %(quantity)s, %(avg_price)s, %(last_fill_id)s);
                """,
                {
                    "symbol": position.symbol,
                    "time": position.last_updated,
                    "quantity": position.quantity,
                    "avg_price": position.avg_price,
                    "last_fill_id": position.last_fill_id,
                },
            )

    def log_planted_anomaly(
        self,
        anomaly_type: str,
        symbol: str,
        planted_at: datetime,
        detail: str,
        related_fill_id: str | None = None,
        related_sequence_start: int | None = None,
        related_sequence_end: int | None = None,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO planted_anomalies
                    (anomaly_type, symbol, planted_at, detail, related_fill_id,
                     related_sequence_start, related_sequence_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                [
                    anomaly_type,
                    symbol,
                    planted_at,
                    detail,
                    related_fill_id,
                    related_sequence_start,
                    related_sequence_end,
                ],
            )

    def _df(self, query: str, params: dict[str, Any]) -> pd.DataFrame:
        with self._conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description or []]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)

    def read_fills_df(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        return self._df(
            "SELECT * FROM fills WHERE symbol = %(symbol)s AND time BETWEEN %(start)s AND %(end)s ORDER BY time;",
            {"symbol": symbol, "start": start, "end": end},
        )

    def read_positions_df(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        return self._df(
            "SELECT * FROM positions WHERE symbol = %(symbol)s AND time BETWEEN %(start)s AND %(end)s ORDER BY time;",
            {"symbol": symbol, "start": start, "end": end},
        )

    def read_planted_anomalies_df(self) -> pd.DataFrame:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM planted_anomalies ORDER BY planted_at;")
            columns = [desc[0] for desc in cur.description or []]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)

    def close(self) -> None:
        self._conn.close()
