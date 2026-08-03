import logging
from datetime import datetime
from typing import Protocol

import pandas as pd
import psycopg

from posttrade.config import settings
from posttrade.models import Tick

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    price NUMERIC NOT NULL,
    size NUMERIC,
    side TEXT,
    exchange TEXT NOT NULL,
    channel TEXT NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL,
    worker TEXT
);
"""

_CREATE_HYPERTABLE_SQL = "SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE);"

_CREATE_INDEX_SQL = "CREATE INDEX IF NOT EXISTS ticks_symbol_time_idx ON ticks (symbol, time DESC);"

_INSERT_SQL = """
INSERT INTO ticks (time, symbol, sequence, price, size, side, exchange, channel, ingest_time, worker)
VALUES (%(exchange_timestamp)s, %(symbol)s, %(sequence)s, %(price)s, %(size)s,
        %(side)s, %(exchange)s, %(channel)s, %(ingest_timestamp)s, %(worker)s)
"""

_COPY_SQL = "COPY ticks (time, symbol, sequence, price, size, side, exchange, channel, ingest_time, worker) FROM STDIN"


class TickStore(Protocol):
    def write_tick(self, tick: Tick, worker: str | None = None) -> None: ...
    def write_ticks_batch(self, ticks: list[Tick], worker: str | None = None) -> int: ...
    def count(self) -> int: ...
    def delete_sequences(self, symbol: str, sequences: list[int]) -> list[int]: ...
    def read_ticks_df(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame: ...
    def close(self) -> None: ...


class TimescaleTickStore:
    """Write/read path backed by TimescaleDB. Default store backend — see
    DuckDBTickStore for the fallback if Timescale can't be stood up."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.timescale_dsn
        self._conn = psycopg.connect(self.dsn, autocommit=True)
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
            try:
                cur.execute(_CREATE_HYPERTABLE_SQL)
            except psycopg.Error as exc:
                logger.warning("could not create hypertable (%s); continuing on plain table", exc)
            cur.execute(_CREATE_INDEX_SQL)

    def write_tick(self, tick: Tick, worker: str | None = None) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                _INSERT_SQL,
                {
                    "exchange_timestamp": tick.exchange_timestamp,
                    "symbol": tick.symbol,
                    "sequence": tick.sequence,
                    "price": tick.price,
                    "size": tick.size,
                    "side": tick.side.value if tick.side else None,
                    "exchange": tick.exchange.value,
                    "channel": tick.channel,
                    "ingest_timestamp": tick.ingest_timestamp,
                    "worker": worker,
                },
            )

    def write_ticks_batch(self, ticks: list[Tick], worker: str | None = None) -> int:
        """Writes a batch of ticks in a single COPY round-trip instead of one
        autocommitted INSERT per tick — the single-row insert path is a real
        bottleneck under load (see load test results)."""
        if not ticks:
            return 0
        with self._conn.cursor() as cur, cur.copy(_COPY_SQL) as copy:
            for tick in ticks:
                copy.write_row(
                    (
                        tick.exchange_timestamp,
                        tick.symbol,
                        tick.sequence,
                        tick.price,
                        tick.size,
                        tick.side.value if tick.side else None,
                        tick.exchange.value,
                        tick.channel,
                        tick.ingest_timestamp,
                        worker,
                    )
                )
        return len(ticks)

    def count(self) -> int:
        with self._conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ticks;")
            row = cur.fetchone()
            return row[0] if row else 0

    def delete_sequences(self, symbol: str, sequences: list[int]) -> list[int]:
        """Deletes stored ticks with the given (symbol, sequence) pairs and
        returns the sequences actually removed. Used by the trade book
        simulator to plant real, verifiable sequence gaps rather than just
        claiming one occurred."""
        if not sequences:
            return []
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ticks WHERE symbol = %s AND sequence = ANY(%s) RETURNING sequence;",
                [symbol, sequences],
            )
            return [row[0] for row in cur.fetchall()]

    def read_ticks_df(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        query = """
            SELECT time, symbol, sequence, price, size, side, exchange, channel, ingest_time, worker
            FROM ticks
            WHERE symbol = %(symbol)s AND time BETWEEN %(start)s AND %(end)s
            ORDER BY time ASC;
        """
        with self._conn.cursor() as cur:
            cur.execute(query, {"symbol": symbol, "start": start, "end": end})
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)

    def close(self) -> None:
        self._conn.close()


class DuckDBTickStore:
    """Fallback store used only when Timescale can't be stood up cleanly.
    Same interface as TimescaleTickStore so callers don't need to branch."""

    def __init__(self, path: str | None = None) -> None:
        import duckdb

        self.path = path or settings.duckdb_path
        self._conn = duckdb.connect(self.path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ticks (
                time TIMESTAMP, symbol TEXT, sequence BIGINT, price DOUBLE,
                size DOUBLE, side TEXT, exchange TEXT, channel TEXT,
                ingest_time TIMESTAMP, worker TEXT
            )
            """
        )

    def write_tick(self, tick: Tick, worker: str | None = None) -> None:
        self._conn.execute(
            "INSERT INTO ticks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                tick.exchange_timestamp,
                tick.symbol,
                tick.sequence,
                float(tick.price),
                float(tick.size) if tick.size is not None else None,
                tick.side.value if tick.side else None,
                tick.exchange.value,
                tick.channel,
                tick.ingest_timestamp,
                worker,
            ],
        )

    def write_ticks_batch(self, ticks: list[Tick], worker: str | None = None) -> int:
        if not ticks:
            return 0
        rows = [
            (
                tick.exchange_timestamp,
                tick.symbol,
                tick.sequence,
                float(tick.price),
                float(tick.size) if tick.size is not None else None,
                tick.side.value if tick.side else None,
                tick.exchange.value,
                tick.channel,
                tick.ingest_timestamp,
                worker,
            )
            for tick in ticks
        ]
        self._conn.executemany("INSERT INTO ticks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        return len(ticks)

    def count(self) -> int:
        return self._conn.execute("SELECT count(*) FROM ticks").fetchone()[0]

    def delete_sequences(self, symbol: str, sequences: list[int]) -> list[int]:
        if not sequences:
            return []
        placeholders = ",".join("?" for _ in sequences)
        existing = self._conn.execute(
            f"SELECT sequence FROM ticks WHERE symbol = ? AND sequence IN ({placeholders})",
            [symbol, *sequences],
        ).fetchall()
        deleted = [row[0] for row in existing]
        self._conn.execute(
            f"DELETE FROM ticks WHERE symbol = ? AND sequence IN ({placeholders})",
            [symbol, *sequences],
        )
        return deleted

    def read_ticks_df(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        return self._conn.execute(
            "SELECT * FROM ticks WHERE symbol = ? AND time BETWEEN ? AND ? ORDER BY time ASC",
            [symbol, start, end],
        ).df()

    def close(self) -> None:
        self._conn.close()


def get_tick_store() -> TickStore:
    if settings.store_backend == "duckdb":
        return DuckDBTickStore()
    return TimescaleTickStore()
