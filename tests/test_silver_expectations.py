from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("great_expectations")

from dataeng.quality.silver_expectations import run_silver_quality_checks

_SCHEMA_FIELDS: list[pa.Field[Any]] = [
    pa.field("symbol", pa.string()),
    pa.field("sequence", pa.int64()),
    pa.field("price", pa.decimal128(18, 8)),
    pa.field("size", pa.decimal128(18, 8)),
    pa.field("side", pa.string()),
    pa.field("exchange", pa.string()),
    pa.field("channel", pa.string()),
    pa.field("exchange_timestamp", pa.timestamp("us", tz="UTC")),
    pa.field("ingest_timestamp", pa.timestamp("us", tz="UTC")),
]
_SCHEMA = pa.schema(_SCHEMA_FIELDS)


def _write_silver_parquet(base_dir: str, symbol: str, date: str, rows: list[dict[str, Any]]) -> None:
    partition_dir = f"{base_dir}/ticks/symbol={symbol}/date={date}"
    os.makedirs(partition_dir, exist_ok=True)
    table = pa.table(
        {
            "symbol": [r["symbol"] for r in rows],
            "sequence": [r["sequence"] for r in rows],
            "price": [r["price"] for r in rows],
            "size": [r.get("size") for r in rows],
            "side": [r.get("side") for r in rows],
            "exchange": [r.get("exchange", "coinbase") for r in rows],
            "channel": [r.get("channel", "ticker") for r in rows],
            "exchange_timestamp": [r["exchange_timestamp"] for r in rows],
            "ingest_timestamp": [r["ingest_timestamp"] for r in rows],
        },
        schema=_SCHEMA,
    )
    pq.write_table(table, f"{partition_dir}/part-test.parquet")


def _clean_rows(symbol: str, n: int) -> list[dict[str, Any]]:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    return [
        {
            "symbol": symbol,
            "sequence": i,
            "price": Decimal("100.00000000"),
            "side": "buy",
            "exchange_timestamp": now,
            "ingest_timestamp": now,
        }
        for i in range(1, n + 1)
    ]


def test_quality_checks_pass_on_clean_silver_data(tmp_path):
    _write_silver_parquet(str(tmp_path), "TEST-QUALITY-CLEAN", "2026-01-01", _clean_rows("TEST-QUALITY-CLEAN", 5))

    try:
        result = run_silver_quality_checks(silver_path=str(tmp_path))
        assert result.success is True
        assert result.row_count == 5
        assert result.unsuccessful_expectations == 0
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_quality_checks_fail_on_negative_price(tmp_path):
    """Regression-style test: the suite must actually catch a real
    violation, not just pass on clean data — mirrors how the dbt custom
    test and the silver dedup logic were each verified against a planted
    failure, not just a happy path."""
    rows = _clean_rows("TEST-QUALITY-BADPRICE", 5)
    rows[0]["price"] = Decimal("-999.00000000")
    _write_silver_parquet(str(tmp_path), "TEST-QUALITY-BADPRICE", "2026-01-01", rows)

    try:
        result = run_silver_quality_checks(silver_path=str(tmp_path))
        assert result.success is False
        assert result.unsuccessful_expectations >= 1
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_quality_checks_fail_on_duplicate_sequence(tmp_path):
    rows = _clean_rows("TEST-QUALITY-DUPSEQ", 5)
    rows[1]["sequence"] = rows[0]["sequence"]  # duplicate (symbol, sequence)
    _write_silver_parquet(str(tmp_path), "TEST-QUALITY-DUPSEQ", "2026-01-01", rows)

    try:
        result = run_silver_quality_checks(silver_path=str(tmp_path))
        assert result.success is False
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_quality_checks_skip_on_empty_dataset(tmp_path):
    result = run_silver_quality_checks(silver_path=str(tmp_path))
    assert result.success is False
    assert result.row_count == 0
