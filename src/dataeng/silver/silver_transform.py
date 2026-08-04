from __future__ import annotations

import glob
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from dataeng.bronze.s3_client import get_s3_client
from dataeng.config import settings
from posttrade.ingest.async_ingestor import AsyncIngestor
from posttrade.models import Tick

logger = logging.getLogger(__name__)

# decimal128(18, 8): 8 decimal places covers crypto's typical tick size,
# stored exactly (not float) — same "Decimal for money" principle as the
# streaming path's Tick model, just expressed in Arrow's decimal type.
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


@dataclass
class SilverTransformResult:
    symbol: str = ""
    date: str = ""
    lines_read: int = 0
    duplicates_dropped: int = 0
    normalization_failures: int = 0
    rows_written: int = 0
    output_files: list[str] = field(default_factory=list)


def list_bronze_objects(bucket: str, prefix: str) -> list[str]:
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_bronze_lines(bucket: str, keys: list[str]) -> list[str]:
    client = get_s3_client()
    lines: list[str] = []
    for key in keys:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        lines.extend(line for line in body.splitlines() if line.strip())
    return lines


def transform_symbol_date(
    symbol: str,
    date: str,
    bucket: str | None = None,
    output_dir: str | None = None,
) -> SilverTransformResult:
    """Rebuilds the silver Parquet partition for one (symbol, date) entirely
    from bronze — a full-partition rebuild rather than an incremental
    upsert, which keeps the transform idempotent: re-running it against the
    same bronze objects is always safe and produces the same result.

    Reuses `AsyncIngestor.normalize` — the exact same normalization code
    the live streaming path uses — so silver's schema enforcement can never
    silently drift from what streaming actually produces. That's the Kappa
    promise made concrete: one normalization code path for both real-time
    and batch, not two implementations that can quietly disagree.
    """
    bucket = bucket or settings.bronze_bucket
    output_dir = output_dir or settings.silver_path
    prefix = f"ticks/symbol={symbol}/date={date}/"

    result = SilverTransformResult(symbol=symbol, date=date)
    keys = list_bronze_objects(bucket, prefix)
    raw_lines = read_bronze_lines(bucket, keys)
    result.lines_read = len(raw_lines)

    seen: set[str] = set()
    deduped: list[str] = []
    for line in raw_lines:
        if line in seen:
            result.duplicates_dropped += 1
            continue
        seen.add(line)
        deduped.append(line)

    ingestor = AsyncIngestor(symbols=[symbol])
    ticks: list[Tick] = []
    for line in deduped:
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            result.normalization_failures += 1
            continue
        tick = ingestor.normalize(data, data.get("type"))
        if tick is None:
            result.normalization_failures += 1
            continue
        ticks.append(tick)

    # Clear any part-files from a prior run of this same partition *before*
    # deciding what to write. Without this, re-materializing a partition
    # (the sensor firing twice, a manual rerun, a backfill overlapping
    # already-processed data) leaves the old file in place alongside the
    # new one — gold's `read_parquet('**/*.parquet')` glob would then
    # double-count every tick in that partition. This is what actually
    # makes "rebuilds from bronze are idempotent" true rather than aspirational.
    partition_dir = f"{output_dir}/ticks/symbol={symbol}/date={date}"
    os.makedirs(partition_dir, exist_ok=True)
    for stale in glob.glob(f"{partition_dir}/*.parquet"):
        os.remove(stale)

    if not ticks:
        logger.info("silver transform: no rows produced", extra=result.__dict__)
        return result

    table = pa.table(
        {
            "symbol": [t.symbol for t in ticks],
            "sequence": [t.sequence for t in ticks],
            "price": [t.price for t in ticks],
            "size": [t.size for t in ticks],
            "side": [t.side.value if t.side else None for t in ticks],
            "exchange": [t.exchange.value for t in ticks],
            "channel": [t.channel for t in ticks],
            "exchange_timestamp": [t.exchange_timestamp for t in ticks],
            "ingest_timestamp": [t.ingest_timestamp for t in ticks],
        },
        schema=_SCHEMA,
    )

    out_path = f"{partition_dir}/part-{uuid.uuid4().hex}.parquet"
    pq.write_table(table, out_path, compression="snappy")
    result.rows_written = len(ticks)
    result.output_files.append(out_path)
    logger.info("silver transform complete", extra=result.__dict__)
    return result
