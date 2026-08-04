import glob
import shutil
import uuid

import pyarrow.parquet as pq

from dataeng.bronze.s3_client import ensure_bucket, get_s3_client
from dataeng.config import settings
from dataeng.silver.silver_transform import transform_symbol_date


def _put_bronze_lines(symbol: str, date: str, hour: str, lines: list[str]) -> str:
    client = get_s3_client()
    ensure_bucket(client, settings.bronze_bucket)
    body = ("\n".join(lines) + "\n").encode("utf-8")
    key = f"ticks/symbol={symbol}/date={date}/hour={hour}/{uuid.uuid4().hex}.ndjson"
    client.put_object(Bucket=settings.bronze_bucket, Key=key, Body=body)
    return key


def _delete_bronze_key(key: str) -> None:
    get_s3_client().delete_object(Bucket=settings.bronze_bucket, Key=key)


def test_silver_transform_dedupes_identical_raw_lines(minio_required, tmp_path):
    symbol = f"TEST-DEDUP-{uuid.uuid4().hex[:8]}"
    date = "2026-01-01"
    line_a = (
        f'{{"type":"ticker","product_id":"{symbol}","price":"100.5","side":"buy",'
        f'"time":"2026-01-01T12:00:00.000000Z"}}'
    )
    line_b = (
        f'{{"type":"ticker","product_id":"{symbol}","price":"101.0","side":"sell",'
        f'"time":"2026-01-01T12:00:01.000000Z"}}'
    )
    # line_a lands twice — a realistic duplicate (e.g. a retried flush)
    bronze_key = _put_bronze_lines(symbol, date, "12", [line_a, line_a, line_b])

    try:
        result = transform_symbol_date(symbol, date, output_dir=str(tmp_path))
        assert result.lines_read == 3
        assert result.duplicates_dropped == 1
        assert result.normalization_failures == 0
        assert result.rows_written == 2

        table = pq.read_table(result.output_files[0])
        assert table.num_rows == 2
        prices = {str(p) for p in table.column("price").to_pylist()}
        assert prices == {"100.50000000", "101.00000000"}
    finally:
        _delete_bronze_key(bronze_key)
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_silver_transform_quarantines_unparseable_lines(minio_required, tmp_path):
    symbol = f"TEST-BADLINE-{uuid.uuid4().hex[:8]}"
    date = "2026-01-01"
    good_line = (
        f'{{"type":"ticker","product_id":"{symbol}","price":"50.0","side":"buy",'
        f'"time":"2026-01-01T12:00:00.000000Z"}}'
    )
    bronze_key = _put_bronze_lines(symbol, date, "12", [good_line, "not-json-at-all", '{"type":"heartbeat"}'])

    try:
        result = transform_symbol_date(symbol, date, output_dir=str(tmp_path))
        assert result.lines_read == 3
        # "not-json-at-all" fails JSON parsing; heartbeat parses but has no
        # product_id/price so normalize() rejects it — both count as failures
        assert result.normalization_failures == 2
        assert result.rows_written == 1
    finally:
        _delete_bronze_key(bronze_key)
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_silver_transform_empty_partition_writes_nothing(minio_required, tmp_path):
    symbol = f"TEST-EMPTY-{uuid.uuid4().hex[:8]}"
    result = transform_symbol_date(symbol, "2099-01-01", output_dir=str(tmp_path))
    assert result.lines_read == 0
    assert result.rows_written == 0
    assert result.output_files == []


def test_silver_transform_rerun_does_not_duplicate_rows(minio_required, tmp_path):
    """Regression test: re-materializing a partition (a sensor firing
    twice, a manual rerun, an overlapping backfill) must replace the
    partition's output, not append a second file alongside it — otherwise
    gold's read_parquet('**/*.parquet') glob double-counts every row.
    Caught for real via the Dagster CLI materializing the same partition
    twice, which produced 402 gold rows for 134 actual ticks."""
    symbol = f"TEST-RERUN-{uuid.uuid4().hex[:8]}"
    date = "2026-01-01"
    line = (
        f'{{"type":"ticker","product_id":"{symbol}","price":"75.0","side":"buy",'
        f'"time":"2026-01-01T12:00:00.000000Z"}}'
    )
    bronze_key = _put_bronze_lines(symbol, date, "12", [line])

    try:
        result1 = transform_symbol_date(symbol, date, output_dir=str(tmp_path))
        result2 = transform_symbol_date(symbol, date, output_dir=str(tmp_path))
        assert result1.rows_written == 1
        assert result2.rows_written == 1

        partition_dir = f"{tmp_path}/ticks/symbol={symbol}/date={date}"
        part_files = glob.glob(f"{partition_dir}/*.parquet")
        assert len(part_files) == 1, f"expected exactly one part-file after two runs, found {part_files}"

        table = pq.read_table(part_files[0])
        assert table.num_rows == 1
    finally:
        _delete_bronze_key(bronze_key)
        shutil.rmtree(tmp_path, ignore_errors=True)
