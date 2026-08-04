"""Measures actual partition-pruning behavior against a large synthetic
dataset (182.5M rows / 36,500 files by default) three ways:

1. Full scan — no filter, every file read.
2. Glob + filter — the naive approach: a recursive glob enumerates every
   file, then a WHERE clause filters after reading metadata/data.
3. Direct partition path — the query targets the exact partition directory,
   so no enumeration of the other 36,499 files happens at all.

The honest finding this surfaces: (2) barely beats (1), because glob
expansion over tens of thousands of files dominates the query time even
when almost none of them are actually read for data. (3) is dramatically
faster because it skips file discovery entirely. This is *why* real
systems use a partition catalog (Hive Metastore, Glue, Iceberg/Delta
metadata) instead of a filesystem glob — the catalog answers "which files
match this partition" without listing the whole tree.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import duckdb
import polars as pl


@dataclass
class QueryTiming:
    label: str
    row_count: int
    elapsed_seconds: float


@dataclass
class ScaleTestReport:
    total_rows_in_dataset: int
    total_files: int
    glob_expansion_seconds: float
    polars_full_scan: QueryTiming
    polars_glob_filter: QueryTiming
    polars_direct_partition: QueryTiming
    duckdb_full_scan: QueryTiming
    duckdb_glob_filter: QueryTiming
    duckdb_direct_partition: QueryTiming


def _time_it(label: str, fn) -> QueryTiming:  # type: ignore[no-untyped-def]
    t0 = time.monotonic()
    row_count = fn()
    return QueryTiming(label=label, row_count=row_count, elapsed_seconds=time.monotonic() - t0)


def _scalar(cursor: duckdb.DuckDBPyConnection) -> int:
    row = cursor.fetchone()
    assert row is not None
    return int(row[0])


def run_partition_pruning_benchmark(
    dataset_path: str, target_symbol: str = "SYM-050", target_date: str = "2023-06-15"
) -> ScaleTestReport:
    import glob as globmod

    glob_pattern = f"{dataset_path}/**/*.parquet"
    direct_path = f"{dataset_path}/symbol={target_symbol}/date={target_date}/*.parquet"

    t0 = time.monotonic()
    all_files = globmod.glob(glob_pattern, recursive=True)
    glob_expansion_seconds = time.monotonic() - t0

    # --- Polars -----------------------------------------------------
    polars_full = _time_it(
        "polars full scan",
        lambda: pl.scan_parquet(glob_pattern, hive_partitioning=True).select(pl.len()).collect().item(),
    )
    polars_glob = _time_it(
        "polars glob + filter",
        lambda: pl.scan_parquet(glob_pattern, hive_partitioning=True)
        .filter((pl.col("symbol") == target_symbol) & (pl.col("date") == pl.lit(target_date).str.to_date()))
        .select(pl.len())
        .collect()
        .item(),
    )
    polars_direct = _time_it(
        "polars direct partition path", lambda: pl.scan_parquet(direct_path).select(pl.len()).collect().item()
    )

    # --- DuckDB -------------------------------------------------------
    con = duckdb.connect()
    duckdb_full = _time_it(
        "duckdb full scan",
        lambda: _scalar(con.execute(f"SELECT count(*) FROM read_parquet('{glob_pattern}', hive_partitioning=true)")),
    )
    duckdb_glob = _time_it(
        "duckdb glob + filter",
        lambda: _scalar(
            con.execute(
                f"SELECT count(*) FROM read_parquet('{glob_pattern}', hive_partitioning=true) "
                "WHERE symbol = ? AND date = ?",
                [target_symbol, target_date],
            )
        ),
    )
    duckdb_direct = _time_it(
        "duckdb direct partition path",
        lambda: _scalar(con.execute(f"SELECT count(*) FROM read_parquet('{direct_path}')")),
    )
    con.close()

    return ScaleTestReport(
        total_rows_in_dataset=polars_full.row_count,
        total_files=len(all_files),
        glob_expansion_seconds=glob_expansion_seconds,
        polars_full_scan=polars_full,
        polars_glob_filter=polars_glob,
        polars_direct_partition=polars_direct,
        duckdb_full_scan=duckdb_full,
        duckdb_glob_filter=duckdb_glob,
        duckdb_direct_partition=duckdb_direct,
    )


def _print_report(report: ScaleTestReport) -> None:
    print(f"dataset: {report.total_rows_in_dataset:,} rows across {report.total_files:,} files")
    print(f"glob expansion alone (list all {report.total_files:,} files): {report.glob_expansion_seconds:.4f}s")
    print()
    for label, full, glob_q, direct in (
        ("polars", report.polars_full_scan, report.polars_glob_filter, report.polars_direct_partition),
        ("duckdb", report.duckdb_full_scan, report.duckdb_glob_filter, report.duckdb_direct_partition),
    ):
        print(f"--- {label} ---")
        print(f"  full scan:              {full.row_count:>12,} rows in {full.elapsed_seconds:.4f}s")
        print(f"  glob + filter:          {glob_q.row_count:>12,} rows in {glob_q.elapsed_seconds:.4f}s "
              f"({full.elapsed_seconds / glob_q.elapsed_seconds:.1f}x vs full scan)")
        print(f"  direct partition path:  {direct.row_count:>12,} rows in {direct.elapsed_seconds:.4f}s "
              f"({full.elapsed_seconds / direct.elapsed_seconds:.0f}x vs full scan)")
        print()


if __name__ == "__main__":
    report = run_partition_pruning_benchmark("./dataeng_lake/scale_test")
    _print_report(report)
