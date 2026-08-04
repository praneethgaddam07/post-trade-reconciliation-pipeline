"""Generates a large synthetic historical tick dataset, partitioned by
symbol/date exactly like real silver output, to honestly measure how
partition pruning behaves at a volume actual capture never reached.

Vectorized with numpy — building 180M+ rows via a Python loop would take
forever; this builds whole columns at once and lets Polars write the
Hive-partitioned Parquet layout in one call.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import polars as pl


@dataclass
class GenerationResult:
    total_rows: int
    n_symbols: int
    n_days: int
    rows_per_partition: int
    generation_seconds: float
    write_seconds: float
    output_path: str


def generate_scale_dataset(
    output_path: str,
    n_symbols: int = 100,
    n_days: int = 365,
    rows_per_partition: int = 5000,
    start_date: date | None = None,
    seed: int = 42,
) -> GenerationResult:
    rng = np.random.default_rng(seed)
    start_date = start_date or date(2023, 1, 1)

    symbols = [f"SYM-{i:03d}" for i in range(n_symbols)]
    dates = np.array([start_date + timedelta(days=d) for d in range(n_days)], dtype="datetime64[D]")
    total_rows = n_symbols * n_days * rows_per_partition

    t0 = time.monotonic()
    symbol_col = np.repeat(symbols, n_days * rows_per_partition)
    date_col = np.tile(np.repeat(dates, rows_per_partition), n_symbols)
    sequence_col = np.tile(np.arange(1, rows_per_partition + 1), n_symbols * n_days)
    price_col = rng.uniform(1, 100_000, total_rows)

    df = pl.DataFrame(
        {"symbol": symbol_col, "date": date_col, "sequence": sequence_col, "price": price_col}
    )
    generation_seconds = time.monotonic() - t0

    t1 = time.monotonic()
    df.write_parquet(output_path, partition_by=["symbol", "date"], mkdir=True)
    write_seconds = time.monotonic() - t1

    return GenerationResult(
        total_rows=total_rows,
        n_symbols=n_symbols,
        n_days=n_days,
        rows_per_partition=rows_per_partition,
        generation_seconds=generation_seconds,
        write_seconds=write_seconds,
        output_path=output_path,
    )


if __name__ == "__main__":
    result = generate_scale_dataset("./dataeng_lake/scale_test")
    print(result)
