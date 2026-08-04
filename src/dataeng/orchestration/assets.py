import os
import subprocess
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dataeng.orchestration.partitions import silver_partitions
from dataeng.silver.silver_transform import transform_symbol_date

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DBT_DIR = _REPO_ROOT / "dbt"


@asset(partitions_def=silver_partitions, group_name="silver")
def silver_ticks(context: AssetExecutionContext) -> MaterializeResult[Any]:
    """One partition per (symbol, date) — materializing it runs the same
    `transform_symbol_date` the sensor and the batch backfill job both use,
    rebuilding that partition's Parquet entirely from bronze."""
    keys = context.partition_key.keys_by_dimension
    symbol, date = keys["symbol"], keys["date"]

    result = transform_symbol_date(symbol, date)
    return MaterializeResult(
        metadata={
            "symbol": symbol,
            "date": date,
            "lines_read": result.lines_read,
            "duplicates_dropped": result.duplicates_dropped,
            "normalization_failures": result.normalization_failures,
            "rows_written": result.rows_written,
        }
    )


@asset(deps=[silver_ticks], group_name="gold")
def gold_tables(context: AssetExecutionContext) -> MaterializeResult[Any]:
    """Runs `dbt build` (models + tests + source freshness) against
    whatever silver partitions exist so far."""
    proc = subprocess.run(
        ["dbt", "build"],
        cwd=_DBT_DIR,
        env={**os.environ, "DBT_PROFILES_DIR": str(_DBT_DIR)},
        capture_output=True,
        text=True,
        check=False,
    )
    context.log.info(proc.stdout)
    if proc.returncode != 0:
        context.log.error(proc.stderr)
        raise RuntimeError(f"dbt build failed (exit {proc.returncode}); see logs above")
    return MaterializeResult(metadata={"dbt_stdout_tail": MetadataValue.text(proc.stdout[-2000:])})
