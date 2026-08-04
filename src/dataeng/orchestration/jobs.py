import os
import subprocess
from pathlib import Path

from dagster import In, Nothing, OpExecutionContext, job, op

from dataeng.discovery import discover_bronze_partitions
from dataeng.silver.silver_transform import transform_symbol_date
from posttrade.config import settings as posttrade_settings
from posttrade.report.report import generate_break_report

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DBT_DIR = _REPO_ROOT / "dbt"


@op
def replay_all_bronze_partitions_op(context: OpExecutionContext) -> int:
    """Kappa-style batch backfill: rebuild silver from every (symbol, date)
    partition currently in the bronze object store, regardless of when it
    landed. Reuses the exact same `transform_symbol_date` the sensor-driven
    per-partition path calls — one code path for both real-time-triggered
    and full-history replay, which is the whole point of treating bronze
    as the immutable log of record."""
    partitions = discover_bronze_partitions()
    context.log.info(f"discovered {len(partitions)} bronze partitions to replay")
    for symbol, date in sorted(partitions):
        result = transform_symbol_date(symbol, date)
        context.log.info(f"{symbol} {date}: {result.rows_written} rows written")
    return len(partitions)


@op(ins={"start": In(Nothing)})
def build_gold_op(context: OpExecutionContext) -> None:
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
        raise RuntimeError(f"dbt build failed (exit {proc.returncode})")


@job
def batch_backfill_job() -> None:
    """The batch backfill DAG: full silver+gold rebuild from bronze."""
    build_gold_op(start=replay_all_bronze_partitions_op())


@op
def run_reconciliation_op(context: OpExecutionContext) -> None:
    """Runs the existing streaming pipeline's reconciler (unchanged) as a
    scheduled batch job, and writes the break report to the same place the
    manual `report.py` run does — orchestration wraps the reconciler, it
    doesn't reimplement it."""
    report_md = generate_break_report(posttrade_settings.symbols)
    context.log.info(f"reconciliation report generated ({len(report_md)} chars)")
    out_path = _REPO_ROOT / "reports" / "break_report.md"
    out_path.write_text(report_md)
    context.log.info(f"wrote {out_path}")


@job
def reconciliation_job() -> None:
    run_reconciliation_op()
