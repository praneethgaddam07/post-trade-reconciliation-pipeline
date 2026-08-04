from __future__ import annotations

import glob
import logging
from dataclasses import dataclass
from pathlib import Path

import great_expectations as gx
import pandas as pd
from great_expectations.checkpoint import UpdateDataDocsAction

from dataeng.config import settings

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_GX_ROOT = _REPO_ROOT / "gx_project"

_SUITE_NAME = "silver_ticks_suite"
_VALIDATION_NAME = "silver_ticks_validation"
_CHECKPOINT_NAME = "silver_ticks_checkpoint"
_DATASOURCE_NAME = "silver_pandas"
_ASSET_NAME = "silver_ticks"
_BATCH_DEF_NAME = "whole"


@dataclass
class QualityCheckResult:
    success: bool
    row_count: int
    evaluated_expectations: int
    successful_expectations: int
    unsuccessful_expectations: int
    data_docs_index: str | None = None


def _load_silver_dataframe(silver_path: str | None = None) -> pd.DataFrame:
    silver_path = silver_path or settings.silver_path
    files = glob.glob(f"{silver_path}/ticks/**/*.parquet", recursive=True)
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def _build_suite(context: gx.data_context.AbstractDataContext) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=_SUITE_NAME)

    # Schema / completeness — the same fields the streaming Tick model and
    # dbt's not_null tests both already treat as required; this is a
    # second, independent layer checking the same guarantee before gold.
    for column in ("symbol", "sequence", "price", "exchange_timestamp", "ingest_timestamp"):
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column))

    # Sanity bounds — catches a decimal parsing bug or a garbage exchange
    # payload before it reaches gold, not just "is it present."
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0, max_value=10_000_000)
    )
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="sequence", min_value=1))

    # side is legitimately null for some ticker messages; where present it
    # must be one of the two real values.
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="side", value_set=["buy", "sell"]))

    # Clock-skew check: ingest can lag exchange time (network + processing
    # delay) but should never appear to happen *before* the exchange sent
    # it — a violation here means a real clock problem somewhere in the
    # pipeline, not just noisy data.
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="ingest_timestamp", column_B="exchange_timestamp", or_equal=True
        )
    )

    # (symbol, sequence) is the natural key silver assigns — the same
    # invariant fct_ticks's tick_id uniqueness test checks in dbt, verified
    # independently here before the data ever reaches gold.
    suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["symbol", "sequence"]))

    try:
        context.suites.delete(_SUITE_NAME)
    except gx.exceptions.DataContextError:
        pass
    return context.suites.add(suite)


def run_silver_quality_checks(silver_path: str | None = None) -> QualityCheckResult:
    """Validates the current silver dataset against a real expectation
    suite via a GX Checkpoint (not a one-off `.validate()` call) — the
    checkpoint is what persists results and updates Data Docs with actual
    pass/fail history, not just the suite's static definition."""
    df = _load_silver_dataframe(silver_path)
    if df.empty:
        logger.warning("no silver data found — skipping quality checks")
        return QualityCheckResult(
            success=False,
            row_count=0,
            evaluated_expectations=0,
            successful_expectations=0,
            unsuccessful_expectations=0,
        )

    context = gx.get_context(mode="file", project_root_dir=str(_GX_ROOT))

    # add_or_update_pandas is idempotent; asset/batch-definition creation
    # isn't, so those fall back to add_* only when get_* finds nothing.
    data_source = context.data_sources.add_or_update_pandas(_DATASOURCE_NAME)
    try:
        asset = data_source.get_asset(_ASSET_NAME)
    except LookupError:
        asset = data_source.add_dataframe_asset(_ASSET_NAME)
    try:
        batch_definition = asset.get_batch_definition(_BATCH_DEF_NAME)
    except KeyError:
        batch_definition = asset.add_batch_definition_whole_dataframe(_BATCH_DEF_NAME)

    suite = _build_suite(context)

    try:
        context.validation_definitions.delete(_VALIDATION_NAME)
    except gx.exceptions.DataContextError:
        pass
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=_VALIDATION_NAME, data=batch_definition, suite=suite)
    )

    try:
        context.checkpoints.delete(_CHECKPOINT_NAME)
    except gx.exceptions.DataContextError:
        pass
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=_CHECKPOINT_NAME,
            validation_definitions=[validation_definition],
            actions=[UpdateDataDocsAction(name="update_data_docs")],
        )
    )

    result = checkpoint.run(batch_parameters={"dataframe": df})
    run_result = next(iter(result.run_results.values()))
    stats = run_result["statistics"]

    docs_index = None
    try:
        sites = context.get_docs_sites_urls()
        if sites:
            docs_index = sites[0]["site_url"]
    except Exception as exc:  # noqa: BLE001 — docs URL lookup is best-effort, never fatal
        logger.warning("could not resolve data docs URL: %s", exc)

    logger.info(
        "silver quality checks complete",
        extra={"success": result.success, "row_count": len(df), **stats},
    )

    return QualityCheckResult(
        success=bool(result.success),
        row_count=len(df),
        evaluated_expectations=stats["evaluated_expectations"],
        successful_expectations=stats["successful_expectations"],
        unsuccessful_expectations=stats["unsuccessful_expectations"],
        data_docs_index=docs_index,
    )


if __name__ == "__main__":
    from posttrade.observability.logging_config import configure_structured_logging

    configure_structured_logging()
    outcome = run_silver_quality_checks()
    print(outcome)
