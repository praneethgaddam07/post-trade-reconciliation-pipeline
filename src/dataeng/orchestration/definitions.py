from dagster import Definitions

from dataeng.orchestration.assets import gold_tables, silver_ticks
from dataeng.orchestration.jobs import batch_backfill_job, reconciliation_job
from dataeng.orchestration.schedules import reconciliation_schedule
from dataeng.orchestration.sensors import new_bronze_partition_sensor

defs = Definitions(
    assets=[silver_ticks, gold_tables],
    jobs=[batch_backfill_job, reconciliation_job],
    sensors=[new_bronze_partition_sensor],
    schedules=[reconciliation_schedule],
)
