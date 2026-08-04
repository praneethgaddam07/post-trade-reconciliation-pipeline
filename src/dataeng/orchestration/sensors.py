from dagster import MultiPartitionKey, RunRequest, SensorEvaluationContext, sensor

from dataeng.discovery import discover_bronze_partitions
from dataeng.orchestration.assets import silver_ticks


@sensor(target=silver_ticks, minimum_interval_seconds=15)
def new_bronze_partition_sensor(context: SensorEvaluationContext) -> list[RunRequest]:
    """Bridges the always-on streaming ingestor (which lands bronze files
    independently of Dagster) to the orchestrated batch side: whenever a
    (symbol, date) partition shows up in bronze that hasn't been seen
    before, request a materialization of the matching silver_ticks
    partition. The cursor is just the set of partitions already requested,
    so a restart doesn't re-trigger everything."""
    seen = {tuple(pair.split("|", 1)) for pair in context.cursor.split(",") if pair} if context.cursor else set()
    available = discover_bronze_partitions()
    new_partitions = available - seen

    run_requests = [
        RunRequest(partition_key=MultiPartitionKey({"symbol": symbol, "date": date}))
        for symbol, date in sorted(new_partitions)
    ]

    context.update_cursor(",".join(f"{s}|{d}" for s, d in available))
    return run_requests
