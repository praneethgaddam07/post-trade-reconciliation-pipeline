from dagster import ScheduleDefinition

from dataeng.orchestration.jobs import reconciliation_job

reconciliation_schedule = ScheduleDefinition(
    job=reconciliation_job,
    cron_schedule="0 * * * *",  # hourly — matches the "hourly/daily" cadence
    # the reviewer's own notes suggested for a scheduled reconciliation job.
)
