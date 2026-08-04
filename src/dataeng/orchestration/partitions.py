from dagster import DailyPartitionsDefinition, MultiPartitionsDefinition, StaticPartitionsDefinition

from posttrade.config import settings

symbol_partitions = StaticPartitionsDefinition(settings.symbols)
date_partitions = DailyPartitionsDefinition(start_date="2026-01-01")

silver_partitions = MultiPartitionsDefinition({"symbol": symbol_partitions, "date": date_partitions})
