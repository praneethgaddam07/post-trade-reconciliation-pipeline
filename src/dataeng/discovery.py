from dataeng.bronze.s3_client import get_s3_client
from dataeng.config import settings


def discover_bronze_partitions() -> set[tuple[str, str]]:
    """Scans the bronze bucket and returns every (symbol, date) pair that
    has at least one landed object — the source of truth both the sensor
    and the batch backfill job use to decide what needs (re)processing."""
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    found: set[tuple[str, str]] = set()
    for page in paginator.paginate(Bucket=settings.bronze_bucket, Prefix="ticks/"):
        for obj in page.get("Contents", []):
            symbol: str | None = None
            date: str | None = None
            for part in obj["Key"].split("/"):
                if part.startswith("symbol="):
                    symbol = part.split("=", 1)[1]
                elif part.startswith("date="):
                    date = part.split("=", 1)[1]
            if symbol and date:
                found.add((symbol, date))
    return found
