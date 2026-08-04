import psycopg
import pytest

from posttrade.config import settings


def _postgres_reachable() -> bool:
    try:
        with psycopg.connect(settings.timescale_dsn, connect_timeout=2):
            return True
    except psycopg.Error:
        return False


@pytest.fixture(scope="session")
def postgres_required():
    if not _postgres_reachable():
        pytest.skip("TimescaleDB not reachable at settings.timescale_dsn — start `docker compose up -d`")


def _minio_reachable() -> bool:
    from botocore.exceptions import BotoCoreError, ClientError

    from dataeng.bronze.s3_client import get_s3_client

    try:
        get_s3_client().list_buckets()
        return True
    except (BotoCoreError, ClientError):
        return False


@pytest.fixture(scope="session")
def minio_required():
    if not _minio_reachable():
        pytest.skip("MinIO not reachable — start `docker compose up -d minio`")
