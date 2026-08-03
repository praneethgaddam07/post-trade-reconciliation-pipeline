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
