import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _split_symbols(raw: str) -> list[str]:
    return [s.strip() for s in raw.split(",") if s.strip()]


@dataclass(frozen=True)
class Settings:
    redis_url: str = field(default_factory=lambda: os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    redis_stream_name: str = field(default_factory=lambda: os.environ.get("REDIS_STREAM_NAME", "ticks"))
    redis_consumer_group: str = field(default_factory=lambda: os.environ.get("REDIS_CONSUMER_GROUP", "tick-workers"))

    timescale_dsn: str = field(
        default_factory=lambda: os.environ.get(
            "TIMESCALE_DSN", "postgresql://posttrade:posttrade@localhost:5432/posttrade"
        )
    )
    store_backend: str = field(default_factory=lambda: os.environ.get("STORE_BACKEND", "timescale"))
    duckdb_path: str = field(default_factory=lambda: os.environ.get("DUCKDB_PATH", "./data/posttrade.duckdb"))

    coinbase_ws_url: str = field(
        default_factory=lambda: os.environ.get("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
    )
    kraken_ws_url: str = field(default_factory=lambda: os.environ.get("KRAKEN_WS_URL", "wss://ws.kraken.com/v2"))
    market_data_provider: str = field(default_factory=lambda: os.environ.get("MARKET_DATA_PROVIDER", "coinbase"))
    symbols: list[str] = field(default_factory=lambda: _split_symbols(os.environ.get("SYMBOLS", "BTC-USD,ETH-USD")))

    worker_pool_size: int = field(default_factory=lambda: int(os.environ.get("WORKER_POOL_SIZE", "4")))

    metrics_enabled: bool = field(default_factory=lambda: os.environ.get("METRICS_ENABLED", "true").lower() == "true")
    metrics_port_ingestor: int = field(default_factory=lambda: int(os.environ.get("METRICS_PORT_INGESTOR", "9100")))
    metrics_port_worker_base: int = field(
        default_factory=lambda: int(os.environ.get("METRICS_PORT_WORKER_BASE", "9101"))
    )


settings = Settings()
