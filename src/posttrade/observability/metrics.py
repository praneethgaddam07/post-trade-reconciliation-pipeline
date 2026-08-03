import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

ticks_ingested_total = Counter(
    "posttrade_ticks_ingested_total",
    "Ticks normalized and published by the async ingestor",
    ["symbol", "channel"],
)

ticks_processed_total = Counter(
    "posttrade_ticks_processed_total",
    "Ticks batch-written to the store by a worker",
    ["worker"],
)

batch_write_seconds = Histogram(
    "posttrade_batch_write_seconds",
    "Time to batch-write one XREADGROUP read to the store",
    ["worker"],
)

consumer_lag = Gauge(
    "posttrade_consumer_lag",
    "Redis consumer group lag, sampled by a worker after each read",
    ["group"],
)


def start_metrics_server(port: int) -> None:
    """Starts a Prometheus scrape endpoint on `port` in the current process.
    Called once per process (ingestor, or each worker) — each gets its own
    port so Prometheus scrapes them as distinct targets and Grafana can
    break throughput down per worker."""
    try:
        start_http_server(port)
        logger.info("metrics server listening", extra={"port": port})
    except OSError as exc:
        logger.warning("could not start metrics server", extra={"port": port, "error": str(exc)})
