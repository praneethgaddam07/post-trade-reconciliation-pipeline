import asyncio
import itertools
import json
import logging
import multiprocessing
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

import redis

from posttrade.config import settings
from posttrade.models import Exchange, Tick
from posttrade.queue.redis_stream import StreamConsumer, StreamPublisher
from posttrade.workers.worker import run_worker

logger = logging.getLogger(__name__)


@dataclass
class LagSample:
    t: float
    lag: int
    published_so_far: int


@dataclass
class StepResult:
    target_rate: float
    duration_s: float
    published: int
    actual_publish_rate: float
    max_lag_during_step: int
    lag_at_step_end: int
    lag_after_grace: int
    caught_up: bool
    samples: list[LagSample] = field(default_factory=list)


def _synthetic_tick(symbol: str, seq: int) -> Tick:
    now = datetime.now(timezone.utc)
    return Tick(
        symbol=symbol,
        sequence=seq,
        price=Decimal("100.00"),
        exchange=Exchange.COINBASE,
        channel="ticker",
        exchange_timestamp=now,
        ingest_timestamp=now,
    )


def _get_lag(client: redis.Redis, stream_name: str, group_name: str) -> int:
    try:
        groups = client.xinfo_groups(stream_name)
    except redis.ResponseError:
        return 0
    for g in groups:
        name = g["name"].decode() if isinstance(g["name"], bytes) else g["name"]
        if name == group_name:
            lag = g.get("lag")
            return lag if lag is not None else g["pending"]
    return 0


def _publish_process(
    process_id: int,
    redis_url: str,
    stream_name: str,
    symbols: list[str],
    target_rate: float,
    duration_s: float,
    batch_size: int,
    result_queue: "multiprocessing.Queue[tuple[int, int, float]]",
) -> None:
    """Runs in its own OS process: pipelines batches of XADD calls at a
    target rate for duration_s, then reports (process_id, published, elapsed)
    back to the parent. Several of these run concurrently — same fan-out
    idea as the worker pool, just on the producer side — so the ceiling
    found isn't an artifact of one asyncio loop's single-core, sequential
    round-trip limit."""

    async def _run() -> None:
        publisher = StreamPublisher(redis_url, stream_name)
        seqs = {s: itertools.count(1) for s in symbols}
        sym_cycle = itertools.cycle(symbols)
        interval = batch_size / target_rate if target_rate > 0 else 0
        start = time.monotonic()
        next_send = start
        published = 0
        while time.monotonic() - start < duration_s:
            batch = []
            for _ in range(batch_size):
                symbol = next(sym_cycle)
                batch.append(_synthetic_tick(symbol, next(seqs[symbol])))
            await publisher.publish_batch(batch)
            published += len(batch)
            next_send += interval
            sleep_for = next_send - time.monotonic()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
        elapsed = time.monotonic() - start
        await publisher.close()
        result_queue.put((process_id, published, elapsed))

    asyncio.run(_run())


class LoadHarness:
    """Replays synthetic ticks onto the real Redis stream at controlled,
    increasing rates while a persistent worker pool consumes concurrently
    (writing to the real store in batches), sampling consumer lag throughout
    to find the actual point where the pipeline stops keeping up.

    The publisher itself is multi-process and pipelines XADD calls in
    batches, so a single asyncio loop's round-trip latency isn't what caps
    the measurement — the first version of this harness found that ceiling
    by accident (see README history) rather than the pipeline's real one.

    Synthetic data is used deliberately — the point is to find *this
    system's* ceiling, not the exchange feed's rate limit.
    """

    def __init__(
        self,
        worker_pool_size: int | None = None,
        stream_name: str | None = None,
        group_name: str | None = None,
        publisher_processes: int = 4,
        publish_batch_size: int = 100,
        worker_read_batch_size: int = 500,
    ) -> None:
        self.worker_pool_size = worker_pool_size or settings.worker_pool_size
        self.stream_name = stream_name or f"loadtest-{int(time.time())}"
        self.group_name = group_name or "loadtest-workers"
        self.publisher_processes = publisher_processes
        self.publish_batch_size = publish_batch_size
        self.worker_read_batch_size = worker_read_batch_size
        self.symbols = ["LOAD-A", "LOAD-B"]
        self._workers: list[multiprocessing.Process] = []
        self._redis = redis.Redis.from_url(settings.redis_url)

    def start_workers(self) -> None:
        bootstrap = StreamConsumer(settings.redis_url, self.stream_name, self.group_name, "bootstrap")
        bootstrap.close()
        for i in range(self.worker_pool_size):
            p = multiprocessing.Process(
                target=run_worker,
                args=(f"loadtest-worker-{i}",),
                kwargs={
                    "max_messages": None,
                    "max_idle_reads": None,
                    "stream_name": self.stream_name,
                    "group_name": self.group_name,
                    "read_batch_size": self.worker_read_batch_size,
                    "metrics_port": settings.metrics_port_worker_base + i,
                },
                daemon=True,
            )
            p.start()
            self._workers.append(p)
        logger.info("started %d persistent workers for load test", self.worker_pool_size)

    def stop_workers(self) -> None:
        for p in self._workers:
            p.terminate()
        for p in self._workers:
            p.join(timeout=10)
        self._workers = []

    def _produce(
        self, target_rate: float, duration_s: float, sample_interval: float = 1.0
    ) -> tuple[int, float, list[LagSample]]:
        result_queue: "multiprocessing.Queue[tuple[int, int, float]]" = multiprocessing.Queue()
        per_process_rate = target_rate / self.publisher_processes
        procs = [
            multiprocessing.Process(
                target=_publish_process,
                args=(
                    i,
                    settings.redis_url,
                    self.stream_name,
                    self.symbols,
                    per_process_rate,
                    duration_s,
                    self.publish_batch_size,
                    result_queue,
                ),
            )
            for i in range(self.publisher_processes)
        ]
        start = time.monotonic()
        for p in procs:
            p.start()

        samples: list[LagSample] = []
        next_sample = start
        # publishers self-terminate after duration_s; poll lag until they
        # actually exit rather than trusting duration_s exactly (process
        # startup/join overhead means real wall time runs a bit longer)
        while any(p.is_alive() for p in procs):
            now_t = time.monotonic()
            if now_t >= next_sample:
                lag = _get_lag(self._redis, self.stream_name, self.group_name)
                samples.append(LagSample(t=now_t - start, lag=lag, published_so_far=-1))
                next_sample += sample_interval
            time.sleep(0.05)

        for p in procs:
            p.join(timeout=10)

        per_process_results: dict[int, tuple[int, float]] = {}
        while not result_queue.empty():
            pid, published, elapsed = result_queue.get()
            per_process_results[pid] = (published, elapsed)

        total_published = sum(p for p, _ in per_process_results.values())
        wall_elapsed = time.monotonic() - start
        for s in samples:
            s.published_so_far = total_published
        return total_published, wall_elapsed, samples

    def run_step(self, target_rate: float, duration_s: float = 8.0, grace_s: float = 10.0) -> StepResult:
        published, elapsed, samples = self._produce(target_rate, duration_s)
        lag_at_step_end = _get_lag(self._redis, self.stream_name, self.group_name)

        grace_start = time.monotonic()
        while time.monotonic() - grace_start < grace_s:
            lag = _get_lag(self._redis, self.stream_name, self.group_name)
            samples.append(
                LagSample(t=duration_s + (time.monotonic() - grace_start), lag=lag, published_so_far=published)
            )
            if lag == 0:
                break
            time.sleep(0.5)
        lag_after_grace = _get_lag(self._redis, self.stream_name, self.group_name)

        return StepResult(
            target_rate=target_rate,
            duration_s=elapsed,
            published=published,
            actual_publish_rate=published / elapsed if elapsed > 0 else 0,
            max_lag_during_step=max((s.lag for s in samples), default=0),
            lag_at_step_end=lag_at_step_end,
            lag_after_grace=lag_after_grace,
            caught_up=lag_after_grace == 0,
            samples=samples,
        )

    def run_ramp(self, rates: list[float], duration_s: float = 8.0, grace_s: float = 10.0) -> list[StepResult]:
        self.start_workers()
        results: list[StepResult] = []
        try:
            for rate in rates:
                logger.info("=== load step: target %.0f msg/s ===", rate)
                result = self.run_step(rate, duration_s, grace_s)
                results.append(result)
                logger.info(
                    "target=%.0f actual_publish=%.1f max_lag=%d lag_after_grace=%d caught_up=%s",
                    rate,
                    result.actual_publish_rate,
                    result.max_lag_during_step,
                    result.lag_after_grace,
                    result.caught_up,
                )
                if not result.caught_up:
                    logger.warning("system did not catch up at %.0f msg/s target rate -- stopping ramp", rate)
                    break
        finally:
            self.stop_workers()
        return results


def results_to_json(results: list[StepResult]) -> str:
    return json.dumps([asdict(r) for r in results], indent=2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [loadtest] %(levelname)s %(message)s")
    harness = LoadHarness()
    rates = [100, 250, 500, 1000, 2000, 4000, 8000]
    results = harness.run_ramp(rates, duration_s=8.0, grace_s=15.0)
    print(results_to_json(results))
