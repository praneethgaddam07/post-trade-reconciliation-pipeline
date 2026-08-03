#!/usr/bin/env bash
# Runs one end-to-end demo pass: ingest a real backlog from the live feed,
# drain it with the worker pool, run the trade book simulator against the
# same stream, then reconcile and print a break report.
set -euo pipefail
cd "$(dirname "$0")/.."

INGEST_SECONDS="${INGEST_SECONDS:-20}"

echo "== bringing up infra =="
docker compose up -d
until docker compose exec -T redis redis-cli ping >/dev/null 2>&1; do sleep 1; done
until docker compose exec -T timescaledb pg_isready -U posttrade >/dev/null 2>&1; do sleep 1; done

echo "== ingesting live ticks for ${INGEST_SECONDS}s =="
python3 -m posttrade.ingest.async_ingestor &
INGEST_PID=$!
sleep "$INGEST_SECONDS"
kill "$INGEST_PID" 2>/dev/null || true
wait "$INGEST_PID" 2>/dev/null || true

echo "== draining backlog with worker pool =="
python3 -c "
from posttrade.workers.worker_pool import run_worker_pool
print(run_worker_pool(max_idle_reads=2))
"

echo "== running trade book simulator =="
python3 -c "
from posttrade.simulate.trade_book_simulator import TradeBookSimulator
sim = TradeBookSimulator()
total = sim.run(max_idle_reads=3)
print('fills emitted:', total, 'anomalies:', sim.anomalies_planted)
"

echo "== reconciling and writing break report =="
python3 -m posttrade.report.report

echo "== done -- see reports/break_report.md =="
