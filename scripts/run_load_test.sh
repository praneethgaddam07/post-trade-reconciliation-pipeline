#!/usr/bin/env bash
# Runs the load harness ramp against synthetic data and writes the
# throughput/lag chart. See src/posttrade/loadtest/load_harness.py for the
# rate ladder and per-step duration/grace settings.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d
until docker compose exec -T redis redis-cli ping >/dev/null 2>&1; do sleep 1; done
until docker compose exec -T timescaledb pg_isready -U posttrade >/dev/null 2>&1; do sleep 1; done

mkdir -p reports
python3 -m posttrade.loadtest.load_harness | tee reports/load_test_raw.log
python3 -c "
import json, re
with open('reports/load_test_raw.log') as f:
    content = f.read()
json_start = content.index('\n[') + 1
with open('reports/load_test_results.json', 'w') as out:
    out.write(content[json_start:])
"
python3 -c "from posttrade.report.report import plot_load_test; plot_load_test('reports/load_test_results.json', 'reports/load_test_throughput_lag.png')"
echo "== done -- see reports/load_test_throughput_lag.png =="
