#!/usr/bin/env bash
# Export audit log and generate RL vs TCP comparison table + throughput graph.
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock

ROOT="$(cd "$(dirname "$0")" && pwd)"
ZROOT="$(cd "$ROOT/../../.." && pwd)"
sed -i 's/\r$//' "$ROOT"/*.sh 2>/dev/null || true

RUN_ID="${1:-$(date +%Y%m%d-%H%M%S)}"
OUT="$ZROOT/research/results/$RUN_ID"
mkdir -p "$OUT"

echo "=== Exporting RL audit log ==="
docker exec zuul-tutorial-scheduler-1 \
  cat /var/lib/zuul/rl_window_audit.jsonl > "$OUT/audit.jsonl" 2>/dev/null \
  || { echo "No audit log yet — run demo-rl-scenarios.sh first"; exit 1; }

if [ -f "$OUT/markers.jsonl" ]; then
  MARKERS=(--markers "$OUT/markers.jsonl")
else
  MARKERS=()
fi

echo "=== Building comparison report ==="
docker compose \
  -f docker-compose.yaml \
  -f docker-compose.research.yaml \
  -f docker-compose.research-app.yaml \
  -p zuul-tutorial run --rm \
  -v "$OUT:/work" \
  rl-report \
  --audit /work/audit.jsonl \
  --output /work \
  --api-url http://web:9000 \
  "${MARKERS[@]/$OUT/\/work}"

echo "=== Publishing report in Zuul UI ==="
docker exec zuul-tutorial-web-1 sh -c \
  'mkdir -p /usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report'
docker cp "$OUT/report.html" \
  zuul-tutorial-web-1:/usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report/report.html
if [ -f "$OUT/throughput_graph.png" ]; then
  docker cp "$OUT/throughput_graph.png" \
    zuul-tutorial-web-1:/usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report/throughput_graph.png
fi
if [ -f "$OUT/comparison_table.csv" ]; then
  docker cp "$OUT/comparison_table.csv" \
    zuul-tutorial-web-1:/usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report/comparison_table.csv
fi
docker exec zuul-tutorial-web-1 sh -c \
  "printf '%s' \"$(date +%s)\" > /usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report/version.txt"

echo ""
echo "Open report: file://$OUT/report.html"
echo "CSV table:   $OUT/comparison_table.csv"
echo "Graph:       $OUT/throughput_graph.png"
