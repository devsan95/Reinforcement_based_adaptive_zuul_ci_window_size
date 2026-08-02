#!/usr/bin/env bash
# Demo varied gate scenarios and watch trained RL window predictions change.
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock

ROOT="$(cd "$(dirname "$0")" && pwd)"
ZROOT="$(cd "$ROOT/../../.." && pwd)"

sed -i 's/\r$//' "$ROOT"/*.sh 2>/dev/null || true

snapshot_rl() {
  local label="$1"
  echo ""
  echo "--- $label ---"
  python3 - "$label" <<'PY'
import json, sys, urllib.request
label = sys.argv[1]
with urllib.request.urlopen('http://localhost:19090/api/tenant/example-tenant/status') as r:
    data = json.load(r)
gate = next((p for p in data.get('pipelines', []) if p.get('name') == 'gate'), {})
rl = gate.get('rl_window') or {}
for q in rl.get('queues', []):
    print(f"  [{label}] queue={q.get('name')} current={q.get('current_window')} "
          f"recommended={q.get('recommended_window')} delta={q.get('action_delta')}")
for cq in gate.get('change_queues', []):
    if cq.get('rl_recommended_window') is not None:
        print(f"  [{label}] live window={cq.get('window')} "
              f"rl={cq.get('rl_recommended_window')} depth={cq.get('_count')}")
PY
  docker exec zuul-tutorial-scheduler-1 sh -c \
    "grep agent_tick /var/lib/zuul/rl_window_audit.jsonl 2>/dev/null | tail -1" \
    | python3 -c "import sys,json; l=sys.stdin.read();
print('  audit:', json.dumps({k:json.loads(l).get(k) for k in ('policy','action_delta','recommended_window','state')}) if l else '(no tick yet)')" 2>/dev/null || true
}

wait_tick() {
  echo "  waiting for RL tick (up to 70s)..."
  for i in $(seq 1 14); do
    if docker exec zuul-tutorial-scheduler-1 sh -c \
       'tail -1 /var/lib/zuul/rl_window_audit.jsonl 2>/dev/null | grep -q agent_tick'; then
      sleep 5
      return 0
    fi
    sleep 5
  done
}

echo "############################################"
echo "#  RL Policy Scenario Demo"
echo "############################################"

# Ensure policy is active
if ! grep -q '^mode=active' "$ROOT/etc_zuul/zuul.conf" 2>/dev/null; then
  echo "Policy not active yet — run: bash activate-rl-policy.sh"
  bash "$ROOT/activate-rl-policy.sh"
fi

RESULTS_DIR="$ZROOT/research/results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

mark_scenario() {
  echo "{\"scenario\":\"$1\",\"timestamp\":$(date +%s)}" >> "$RESULTS_DIR/markers.jsonl"
}

mark_scenario "baseline"
snapshot_rl "baseline"

echo ""
echo "=== Scenario 1: Gate burst (high queue depth) ==="
echo "Submit 8 gate changes quickly to fill the queue..."
python3 "$ZROOT/research/traffic/generator.py" --count 8 --gate
sleep 20
wait_tick
mark_scenario "after-burst"
snapshot_rl "after-burst"

echo ""
echo "=== Scenario 2: Steady success traffic (low failure rate) ==="
echo "Submit 3 more gate changes while queue drains..."
python3 "$ZROOT/research/traffic/generator.py" --count 3 --gate
sleep 45
wait_tick
mark_scenario "after-success-traffic"
snapshot_rl "after-success-traffic"

echo ""
echo "=== Scenario 3: Mixed check + gate load (executor util) ==="
python3 "$ZROOT/research/traffic/generator.py" --count 5
python3 "$ZROOT/research/traffic/generator.py" --count 2 --gate
sleep 30
wait_tick
mark_scenario "after-mixed-load"
snapshot_rl "after-mixed-load"

echo ""
echo "=== Recent RL decisions (policy + delta variety) ==="
AUDIT_LINES=$(docker exec zuul-tutorial-scheduler-1 sh -c \
  "grep agent_tick /var/lib/zuul/rl_window_audit.jsonl | tail -8")
printf '%s\n' "$AUDIT_LINES" | python3 <<'PY'
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    r = json.loads(line)
    state = r.get('state') or []
    depth = state[1] if len(state) > 1 else 0.0
    fail = state[2] if len(state) > 2 else 0.0
    delta = r.get('action_delta')
    print(f"  policy={r.get('policy','?')} delta={delta:+} "
          f"rec={r.get('recommended_window')} depth={depth:.2f} fail={fail:.2f}")
PY

echo ""
echo "=== RL vs TCP comparison report ==="
docker exec zuul-tutorial-scheduler-1 \
  cat /var/lib/zuul/rl_window_audit.jsonl > "$RESULTS_DIR/audit.jsonl"
docker compose \
  -f "$ROOT/docker-compose.yaml" \
  -f "$ROOT/docker-compose.research.yaml" \
  -f "$ROOT/docker-compose.research-app.yaml" \
  -p zuul-tutorial run --rm \
  -v "$RESULTS_DIR:/work" \
  rl-report \
  --audit /work/audit.jsonl \
  --markers /work/markers.jsonl \
  --output /work \
  --api-url http://web:9000

echo ""
echo "=== Publishing report in Zuul UI ==="
docker exec zuul-tutorial-web-1 sh -c \
  'mkdir -p /usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report'
docker cp "$RESULTS_DIR/report.html" \
  zuul-tutorial-web-1:/usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report/report.html
if [ -f "$RESULTS_DIR/throughput_graph.png" ]; then
  docker cp "$RESULTS_DIR/throughput_graph.png" \
    zuul-tutorial-web-1:/usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report/throughput_graph.png
fi
if [ -f "$RESULTS_DIR/comparison_table.csv" ]; then
  docker cp "$RESULTS_DIR/comparison_table.csv" \
    zuul-tutorial-web-1:/usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report/comparison_table.csv
fi
docker exec zuul-tutorial-web-1 sh -c \
  "printf '%s' \"$(date +%s)\" > /usr/local/lib/python3.11/site-packages/zuul/web/static/rl-report/version.txt"

echo ""
echo "=========================================="
echo " Comparison table: $RESULTS_DIR/comparison_table.csv"
echo " Throughput graph: $RESULTS_DIR/throughput_graph.png"
echo " HTML report:      $RESULTS_DIR/report.html"
echo " View live: http://localhost:19090/t/example-tenant/status"
echo " Banner shows [active] mode with trained recommendations."
echo " In active mode recommended window is applied to the gate queue."
echo "=========================================="
