#!/usr/bin/env bash
# Demo: RL recommended window sizes on the Zuul status page.
#
# Usage:
#   bash demo-rl-window.sh          # full demo (start stack if needed)
#   bash demo-rl-window.sh --quick  # skip stack start; only traffic + results
#
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock

ROOT="$(cd "$(dirname "$0")" && pwd)"
ZROOT="$(cd "$ROOT/../../.." && pwd)"
QUICK=false
if [ "${1:-}" = "--quick" ]; then
  QUICK=true
fi

sed -i 's/\r$//' "$ROOT"/*.sh 2>/dev/null || true

wait_http() {
  local name="$1" url="$2"
  for i in $(seq 1 40); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$url" || echo 000)
    if [ "$code" = "200" ]; then
      echo "  $name ready"
      return 0
    fi
    sleep 3
  done
  echo "ERROR: $name not ready at $url"
  exit 1
}

ensure_stack() {
  if [ "$QUICK" = true ]; then
    echo "=== Quick mode: assuming stack is already running ==="
    wait_http "Gerrit" "http://localhost:8080/"
    wait_http "Zuul"   "http://localhost:19090/"
    return
  fi

  echo "=== Starting Zuul tutorial stack with RL patches ==="
  cd "$ROOT"
  docker compose -p zuul-tutorial up -d
  docker compose -f docker-compose.yaml -f docker-compose.research-mount.yaml \
    -p zuul-tutorial up -d --force-recreate scheduler web
  wait_http "Gerrit" "http://localhost:8080/"
  wait_http "Zuul"   "http://localhost:19090/"
  echo "  Waiting for scheduler to prime..."
  sleep 25
}

submit_gate_traffic() {
  echo ""
  echo "=== Submitting gate traffic (creates queue activity) ==="
  python3 "$ZROOT/research/traffic/generator.py" --count 2 --gate
  echo "  Waiting for gate jobs to start..."
  sleep 25
}

wait_for_rl_tick() {
  echo ""
  echo "=== Waiting for RL agent tick (up to 75s) ==="
  for i in $(seq 1 15); do
    if docker exec zuul-tutorial-scheduler-1 \
        sh -c 'grep -q agent_tick /var/lib/zuul/rl_window_audit.jsonl 2>/dev/null'; then
      echo "  RL agent tick recorded (attempt $i)"
      return 0
    fi
    echo "  ... waiting ($((i * 5))s)"
    sleep 5
  done
  echo "  WARN: no agent_tick in audit yet; results may be empty"
}

show_rl_status_api() {
  echo ""
  echo "=== RL data from status API (powers the status page banner) ==="
  python3 <<'PY'
import json
import urllib.request

url = 'http://localhost:19090/api/tenant/example-tenant/status'
with urllib.request.urlopen(url) as resp:
    data = json.load(resp)

gate = next((p for p in data.get('pipelines', []) if p.get('name') == 'gate'), None)
if not gate:
    print('  (gate pipeline not found)')
    raise SystemExit(0)

rl = gate.get('rl_window')
if not rl or not rl.get('queues'):
    print('  (no RL recommendations yet — wait ~60s after scheduler start, then re-run)')
    print('  bash show-outcomes.sh')
    raise SystemExit(0)

print(f"  mode: {rl.get('mode')}  interval: {rl.get('interval')}s")
for q in rl['queues']:
    delta = q.get('action_delta')
    sign = '+' if delta and delta > 0 else ''
    print(f"  queue {q.get('name')}: current={q.get('current_window')}  "
          f"recommended={q.get('recommended_window')}  "
          f"delta={sign}{delta}")

for cq in gate.get('change_queues', []):
    if cq.get('rl_recommended_window') is not None:
        print(f"  live queue badge: {cq.get('_count', '?')} / {cq.get('window')} "
              f"→ RL {cq.get('rl_recommended_window')}")
PY
}

show_audit_tail() {
  echo ""
  echo "=== RL audit log (last 3 ticks) ==="
  docker exec zuul-tutorial-scheduler-1 \
    sh -c "grep agent_tick /var/lib/zuul/rl_window_audit.jsonl 2>/dev/null | tail -3" \
    || echo "  (no audit file yet)"
}

print_viewing_guide() {
  echo ""
  echo "=========================================="
  echo " VIEW THE DEMO IN YOUR BROWSER"
  echo "=========================================="
  echo ""
  echo "  Status page:  http://localhost:19090/t/example-tenant/status"
  echo "  Gate detail:  http://localhost:19090/t/example-tenant/status/pipeline/gate"
  echo ""
  echo "Look for:"
  echo "  1. Blue banner at top:"
  echo "     RL recommended window sizes — gate [shadow]: test1: current N → RL M (+2)"
  echo "  2. Gate pipeline card — orange 'RL <N>' badge next to item count"
  echo "  3. Queue card badge — 'items / window → RL <recommended>'"
  echo ""
  echo ""
echo "Hard-refresh the status page after running this script:"
echo "  Ctrl+Shift+R  (or Ctrl+F5) on http://localhost:19090/t/example-tenant/status"
  echo "Check API only:  bash check-rl-status.sh"
  echo "More traffic:    python3 $ZROOT/research/traffic/generator.py --count 3 --gate"
  echo ""
}

echo ""
echo "############################################"
echo "#  Zuul RL Window — Status Page Demo"
echo "############################################"
echo ""

ensure_stack
submit_gate_traffic
wait_for_rl_tick
show_rl_status_api
show_audit_tail
print_viewing_guide
