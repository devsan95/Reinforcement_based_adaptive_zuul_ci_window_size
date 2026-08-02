#!/usr/bin/env bash
# End-to-end local research demo: stack + bootstrap + traffic + outcomes.
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock
ROOT="$(cd "$(dirname "$0")" && pwd)"
ZROOT="$(cd "$ROOT/../../.." && pwd)"

fix_scripts() {
  sed -i 's/\r$//' "$ROOT"/*.sh 2>/dev/null || true
}

wait_services() {
  echo "=== Waiting for Gerrit and Zuul ==="
  for i in $(seq 1 60); do
    gcode=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/ || echo 000)
    zcode=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:19090/ || echo 000)
    echo "  attempt $i: gerrit=$gcode zuul=$zcode"
    if [ "$gcode" = "200" ] && [ "$zcode" = "200" ]; then
      return 0
    fi
    sleep 5
  done
  echo "ERROR: services did not become ready"
  exit 1
}

start_stack() {
  echo "=== Starting stack with RL patches (volume mount, no rebuild) ==="
  cd "$ROOT"
  docker compose -p zuul-tutorial up -d
  docker compose -f docker-compose.yaml -f docker-compose.research-mount.yaml \
    -p zuul-tutorial up -d --force-recreate scheduler
  wait_services
  sleep 10
}

bootstrap_config() {
  echo "=== Bootstrapping zuul-config (gate + research jobs) ==="
  cd "$ROOT"
  bash demo-change.sh 2>&1 | tee /tmp/zuul-demo-bootstrap.log
  echo "Restarting scheduler to pick up merged config..."
  docker restart zuul-tutorial-scheduler-1 >/dev/null
  sleep 35
}

generate_traffic() {
  echo "=== Generating check pipeline traffic ==="
  python3 "$ZROOT/research/traffic/generator.py" --count 3 || true
  echo "Waiting for check jobs..."
  sleep 40
  echo "=== Generating gate pipeline traffic ==="
  python3 "$ZROOT/research/traffic/generator.py" --count 3 --gate || true
  echo "Waiting for gate jobs and window adjustments..."
  sleep 60
}

show_outcomes() {
  echo ""
  echo "=========================================="
  echo " LOCAL RESEARCH OUTCOMES"
  echo "=========================================="
  echo ""
  echo "Zuul dashboard:  http://localhost:19090/t/example-tenant/status"
  echo "Gerrit:          http://localhost:8080"
  echo "Build logs:      http://localhost:8000"
  echo ""
  echo "--- Open Gerrit changes ---"
  curl -s -u admin:secret \
    'http://localhost:8080/a/changes/?q=status:open&n=10' \
    | python3 -c "
import sys, json
raw = sys.stdin.read()
if raw.startswith(')]}'): raw = raw[4:]
items = json.loads(raw or '[]')
for c in items:
    print(f\"  #{c['_number']} {c['project']}: {c['subject']} [{c['status']}]\")
if not items: print('  (none open)')
" 2>/dev/null || echo "  (could not fetch)"
  echo ""
  echo "--- Recent builds ---"
  curl -s 'http://localhost:19090/api/tenant/example-tenant/builds?limit=15' \
    | python3 -c "
import sys, json
items = json.loads(sys.stdin.read() or '[]')
for b in items[:15]:
    print(f\"  {b.get('job_name','?'):25s} {b.get('result','?'):8s} change={b.get('change','?')}\")
if not items: print('  (no builds yet - refresh dashboard in 1-2 min)')
" 2>/dev/null || echo "  (could not fetch)"
  echo ""
  echo "--- RL agent audit (last 5 lines) ---"
  docker exec zuul-tutorial-scheduler-1 \
    sh -c 'tail -5 /var/lib/zuul/rl_window_audit.jsonl 2>/dev/null' \
    2>/dev/null || echo "  (audit file not created yet - wait 60s for agent tick)"
  echo ""
  echo "--- Window / RL scheduler log ---"
  docker logs zuul-tutorial-scheduler-1 2>&1 \
    | grep -iE 'RL window|window size (increased|decreased|adjusted|held)' | tail -10 \
    || echo "  (no window log lines yet)"
  echo ""
  echo "Re-run traffic:  python3 $ZROOT/research/traffic/generator.py --count 5 --gate"
  echo "Live logs:       docker logs -f zuul-tutorial-scheduler-1"
}

fix_scripts
start_stack
bootstrap_config
generate_traffic
show_outcomes

# Allow a second pass after jobs finish
echo ""
echo "=== Waiting 300s for 200 gate jobs + RL agent ticks ==="
sleep 300
bash "$ROOT/show-outcomes.sh" | tail -40
