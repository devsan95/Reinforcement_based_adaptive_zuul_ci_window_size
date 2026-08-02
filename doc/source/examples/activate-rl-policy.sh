#!/usr/bin/env bash
# Activate trained PPO policy on the live scheduler (active mode).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
ZROOT="$(cd "$ROOT/../../.." && pwd)"
TABLE="$ZROOT/research/models/ppo_gate_window_table.json"
sed -i 's/\r$//' "$ROOT"/*.sh "$ROOT/lib"/*.sh "$ZROOT/research"/*.sh 2>/dev/null || true
source "$ROOT/lib/compose.sh"

if [ ! -f "$TABLE" ]; then
  echo "No policy table found — training first (may take a few minutes)..."
  bash "$ZROOT/research/train-and-export.sh" 80000
fi

echo "=== Switching zuul.conf to active + trained policy ==="
CONF="$ROOT/etc_zuul/zuul.conf"
python3 <<PY
from pathlib import Path
import re
text = Path("$CONF").read_text()
text = re.sub(r'^mode=.*$', 'mode=active', text, flags=re.M)
text = re.sub(r'^policy_path=.*$', 'policy_path=/var/lib/zuul/models/ppo_gate_window_table.json', text, flags=re.M)
Path("$CONF").write_text(text)
print("Updated $CONF:")
for line in text.splitlines():
    if line.startswith(("mode=", "policy_path=", "enabled=")):
        print(" ", line)
PY

echo "=== Restarting scheduler + web with RL mounts ==="
cd "$ROOT"
compose -f docker-compose.yaml -f docker-compose.research-mount.yaml \
  -p zuul-tutorial up -d --force-recreate scheduler web

echo "=== Waiting for scheduler + policy load ==="
sleep 40
docker logs zuul-tutorial-scheduler-1 2>&1 | grep -iE 'RL window|policy table|policy from' | tail -5

echo ""
echo "Trained policy is ACTIVE. Window overrides now apply to the gate queue."
echo "Status page: http://localhost:19090/t/example-tenant/status"
echo "           or http://localhost:9090/t/example-tenant/status"
echo "Hard-refresh: Ctrl+Shift+R"
echo "Run scenarios: bash demo-rl-scenarios.sh"
