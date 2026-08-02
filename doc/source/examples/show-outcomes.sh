#!/usr/bin/env bash
# Print current research outcomes (safe to re-run anytime).
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock
ROOT="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "=========================================="
echo " LOCAL RESEARCH OUTCOMES"
echo "=========================================="
echo "Zuul:   http://localhost:19090/t/example-tenant/status"
echo "Gerrit: http://localhost:8080"
echo "Logs:   http://localhost:8000"
echo ""
echo "--- Open changes ---"
curl -s -u admin:secret 'http://localhost:8080/a/changes/?q=status:open&n=10' \
  | python3 -c "import sys,json; r=sys.stdin.read(); r=r[4:] if r.startswith(')]}') else r; [print(' ',c['_number'],c['project'],c['subject'][:50]) for c in json.loads(r or '[]')]"
echo ""
echo "--- Recent builds ---"
curl -s 'http://localhost:19090/api/tenant/example-tenant/builds?limit=15' \
  | python3 -c "import sys,json; [print(' ',b.get('job_name'),b.get('result'),b.get('change')) for b in json.loads(sys.stdin.read() or '[]')[:15]]"
echo ""
echo "--- RL audit (last 5) ---"
docker exec zuul-tutorial-scheduler-1 tail -5 /var/lib/zuul/rl_window_audit.jsonl 2>/dev/null || echo "  (wait 60s for agent tick)"
echo ""
echo "--- RL status API (gate) ---"
bash "$ROOT/check-rl-status.sh" 2>/dev/null || echo "  (run: bash check-rl-status.sh)"
echo ""
echo "--- Status page ---"
echo "  http://localhost:19090/t/example-tenant/status"
echo "  (blue RL banner + gate card RL badge)"
