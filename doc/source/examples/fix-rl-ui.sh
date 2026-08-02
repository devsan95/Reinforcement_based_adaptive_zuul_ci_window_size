#!/usr/bin/env bash
# Fix RL banner + badges on the Zuul status page (re-mount web overlay).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
sed -i 's/\r$//' "$ROOT"/*.sh "$ROOT/lib"/*.sh 2>/dev/null || true
source "$ROOT/lib/compose.sh"

cd "$ROOT"
echo "=== Recreating web + scheduler with RL overlay ==="
compose -f docker-compose.yaml -f docker-compose.research-mount.yaml \
  -p zuul-tutorial up -d --force-recreate web scheduler

echo "=== Waiting for services ==="
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:19090/ || echo 000)
  if [ "$code" = "200" ]; then break; fi
  sleep 2
done

echo "=== Verify overlay in container + HTTP ==="
if docker exec zuul-tutorial-web-1 grep -q zuul-rl-window \
    /usr/local/lib/python3.11/site-packages/zuul/web/static/index.html 2>/dev/null; then
  echo "  OK: RL overlay in container index.html"
else
  echo "  Copying overlay directly into container..."
  docker cp "$ROOT/web-overlay/index.html" \
    zuul-tutorial-web-1:/usr/local/lib/python3.11/site-packages/zuul/web/static/index.html
  compose -f docker-compose.yaml -f docker-compose.research-mount.yaml \
    -p zuul-tutorial restart web
  sleep 15
fi

if curl -s http://localhost:19090/ | grep -q zuul-rl-window; then
  echo "  OK: RL overlay served over HTTP"
else
  echo "  FAIL: overlay still missing after copy"
  echo "  Check: docker exec zuul-tutorial-web-1 head -20 /usr/local/lib/python3.11/site-packages/zuul/web/static/index.html"
  exit 1
fi

bash "$ROOT/check-rl-status.sh" || true

echo ""
echo "Open (hard-refresh Ctrl+Shift+R):"
echo "  http://localhost:19090/t/example-tenant/status"
echo "  http://localhost:9090/t/example-tenant/status"
