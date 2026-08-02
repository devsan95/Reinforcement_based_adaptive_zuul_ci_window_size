#!/usr/bin/env bash
# Build and start the turnkey RL Zuul app (active mode, UI demo button, graphs).
set -euo pipefail

cd "$(dirname "$0")"

echo "Building and starting Zuul RL app..."
docker compose -f docker-compose.rl-app.yaml -p zuul-rl up -d --build

echo ""
echo "Waiting for bootstrap (up to 3 minutes)..."
for i in $(seq 1 36); do
  if docker compose -f docker-compose.rl-app.yaml -p zuul-rl ps rl-bootstrap 2>/dev/null \
     | grep -qE 'Exited \(0\)|exited \(0\)'; then
    echo "Bootstrap finished."
    break
  fi
  sleep 5
done

echo ""
echo "=========================================="
echo " Zuul RL app is up (active mode)"
echo "=========================================="
echo " Status page:  http://localhost:9090/t/example-tenant/status"
echo "               http://localhost:19090/t/example-tenant/status"
echo " Gerrit:       http://localhost:8080"
echo ""
echo " In the status page bottom-right panel:"
echo "   - Click 'Run demo'"
echo "   - Watch throughput + RL-vs-TCP window graphs"
echo ""
echo " Hard refresh once: Ctrl+Shift+R"
echo "=========================================="
