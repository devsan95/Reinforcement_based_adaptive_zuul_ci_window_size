#!/usr/bin/env bash
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock
cd "$(dirname "$0")"

echo "=== Resetting stack ==="
docker compose -p zuul-tutorial down -v
docker compose -p zuul-tutorial up -d

for i in $(seq 1 90); do
  gcode=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/ || echo 000)
  zcode=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:19090/ || echo 000)
  gc=$(docker inspect zuul-tutorial-gerritconfig-1 --format '{{.State.ExitCode}}' 2>/dev/null || echo x)
  echo "attempt $i gerrit=$gcode zuul=$zcode gerritconfig_exit=$gc"
  if [ "$gcode" = "200" ] && [ "$zcode" = "200" ] && [ "$gc" = "0" ]; then
    break
  fi
  sleep 5
done

echo "=== Bootstrapping and creating change ==="
bash "$(dirname "$0")/demo-change.sh"
sleep 30
bash "$(dirname "$0")/check-zuul.sh"
