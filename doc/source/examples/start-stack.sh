#!/usr/bin/env bash
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock
cd "$(dirname "$0")"
docker compose -p zuul-tutorial up -d
for i in $(seq 1 60); do
  gcode=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/ || echo 000)
  zcode=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:19090/ || echo 000)
  echo "attempt $i gerrit=$gcode zuul=$zcode"
  if [ "$gcode" = "200" ] && [ "$zcode" = "200" ]; then
    exit 0
  fi
  sleep 5
done
echo "Timed out waiting for services"
exit 1
