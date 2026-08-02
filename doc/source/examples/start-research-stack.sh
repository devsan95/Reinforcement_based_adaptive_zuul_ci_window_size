#!/usr/bin/env bash
# Build and start the RL-patched Zuul research stack.
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock
cd "$(dirname "$0")"
bash start-stack.sh
docker compose -f docker-compose.yaml -f docker-compose.research.yaml \
  -f docker-compose.research-app.yaml \
  -p zuul-tutorial up -d --build scheduler web executor launcher rl-control
docker compose -f docker-compose.yaml -f docker-compose.research.yaml \
  -f docker-compose.research-app.yaml \
  -p zuul-tutorial build rl-report rl-control
echo "Research stack started. Run demo-change.sh to bootstrap Gerrit config."
