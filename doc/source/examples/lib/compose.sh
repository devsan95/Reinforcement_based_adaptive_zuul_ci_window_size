#!/usr/bin/env bash
# Resolve docker compose vs docker-compose for tutorial scripts.
# shellcheck source=lib/docker-env.sh
source "$(dirname "${BASH_SOURCE[0]}")/docker-env.sh"
if docker compose version >/dev/null 2>&1; then
  compose() {
    docker compose "$@"
  }
elif command -v docker-compose >/dev/null 2>&1; then
  compose() {
    docker-compose "$@"
  }
else
  compose() {
    echo "ERROR: need 'docker compose' (plugin) or 'docker-compose' installed" >&2
    return 127
  }
fi
