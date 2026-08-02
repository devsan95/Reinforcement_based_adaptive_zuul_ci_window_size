#!/usr/bin/env bash
# Pick a working Docker socket (WSL, Docker Desktop, native Linux, Podman).

if [ -z "${DOCKER_HOST:-}" ]; then
  if ! docker info >/dev/null 2>&1; then
    if [ -S /var/run/docker.sock ]; then
      export DOCKER_HOST=unix:///var/run/docker.sock
    elif [ -S "${HOME}/.docker/run/docker.sock" ]; then
      export DOCKER_HOST="unix://${HOME}/.docker/run/docker.sock"
    elif [ -S /mnt/wsl/shared-docker/docker.sock ]; then
      export DOCKER_HOST=unix:///mnt/wsl/shared-docker/docker.sock
    fi
  fi
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Cannot reach Docker daemon." >&2
  echo "  Start Docker Desktop (WSL integration on), or: sudo systemctl start docker" >&2
  echo "  Then verify: docker ps" >&2
  return 1 2>/dev/null || exit 1
fi
