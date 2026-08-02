#!/usr/bin/env bash
# Clone upstream community Zuul repo inside a container and apply local research changes.
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock

ROOT="$(cd "$(dirname "$0")" && pwd)"
LOCAL_REPO="$(cd "$ROOT/../../.." && pwd)"
TARGET_PARENT="${1:-$LOCAL_REPO/containerized-src}"
TARGET_REPO="$TARGET_PARENT/zuul"

mkdir -p "$TARGET_PARENT"

echo "=== Clone upstream Zuul in container ==="
docker run --rm \
  -v "$TARGET_PARENT:/work" \
  alpine/git sh -c '
    set -e
    rm -rf /work/zuul
    git clone --depth 1 https://opendev.org/zuul/zuul /work/zuul
  '

echo "=== Apply local research changes ==="
docker run --rm \
  -v "$LOCAL_REPO:/src:ro" \
  -v "$TARGET_REPO:/dst" \
  alpine sh -c '
    set -e
    mkdir -p /dst/research /dst/doc/source/examples /dst/zuul /dst/tests/unit
    cp -a /src/research/. /dst/research/
    cp -a /src/doc/source/examples/. /dst/doc/source/examples/
    cp -a /src/zuul/rl_window.py /dst/zuul/rl_window.py
    cp -a /src/zuul/scheduler.py /dst/zuul/scheduler.py
    cp -a /src/zuul/manager/__init__.py /dst/zuul/manager/__init__.py
    cp -a /src/tests/unit/test_rl_window.py /dst/tests/unit/test_rl_window.py
  '

echo "=== Force active RL mode ==="
docker run --rm \
  -v "$TARGET_REPO:/repo" \
  python:3.11-alpine sh -c '
    python - <<PY
from pathlib import Path
import re
conf = Path("/repo/doc/source/examples/etc_zuul/zuul.conf")
text = conf.read_text()
text = re.sub(r"^mode=.*$", "mode=active", text, flags=re.M)
text = re.sub(r"^policy_path=.*$",
              "policy_path=/var/lib/zuul/models/ppo_gate_window_table.json",
              text, flags=re.M)
conf.write_text(text)
print("Updated:", conf)
PY
  '

echo ""
echo "Containerized source prepared at:"
echo "  $TARGET_REPO"
echo ""
echo "Next:"
echo "  cd \"$TARGET_REPO/doc/source/examples\""
echo "  bash start-research-stack.sh"
