#!/usr/bin/env bash
set -euo pipefail

export DOCKER_HOST=unix:///var/run/docker.sock
WORKDIR=$(mktemp -d)
SRC="$(cd "$(dirname "$0")" && pwd)"
trap 'rm -rf "$WORKDIR"' EXIT

gerrit_review_submit() {
  local project=$1 change_num=$2
  curl -s -u admin:secret \
    -H 'Content-Type: application/json' \
    -X POST \
    -d '{"labels":{"Code-Review":2,"Verified":2,"Workflow":1},"message":"Auto merge for demo"}' \
    "http://localhost:8080/a/changes/${project}~${change_num}/revisions/current/review" >/dev/null
  curl -s -u admin:secret -X POST \
    "http://localhost:8080/a/changes/${project}~${change_num}/submit" >/dev/null
}

setup_git_repo() {
  local repo=$1
  git -C "$WORKDIR/$repo" config user.email admin@example.com
  git -C "$WORKDIR/$repo" config user.name Admin
  curl -s -o "$WORKDIR/$repo/.git/hooks/commit-msg" \
    http://localhost:8080/tools/hooks/commit-msg
  chmod +x "$WORKDIR/$repo/.git/hooks/commit-msg"
}

extract_change_num() {
  sed -n 's/.*\/c\/[^/]*\/+\/\([0-9]*\).*/\1/p' | tail -1
}

PIPELINES=$(curl -s http://localhost:19090/api/tenant/example-tenant/pipelines)
if [ "$PIPELINES" = "[]" ]; then
  echo "=== Bootstrapping zuul-config ==="
  git clone http://admin:secret@localhost:8080/zuul-config "$WORKDIR/zuul-config"
  cp -a "$SRC/zuul-config/zuul.d" "$WORKDIR/zuul-config/"
  cp -a "$SRC/zuul-config/playbooks" "$WORKDIR/zuul-config/"
  setup_git_repo zuul-config
  git -C "$WORKDIR/zuul-config" add zuul.d playbooks
  git -C "$WORKDIR/zuul-config" commit -m "Add initial Zuul configuration"
  PUSH_OUT=$(git -C "$WORKDIR/zuul-config" push \
    http://admin:secret@localhost:8080/zuul-config HEAD:refs/for/master 2>&1)
  echo "$PUSH_OUT"
  CHANGE=$(echo "$PUSH_OUT" | extract_change_num)
  [ -n "$CHANGE" ] || CHANGE=1
  gerrit_review_submit zuul-config "$CHANGE"
  echo "Merged zuul-config change ${CHANGE}"
  sleep 25
fi

echo "=== Creating test1 change ==="
git clone http://admin:secret@localhost:8080/test1 "$WORKDIR/test1"
cp "$SRC/test1/zuul.yaml" "$WORKDIR/test1/"
mkdir -p "$WORKDIR/test1/playbooks"
cp "$SRC/test1/playbooks/testjob.yaml" "$WORKDIR/test1/playbooks/"
echo "# Zuul demo $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$WORKDIR/test1/README.md"
setup_git_repo test1
git -C "$WORKDIR/test1" add .
git -C "$WORKDIR/test1" commit -m "Add test Zuul job and demo change"
PUSH_OUT=$(git -C "$WORKDIR/test1" push \
  http://admin:secret@localhost:8080/test1 HEAD:refs/for/master 2>&1)
echo "$PUSH_OUT"
CHANGE=$(echo "$PUSH_OUT" | extract_change_num)
echo "TEST1_CHANGE=${CHANGE}"
echo "GERRIT_URL=http://localhost:8080/c/test1/+/${CHANGE}"
echo "ZUUL_URL=http://localhost:19090/t/example-tenant/status/change/${CHANGE},1"
sleep 20
echo "=== Zuul pipelines ==="
curl -s http://localhost:19090/api/tenant/example-tenant/pipelines
echo
echo "=== Change status ==="
curl -s "http://localhost:19090/api/tenant/example-tenant/status/changes?change=${CHANGE},1"
