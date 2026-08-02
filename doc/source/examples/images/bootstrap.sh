#!/bin/sh
# Bootstrap: load check→gate research Zuul config and keep it in sync with /bootstrap.
set -eu

GERRIT="${GERRIT_URL:-http://gerrit:8080}"
ZUUL="${ZUUL_URL:-http://web:9000}"
AUTH="admin:secret"
WORKDIR=/tmp/bootstrap
MARKER=/var/lib/bootstrap/done
POLL_SEC=2

mkdir -p "$(dirname "$MARKER")"

sync_zuul_config() {
  echo "Syncing zuul-config from bootstrap bundle..."
  rm -rf "$WORKDIR"
  mkdir -p "$WORKDIR"
  git clone "http://${AUTH}@${GERRIT#http://}/zuul-config" "$WORKDIR/zuul-config"
  rm -rf "$WORKDIR/zuul-config/zuul.d" "$WORKDIR/zuul-config/playbooks"
  cp -a /bootstrap/zuul-config/zuul.d "$WORKDIR/zuul-config/"
  cp -a /bootstrap/zuul-config/playbooks "$WORKDIR/zuul-config/"
  if [ -f /bootstrap/zuul-config/.zuul.yaml ]; then
    cp -a /bootstrap/zuul-config/.zuul.yaml "$WORKDIR/zuul-config/"
  else
    rm -f "$WORKDIR/zuul-config/.zuul.yaml"
  fi
  git -C "$WORKDIR/zuul-config" config user.email admin@example.com
  git -C "$WORKDIR/zuul-config" config user.name Admin
  curl -s -o "$WORKDIR/zuul-config/.git/hooks/commit-msg" \
    "$GERRIT/tools/hooks/commit-msg"
  chmod +x "$WORKDIR/zuul-config/.git/hooks/commit-msg"
  git -C "$WORKDIR/zuul-config" add -A
  if git -C "$WORKDIR/zuul-config" diff --cached --quiet; then
    echo "zuul-config already up to date."
    return 0
  fi
  git -C "$WORKDIR/zuul-config" commit -m "Update RL research Zuul configuration"
  PUSH_OUT=$(git -C "$WORKDIR/zuul-config" push \
    "http://${AUTH}@${GERRIT#http://}/zuul-config" \
    HEAD:refs/for/master 2>&1) || true
  CHANGE=$(echo "$PUSH_OUT" | sed -n 's/.*\/+\/\([0-9]*\).*/\1/p' | tail -1)
  [ -n "$CHANGE" ] || CHANGE=1
  curl -s -u "$AUTH" -H 'Content-Type: application/json' -X POST \
    -d '{"labels":{"Code-Review":2,"Verified":2,"Workflow":1}}' \
    "$GERRIT/a/changes/zuul-config~${CHANGE}/revisions/current/review" >/dev/null
  curl -s -u "$AUTH" -X POST \
    "$GERRIT/a/changes/zuul-config~${CHANGE}/submit" >/dev/null
  echo "Merged zuul-config change ${CHANGE}"
  sleep 15
  return 0
}

wait_for_check_then_gate_layout() {
  echo "Waiting for check→gate layout (test1: research-check-job + research-gate-job)..."
  deadline=$(( $(date +%s) + 90 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if curl -sf "$ZUUL/api/tenant/example-tenant/project/test1" 2>/dev/null \
        | python3 -c "
import json, sys
data = json.load(sys.stdin)
check = []
gate = []
for cfg in data.get('configs', []):
    for p in cfg.get('pipelines', []):
        jobs = [j.get('name') for grp in p.get('jobs', []) for j in grp if j.get('name')]
        if p.get('name') == 'check':
            check.extend(jobs)
        if p.get('name') == 'gate':
            gate.extend(jobs)
sys.exit(0 if 'research-check-job' in check and 'research-gate-job' in gate else 1)
" 2>/dev/null; then
      echo "check → Verified+1 → gate layout verified."
      return 0
    fi
    sleep 3
  done
  echo "WARNING: check→gate layout not confirmed within 90s."
  return 1
}

echo "Waiting for Gerrit and Zuul..."
for i in $(seq 1 60); do
  gcode=$(curl -s -o /dev/null -w '%{http_code}' "$GERRIT/" || echo 000)
  zcode=$(curl -s -o /dev/null -w '%{http_code}' "$ZUUL/" || echo 000)
  if [ "$gcode" = "200" ] && [ "$zcode" = "200" ]; then
    break
  fi
  sleep "$POLL_SEC"
done

PIPELINES=$(curl -s "$ZUUL/api/tenant/example-tenant/pipelines" 2>/dev/null || echo '[]')
if [ "$PIPELINES" = "[]" ]; then
  sync_zuul_config
  for i in $(seq 1 30); do
    PIPELINES=$(curl -s "$ZUUL/api/tenant/example-tenant/pipelines" || echo '[]')
    if [ "$PIPELINES" != "[]" ]; then
      break
    fi
    sleep "$POLL_SEC"
  done
else
  sync_zuul_config
fi

wait_for_check_then_gate_layout || true

PIPELINES=$(curl -s "$ZUUL/api/tenant/example-tenant/pipelines" || echo '[]')
if [ "$PIPELINES" = "[]" ]; then
  echo "ERROR: pipelines still empty after bootstrap."
  exit 1
fi

echo "Bootstrap complete. Pipelines loaded."
touch "$MARKER"
