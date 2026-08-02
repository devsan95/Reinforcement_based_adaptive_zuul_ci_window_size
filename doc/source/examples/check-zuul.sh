#!/usr/bin/env bash
set -euo pipefail
curl -s http://localhost:19090/api/tenant/example-tenant/jobs | tr ',' '\n' | grep -E 'noop|base|testjob' || true
echo "--- status ---"
curl -s http://localhost:19090/api/tenant/example-tenant/status
echo
echo "--- builds for 22,1 ---"
curl -s 'http://localhost:19090/api/tenant/example-tenant/builds?change=22,1'
echo
echo "--- enqueue ---"
docker exec zuul-tutorial-scheduler-1 zuul-admin enqueue --help 2>&1 | head -15
