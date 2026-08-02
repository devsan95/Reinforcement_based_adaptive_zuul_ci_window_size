#!/usr/bin/env bash
set -euo pipefail
curl -s -u admin:secret \
  -H 'Content-Type: application/json' \
  -X POST \
  -d '{"message":"recheck"}' \
  'http://localhost:8080/a/changes/22/revisions/current/review'
echo
sleep 25
echo "builds:"
curl -s 'http://localhost:19090/api/tenant/example-tenant/builds?change=22,1'
echo
echo "status:"
curl -s http://localhost:19090/api/tenant/example-tenant/status
echo
docker logs zuul-tutorial-scheduler-1 --tail 15 2>&1
