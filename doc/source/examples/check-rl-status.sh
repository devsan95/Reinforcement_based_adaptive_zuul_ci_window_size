#!/usr/bin/env bash
set -euo pipefail
python3 <<'PY'
import json, urllib.request
with urllib.request.urlopen('http://localhost:19090/api/tenant/example-tenant/status') as resp:
    d = json.load(resp)
for p in d.get('pipelines', []):
    if p.get('name') != 'gate':
        continue
    print('rl_window:', json.dumps(p.get('rl_window'), indent=2))
    for q in p.get('change_queues', []):
        if q.get('rl_recommended_window') is not None:
            print('queue', q.get('name'), 'window', q.get('window'),
                  'rl', q.get('rl_recommended_window'))
PY
