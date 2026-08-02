# RL Gate Window Research Stack

Implementation of the research proposal *Enhancing Zuul Throughput by Adaptive
Window Size using Reinforcement Learning* on the community Zuul Docker tutorial
stack.

## What is implemented

| Proposal item | Location |
|---------------|----------|
| `get_rl_state()` | `zuul/rl_window.py` |
| `set_window_from_api()` | `zuul/rl_window.py` |
| `_adjustWindow` / TCP fallback | `adjust_window_after_cycle()` hooked in `zuul/manager/__init__.py` |
| `start_rl_window_agent()` | `zuul/rl_window.py` + `zuul/scheduler.py` |
| Gate pipeline + window params | `doc/source/examples/zuul-config/zuul.d/gate-pipeline.yaml` |
| Synthetic jobs (5–40% failure) | `zuul-config/playbooks/research/` |
| Traffic generator | `research/traffic/generator.py` |
| PPO training shim (Gymnasium) | `research/training/` |
| Baseline evaluation | `research/training/evaluate_baselines.py` |
| RL vs TCP report | `research/analysis/compare_report.py` + `compare-rl-tcp.sh` |
| Audit / DB schema | JSONL audit + `research/sql/001_window_events.sql` |
| Unit tests | `tests/unit/test_rl_window.py` |

## Quick start

### 1. Start the patched research stack

```bash
cd doc/source/examples
bash start-stack.sh
docker compose -f docker-compose.yaml -f docker-compose.research.yaml \
  -p zuul-tutorial up -d --build scheduler web executor launcher
bash demo-change.sh
```

Zuul UI: http://localhost:19090  
Gerrit: http://localhost:8080

### 2. Generate gate traffic

```bash
# Check pipeline changes
python3 ../../../research/traffic/generator.py --count 10

# Gate pipeline changes (traditional: check → Verified+1 → Workflow+1 → gate)
python3 ../../../research/traffic/generator.py --count 10 --gate

# Legacy: Workflow+1 immediately on upload (skips Verified wait)
python3 ../../../research/traffic/generator.py --count 10 --immediate-gate
```

Set failure rate per job via Zuul vars or executor env `RESEARCH_FAILURE_RATE`.

### 3. Train PPO (offline)

```bash
cd ../../../research
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python training/train_ppo.py --steps 200000 --output models/ppo_gate_window.zip
python training/evaluate_baselines.py
```

Copy the trained model into the scheduler volume and switch mode:

```ini
[rl_window]
enabled=true
mode=active
policy_path=/var/lib/zuul/models/ppo_gate_window.zip
```

### 5. Compare RL vs TCP after a run

```bash
cd doc/source/examples
bash demo-rl-scenarios.sh          # runs scenarios + generates report
# or export an existing run:
bash compare-rl-tcp.sh
```

Outputs under `research/results/<timestamp>/`:
- `comparison_table.csv` — RL window, TCP shadow window, throughput per scenario
- `throughput_graph.png` — bar chart with % improvement vs baseline
- `report.html` — table + graph in browser

The report runs inside Docker (`rl-report` service), so no local Python deps needed.


```bash
cd ../../..   # zuul-community root
tox -e py311 -- tests/unit/test_rl_window.py
```

## RL modes

- `shadow` (default): agent recommends windows, TCP rule still governs merges
- `active`: agent override persists between 60s ticks
- `disabled`: pure TCP baseline

Audit records append to `/var/lib/zuul/rl_window_audit.jsonl`.

## Research phases mapping

1. **Data collection** – traffic generator + audit JSONL + optional MySQL `window_events`
2. **Embed agent** – scheduler hooks (this patch)
3. **Train/compare** – `research/training/*`
4. **Shadow eval** – run with `mode=shadow` for 3 weeks
5. **Analysis** – export audit JSONL to Polars/DuckDB

## License

Apache 2.0 (aligned with Zuul upstream).
