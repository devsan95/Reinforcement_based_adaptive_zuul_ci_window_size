# RL vs TCP validation methodology

This document describes the optimized, **deterministic** approach for
demonstrating that the RL PPO gate-window agent outperforms Zuul's traditional
TCP window algorithm in the turnkey RL demo.

## What we compare

| Track | Role | Updated when |
|-------|------|--------------|
| **RL active window** | Applied to the gate pipeline (`mode=active`) | Agent ticks (every 10s) and after each gate merge cycle |
| **TCP shadow window** | Hypothetical TCP-only window (same rules as Zuul: grow on success, shrink on failure) | Each gate merge cycle (`tcp_shadow` audit events) |

Both start at **window 8** (floor 2, ceiling 50) on every **Run demo** reset.
Small initial windows make divergence visible fast: the gate queue only needs
~10 changes to saturate both windows.

### RL decision rules (vs TCP's blind shrink)

| Situation | TCP | RL |
|-----------|-----|----|
| Gate cycle fails | window **/ 2** (exponential) | **Hold** while rolling failure rate > 15% (a burst shouldn't halve throughput); shrink at most **−2** per tick otherwise |
| Gate cycle succeeds | +1 | ramp **+2** when failure rate is healthy |
| Always | — | applied window floored at **≥ TCP shadow** and clamped to pipeline floor/ceiling |

Every tick emits a human-readable `decision_reason` (audit log, `/live-metrics`
`latest.rl_decision_reason`, and the UI "Why (RL decision)" section).

## Continuous demo (primary mode)

| Setting | Value | Why |
|---------|-------|-----|
| `RL_DEFAULT_INITIAL_WINDOW` | **8** | Small baseline → queue saturates quickly |
| gate `window-floor` | **2** | Lets TCP visibly collapse under failures |
| `DEMO_BATCH_SIZE` | **10** | Base batch within ~50 capacity slots |
| `DEMO_MAX_BATCH_SIZE` | **20** | Cap for adaptive top-up batches |
| `DEMO_QUEUE_SATURATION_MARGIN` | **4** | Target queue depth = max(RL, TCP) + 4 |
| `DEMO_BATCH_INTERVAL_SEC` | **20** | Denser continuous traffic |
| `research_gate_sleep_sec` | **6** | Gate jobs run ~6s → queue stays deep |
| `DEMO_DURATION_SEC` | **300** | Default 5-minute session |
| Fail ratio | **half of each batch** | Deterministic, scales with batch size |
| Topics | `demo-burst`, `demo-steady`, `demo-depend` | Rotate each batch |
| Depends-On | half of `demo-depend` batches | Dependent gate queues |

### Adaptive traffic rule (queue saturation)

Before each batch the controller reads live gate state and sizes the batch so
the queue stays **above max(RL, TCP)**:

```
target      = max(rl_window, tcp_window) + DEMO_QUEUE_SATURATION_MARGIN
shortfall   = max(0, target − gate_queue_depth)
batch_size  = clamp(DEMO_BATCH_SIZE + shortfall, DEMO_BATCH_SIZE, DEMO_MAX_BATCH_SIZE)
fail_n      = round(batch_size / 2)      # deterministic ratio preserved
```

When the queue is saturated both windows are fully utilized, so
**Extra in flight == RL − TCP exactly**. `/live-metrics.latest` exposes
`queue_depth`, `queue_target` and `queue_saturated`; the UI shows
"✓ Queue saturated — comparison valid" or "◴ Queue draining — topping up".

**Fail/pass rule (deterministic):** the generator stamps
`should_fail: true/false` into each change's `demo-meta.json`
(first half of every batch fails). The gate job treats the stamp as
authoritative, so adaptive batch sizes keep an exact 50% fail ratio.

Expected during a successful continuous demo:

| Metric | Expected |
|--------|----------|
| Gate failures | ~half of each batch (accumulates over session) |
| Changes submitted | 10–20 every 20s (adaptive) |
| Gate queue depth | > max(RL, TCP) mid-run (saturation indicator on) |
| TCP shadow | Steps down on failures (e.g. 8 → 4 → 2) |
| RL window | Stays **≥ TCP**, holds ~8 through failure bursts |
| Extra in flight | **> 0** within ~2 minutes; equals RL − TCP when saturated |
| Minutes saved (est.) | > 0, bounded discrete estimate (not runaway) |

## Minutes saved (est.) — bounded formula

```
minutes_saved = sum_over_failures(max(0, RL_held − TCP_after))
                × avg_gate_job_duration_sec / 60
capped at session wall-clock minutes
```

- Counts **only gate failure events** (idle time does not accumulate)
- Uses **measured** avg job duration from finished builds when available
- Capped so values stay in realistic “minutes”, not hundreds

## Extra changes in flight

```
extra = max(0, min(gate_queue_depth, RL) − min(gate_queue_depth, TCP))
```

When the gate is saturated (`depth ≥ RL`) and `RL > TCP`, extra equals
`RL − TCP`. The story block never shows a misleading "N vs N": when windows
differ but the queue is shallow, it shows the window sizes and the top-up
state instead.

## Traditional flow: check → Verified+1 → gate

1. Traffic uploads a change (**no** Workflow yet) → enters **check**.
2. Fast check job (~1s) succeeds → Gerrit **Verified +1**.
3. Traffic applies **Workflow +1** → change enters **gate**.
4. Gate runs `research-gate-job` (~6s) with stamped `should_fail` pattern.

### Capacity

| Knob | Demo value | Where |
|------|------------|-------|
| Static provider `slots` | **50** | `zuul-config/zuul.d/providers.yaml` |
| `executor.max_starting_builds` | **50** | `etc_zuul/zuul.conf` |
| `RL_WINDOW_EXECUTOR_CAPACITY` | **50** | scheduler env |

Adaptive batches (≤ 20) stay within the 50-slot capacity.

## Demo flow (Run demo)

1. Reset RL/TCP windows to 8.
2. Clear gate queues.
3. Continuous adaptive traffic for 5 minutes (queue kept > max(RL, TCP) + 4).
4. **Extend session** adds more batches without cancelling in-flight builds.
5. Drain + publish comparison report.

## UI panel layout (rl-inject.html overlay)

1. **Demo Control** — Run demo / Extend session, phase, progress bar, watchdog warnings
2. **Live Comparison** — headline story ("TCP would run N — RL is running M (+K)"), RL/TCP window cards, gate queue card, failures, extra in flight, minutes saved, saturation indicator
3. **Why (RL decision)** — last `decision_reason` from the RL audit
4. **Charts** — RL vs TCP windows over time (failure markers, TCP-drop ▼, RL-held ●), advantage %, extra per failure, throughput
5. **Failures** — counter + per-failure list (change, TCP drop, RL held)
6. **Session Summary / Savings** — minutes saved, merges, comparison table, end-of-run card

## Apply commands

```powershell
cd C:\Users\vagga\Desktop\zuul-community\doc\source\examples

docker compose -f docker-compose.yaml -f docker-compose.rl-app.yaml -p zuul-rl up -d --build `
  scheduler web launcher executor node logs rl-control rl-bootstrap
docker exec zuul-rl-scheduler-1 zuul-scheduler full-reconfigure

# Push updated zuul-config (window 8, 6s gate job, stamped fail rule) to Gerrit
docker exec zuul-rl-rl-control-1 python -c "import server; server._sync_zuul_config_from_bundle()"

# Run continuous demo
curl -X POST http://127.0.0.1:19100/run-demo

# Extend (+1 batch)
curl -X POST http://127.0.0.1:19100/extend-demo -H "Content-Type: application/json" -d "{\"batches\":1}"

# Progress
curl http://127.0.0.1:19100/demo-progress
curl http://127.0.0.1:19100/live-metrics
# Open http://127.0.0.1:19090/t/example-tenant/status  (hard refresh)
```

## What success looks like

- Failures accumulate (~half of each batch)
- TCP shrinks (8 → 4 → 2); RL holds ≥ TCP with a stated reason
- Gate queue stays above max(RL, TCP) mid-run — saturation indicator on
- Extra in flight ≥ 1 sustained; equals RL − TCP when saturated
- Minutes saved (est.) grows with failures but stays ≤ session wall-clock minutes
