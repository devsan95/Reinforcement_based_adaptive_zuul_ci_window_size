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
| `research_gate_sleep_sec` | **1** | Gate jobs ~1s → savings shown as job-runs |
| `DEMO_DURATION_SEC` | **300** | Default 5-minute session (when Changes left empty) |
| Fail count | **user-set or half of each batch** | Absolute `gate_failures` via UI/API, else ratio |
| Topics | `demo-burst`, `demo-steady`, `demo-depend` | Rotate each batch |
| Depends-On | half of `demo-depend` batches | Dependent gate queues |

### Adaptive traffic rule (queue saturation)

Before each batch the controller reads live gate state and sizes the batch so
the queue stays **above max(RL, TCP)**:

```
target      = max(rl_window, tcp_window) + DEMO_QUEUE_SATURATION_MARGIN
shortfall   = max(0, target − gate_queue_depth)
batch_size  = clamp(DEMO_BATCH_SIZE + shortfall, DEMO_BATCH_SIZE, DEMO_MAX_BATCH_SIZE)
fail_n      = user gate_failures remaining  # or round(batch_size / 2)
```

When the queue is saturated both windows are fully utilized, so
**Extra in flight == RL − TCP exactly**. `/live-metrics.latest` exposes
`queue_depth`, `queue_target` and `queue_saturated`; the UI shows
"✓ Queue saturated — comparison valid" or "◴ Queue draining — topping up".

**Fail/pass rule (deterministic):** the generator stamps
`should_fail: true/false` into each change's `demo-meta.json`.
With no UI/API targets, the first half of every batch fails (50% ratio).
When `/run-demo` receives `total_changes` / `gate_failures`, fails are
distributed so the session stamps exactly the requested count.

Optional Run demo body (empty body keeps duration defaults):

```json
{"total_changes": 100, "gate_failures": 10}
```

Expected during a successful continuous demo:

| Metric | Expected |
|--------|----------|
| Gate failures | user target, or ~half of each batch |
| Changes submitted | user `total_changes`, or 10–20 every 20s |
| Gate queue depth | > max(RL, TCP) mid-run (saturation indicator on) |
| TCP shadow | Steps down on failures (e.g. 8 → 4 → 2) |
| RL window | Stays **≥ TCP**, holds ~8 through failure bursts |
| Extra in flight | **> 0** within ~2 minutes; equals RL − TCP when saturated |
| Job-runs saved (est.) | = extras total; ≈ extras × 1s gate jobs |

## Job-runs saved (est.) — same extras sum

```
extra_changes_total = sum_over_failures(max(0, RL_held − TCP_after))
job_runs_saved      = extra_changes_total   # ≈ serial 1s gate job-runs
minutes_saved       = job_runs_saved × avg_gate_job_duration_sec / 60
                      capped at session wall-clock minutes
```

- Counts **only gate failure events** (idle time does not accumulate)
- Uses **measured** avg job duration from finished builds when available
- With ~1s gate jobs, prefer the **Job-runs saved** label in the UI
- Capped so minute values stay realistic when job duration is longer

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
4. Gate runs `research-gate-job` (~1s) with stamped `should_fail` pattern.

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
3. Continuous adaptive traffic for 5 minutes (or until `total_changes`),
   queue kept > max(RL, TCP) + 4; fail stamps honor `gate_failures` when set.
4. **Extend session** adds more batches without cancelling in-flight builds.
5. Drain + publish comparison report.

## UI panel layout (rl-inject.html overlay)

1. **Demo Control** — Changes / Fails inputs (prefilled with server defaults; Fails = gate job failures, not merge conflicts), Run demo / Extend session, phase, progress bar, watchdog warnings
2. **Session summary** — Submitted, Merged, Failed (gate) X/Y (Y = expected; stamped / speculative fails, not git merge conflicts), Extra vs TCP (`extra_changes_total`), Advantage %, Peak RL / TCP floor (each metric once; job-runs = extras, shown in After run)
3. **Live & decision** — live RL/TCP/queue + plain-English RL reason (method/kNN only in debug line)
4. **Charts** — full-session RL vs TCP windows, advantage trend, extras per failure, throughput, plus an **After run** comparison aligned with `session_summary` (submitted / merged / failed gate / extras)
5. **Recent failures** — per-failure list only (count lives in Session summary)

`PANEL_LAYOUT_VERSION` in the overlay forces a panel rebuild after hard-refresh when the DOM structure changes.

## Apply commands

```powershell
cd C:\Users\vagga\Desktop\zuul-community\doc\source\examples

# Rebuild web (overlay + inject) and control (API) when those changed
docker compose -f docker-compose.yaml -f docker-compose.rl-app.yaml -p zuul-rl up -d --build `
  scheduler web launcher executor node logs rl-control rl-bootstrap

# Overlay-only iterate: rl-inject.html is bind-mounted; recreate web to re-inject
# docker compose -f docker-compose.yaml -f docker-compose.rl-app.yaml -p zuul-rl up -d --force-recreate web

docker exec zuul-rl-scheduler-1 zuul-scheduler full-reconfigure

# Push updated zuul-config (window 8, 1s gate job, stamped fail rule) to Gerrit
docker exec zuul-rl-rl-control-1 python -c "import server; server._sync_zuul_config_from_bundle()"

# Run continuous demo (defaults)
curl -X POST http://127.0.0.1:19100/run-demo

# Or set absolute targets
curl -X POST http://127.0.0.1:19100/run-demo -H "Content-Type: application/json" `
  -d "{\"total_changes\":100,\"gate_failures\":10}"

# Extend (+1 batch)
curl -X POST http://127.0.0.1:19100/extend-demo -H "Content-Type: application/json" -d "{\"batches\":1}"

# Progress
curl http://127.0.0.1:19100/demo-progress
curl http://127.0.0.1:19100/live-metrics
# Open http://127.0.0.1:19090/t/example-tenant/status  (hard refresh Ctrl+Shift+R)
```

## What success looks like

- Failures accumulate to the user target (or ~half of each batch)
- TCP shrinks (8 → 4 → 2); RL holds ≥ TCP with a stated reason
- Gate queue stays above max(RL, TCP) mid-run — saturation indicator on
- Extra in flight ≥ 1 sustained; equals RL − TCP when saturated
- Job-runs saved (est.) = extras total and grows with failures
