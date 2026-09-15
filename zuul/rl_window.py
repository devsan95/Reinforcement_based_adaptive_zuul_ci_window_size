# Copyright 2026 Santoshkumar Vagga
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reinforcement-learning gate window control for Zuul CI.

Implements the embedded PPO agent interface described in the research
proposal: state observation, persistent window override, TCP fallback,
and audit logging.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

try:
    import numpy as np
except ImportError:  # pragma: no cover - optional at runtime
    np = None

log = logging.getLogger("zuul.RLWindow")

ACTION_DELTAS = (-2, -1, 0, 1, 2)

# ---------------------------------------------------------------------------
# State design (input → prediction pipeline)
#
# The agent observes a 6-feature vector, every feature clipped to [0, 1]:
#
#   idx  label                definition
#   0    norm_window          window / ceiling
#   1    queue_saturation     min(queue_depth / (2 * ceiling), 1) — how full
#                             the gate queue is relative to twice the max
#                             window
#   2    failure_rate         exponentially-decayed failure rate over the last
#                             FAILURE_MAX_CYCLES cycles within
#                             FAILURE_WINDOW_SECONDS (recency-weighted; a
#                             5-min demo is not dominated by 30-min-old
#                             outcomes)
#   3    success_streak       consecutive successful cycles / 10, clipped
#   4    executor_available   READY/unused slots / capacity (available_ratio).
#                             Replaces the dead executor_util that read
#                             ExecutorClient.running_builds (the scheduler
#                             client has no such field, so util was always 0).
#                             Occupancy = 1 - executor_available.
#   5    queue_pressure       min(queue_depth / window, 1) — matches the 6th
#                             feature the offline policy table was trained
#                             with
#
# The legacy state used hour_sin/hour_cos (time of day) at idx 3/4. Those
# were noise for a demo and, worse, dominated the nearest-neighbour distance
# against a table exported at fixed hour values. They are dropped; table
# queries pin them to the grid constants with zero weight (see
# TABLE_FEATURE_WEIGHTS / _table_query). Live idx 4 (executor_available) is
# also dropped from the table query — the exported grid's 6th dim is
# queue/window, not nodepool occupancy. Heuristic, guardrails, and the
# post-policy capacity clamp honour availability even when kNN ignores it.
# ---------------------------------------------------------------------------
STATE_SIZE = 6
STATE_LABELS = (
    "norm_window", "queue_saturation", "failure_rate",
    "success_streak", "executor_available", "queue_pressure",
)
# Recency-weighted failure rate: only the last FAILURE_MAX_CYCLES outcomes
# within FAILURE_WINDOW_SECONDS count, each decayed with FAILURE_HALF_LIFE.
FAILURE_WINDOW_SECONDS = float(
    os.environ.get("RL_FAILURE_WINDOW_SEC", "180"))
FAILURE_HALF_LIFE_SECONDS = float(
    os.environ.get("RL_FAILURE_HALFLIFE_SEC", "90"))
FAILURE_MAX_CYCLES = int(os.environ.get("RL_FAILURE_MAX_CYCLES", "10"))
SUCCESS_STREAK_NORM = 10.0
# Legacy 30-min rolling window (kept for reference/compat in audit).
ROLLING_WINDOW_SECONDS = 30 * 60

# Policy-table lookup: weighted k-NN vote instead of a single unweighted
# nearest neighbour. Weights de-emphasise features the table grid did not
# vary (hour slots weight 0) and emphasise the failure signal.
# Table feature layout: [norm_w, norm_d(=queue/ceiling), fail,
#                        hour_sin, hour_cos, util(=queue/window)].
# Live executor_available is not in this layout (weight would be wasted);
# availability is applied via heuristic/guards + capacity clamp.
TABLE_FEATURE_WEIGHTS = (1.0, 1.0, 2.0, 0.0, 0.0, 1.0)
KNN_K = int(os.environ.get("RL_KNN_K", "5"))
# If even the best (weighted) neighbour is farther than this, the table has
# no relevant experience for the current state → heuristic fallback.
KNN_MAX_DISTANCE = float(os.environ.get("RL_KNN_MAX_DISTANCE", "0.35"))

# Small demo baseline (8): with 10-change batches the gate queue exceeds the
# window immediately, so in-flight counts (min(queue, window)) differentiate
# RL vs TCP as soon as the first failure shrinks the TCP shadow.
DEFAULT_INITIAL_WINDOW = int(os.environ.get("RL_DEFAULT_INITIAL_WINDOW", "8"))
# Demo divergence floor: when the recent failure rate exceeds this, the RL
# agent refuses shrink actions (holds instead) while the TCP shadow keeps
# shrinking exponentially. Combined with the RL >= TCP floor enforcement,
# this guarantees a visible RL-over-TCP gap in every demo run with failures.
# This is intentional demo behaviour, not a claim about the trained policy.
DEMO_HOLD_FAILURE_RATE = float(
    os.environ.get("RL_DEMO_HOLD_FAILURE_RATE", "0.15"))
# Hold / refuse growth when this fraction or fewer of slots are READY.
EXECUTOR_HOLD_AVAILABLE = float(
    os.environ.get("RL_EXECUTOR_HOLD_AVAILABLE", "0.2"))
# Allow growth when at least this fraction of slots are READY (and the
# queue is deep with low failures).
EXECUTOR_GROW_AVAILABLE = float(
    os.environ.get("RL_EXECUTOR_GROW_AVAILABLE", "0.35"))
# Reuse launcher/nodepool observations for this many seconds so a burst
# of cycle ticks does not list ZK nodes on every call.
EXECUTOR_SNAPSHOT_TTL = float(
    os.environ.get("RL_EXECUTOR_SNAPSHOT_TTL", "2"))

# ProviderNode / Nodepool states used when counting job slots.
_SLOT_READY = "ready"
_SLOT_IN_USE = "in-use"
_SLOT_HOST = "slot-host"
_SLOT_BUILDING = "building"
_SLOT_REQUESTED = "requested"


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


@dataclass
class QueueMetrics:
    outcomes: Deque[Tuple[float, bool]] = field(
        default_factory=lambda: deque(maxlen=500))
    fallback_count: int = 0
    decision_count: int = 0


@dataclass
class QueueOverride:
    size: int
    source: str
    set_at: float
    expires_at: Optional[float] = None


@dataclass
class ExecutorSnapshot:
    """Live nodepool/launcher slot counts for the RL window agent."""

    capacity: int
    available: int
    in_use: int
    available_ratio: float
    occupancy: float
    source: str  # launcher | nodepool | executor_api | running_builds | env


def _env_executor_capacity() -> int:
    return max(1, int(os.environ.get("RL_WINDOW_EXECUTOR_CAPACITY", "4")))


def _executor_audit_fields(snapshot: Optional[ExecutorSnapshot]) -> dict:
    """Named nodepool fields for audit / live-metrics (full-session chart).

    ``executor_available`` is the 0–1 ratio (state dim 4). Integer free
    slots live on ``available_slots``.
    """
    if snapshot is None:
        return {
            "available_slots": None,
            "executor_capacity": None,
            "executor_in_use": None,
            "executor_available": None,
            "executor_occupancy": None,
            "executor_source": None,
        }
    return {
        "available_slots": int(snapshot.available),
        "executor_capacity": int(snapshot.capacity),
        "executor_in_use": int(snapshot.in_use),
        "executor_available": float(snapshot.available_ratio),
        "executor_occupancy": float(snapshot.occupancy),
        "executor_source": snapshot.source,
    }


class WindowController:
    """Thread-safe controller for RL window overrides and metrics."""

    def __init__(self):
        self._lock = threading.RLock()
        self._overrides: Dict[str, QueueOverride] = {}
        self._metrics: Dict[str, QueueMetrics] = {}
        self._audit_path = os.environ.get(
            "RL_WINDOW_AUDIT_PATH", "/var/lib/zuul/rl_window_audit.jsonl")
        self._enabled = False
        self._tenant = "example-tenant"
        self._pipeline = "gate"
        self._mode = "shadow"
        self._policy_path = os.environ.get("RL_WINDOW_POLICY_PATH", "")
        self._policy = None
        self._policy_table: Optional[dict] = None
        self._policy_entries: List[dict] = []
        self._policy_kind = "heuristic"
        self._recommendations: Dict[str, dict] = {}
        # Parallel TCP-only window (for RL vs TCP comparison in audit).
        self._tcp_shadow: Dict[str, int] = {}
        self._session_baseline_set = False
        self._pending_demo_baseline = False
        self._demo_reset_path = os.environ.get(
            "RL_DEMO_RESET_PATH", "/var/lib/zuul/rl_demo_reset.request")
        self._scheduler = None
        self._executor_snapshot: Optional[ExecutorSnapshot] = None
        self._executor_snapshot_at: float = 0.0

    def configure(self, config):
        if not config.has_section("rl_window"):
            return
        self._enabled = config.getboolean("rl_window", "enabled", fallback=False)
        self._tenant = config.get(
            "rl_window", "tenant", fallback="example-tenant")
        self._pipeline = config.get(
            "rl_window", "pipeline", fallback="gate")
        self._mode = config.get(
            "rl_window", "mode", fallback="shadow").lower()
        self._policy_path = config.get(
            "rl_window", "policy_path",
            fallback=os.environ.get("RL_WINDOW_POLICY_PATH", ""))
        if self._policy_path:
            self._load_policy()

    def _load_policy(self):
        if not self._policy_path:
            return
        if self._policy_path.endswith(".json"):
            self._load_policy_table()
            return
        if np is None:
            log.warning("numpy not installed; RL policy disabled")
            return
        try:
            from stable_baselines3 import PPO
            self._policy = PPO.load(self._policy_path)
            self._policy_kind = "ppo"
            log.info("Loaded RL window policy from %s", self._policy_path)
        except Exception:
            log.exception("Failed to load RL policy from %s",
                          self._policy_path)
            self._policy = None

    def _load_policy_table(self):
        try:
            with open(self._policy_path, encoding="utf-8") as policy_file:
                data = json.load(policy_file)
            exact = {}
            for entry in data.get("entries", []):
                key = tuple(round(float(x), 2) for x in entry["state"])
                exact[key] = int(entry["action_idx"])
            self._policy_table = exact
            self._policy_entries = data.get("entries", [])
            self._policy_kind = "ppo_table"
            log.info(
                "Loaded RL policy table from %s (%s entries)",
                self._policy_path, len(self._policy_entries))
        except Exception:
            log.exception("Failed to load RL policy table from %s",
                          self._policy_path)
            self._policy_table = None
            self._policy_entries = []

    @staticmethod
    def _table_query(state: List[float]) -> List[float]:
        """Map the live state vector into the policy table's feature space.

        The offline table was exported with features
        [norm_window, queue/ceiling, failure_rate, hour_sin, hour_cos,
        queue/window]. The live state carries [norm_window,
        queue/(2*ceiling), failure_rate, success_streak,
        executor_available, queue/window], so translate the shared
        features and pin the hour slots to the grid constants (they
        carry zero weight anyway). executor_available is not in the
        table; heuristic/guards/capacity clamp apply it instead.
        """
        norm_window = float(state[0])
        queue_saturation = float(state[1])
        failure_rate = float(state[2])
        queue_pressure = float(state[5]) if len(state) > 5 else 0.0
        return [
            norm_window,
            _clip01(queue_saturation * 2.0),  # back to queue/ceiling
            failure_rate,
            0.0,   # hour_sin grid constant (weight 0)
            1.0,   # hour_cos grid constant (weight 0)
            queue_pressure,
        ]

    def _lookup_table_action(
            self, state: List[float]) -> Tuple[Optional[int], float, str]:
        """Weighted k-NN vote over the policy table.

        Returns (action_idx | None, best_distance, detail). None means the
        table has no neighbour within KNN_MAX_DISTANCE — the state is out
        of distribution and the caller should use the heuristic instead of
        trusting an arbitrary far-away table entry (the old design's
        single-nearest-neighbour over the whole table did exactly that).
        """
        query = self._table_query(state)
        key = tuple(round(float(x), 2) for x in query)
        if self._policy_table and key in self._policy_table:
            return self._policy_table[key], 0.0, "table exact match"
        neighbours: List[Tuple[float, int]] = []
        for entry in self._policy_entries:
            entry_state = entry["state"]
            dist = 0.0
            for weight, a, b in zip(TABLE_FEATURE_WEIGHTS, query,
                                    entry_state):
                if weight <= 0.0:
                    continue
                diff = float(a) - float(b)
                dist += weight * diff * diff
            neighbours.append((math.sqrt(dist), int(entry["action_idx"])))
        if not neighbours:
            return None, float("inf"), "policy table empty"
        neighbours.sort(key=lambda pair: pair[0])
        best_dist = neighbours[0][0]
        if best_dist > KNN_MAX_DISTANCE:
            return None, best_dist, (
                f"state out of table distribution "
                f"(nearest {best_dist:.2f} > {KNN_MAX_DISTANCE})")
        votes: Dict[int, float] = {}
        for dist, action_idx in neighbours[:max(1, KNN_K)]:
            votes[action_idx] = votes.get(action_idx, 0.0) + \
                1.0 / (dist + 1e-3)
        action = max(votes, key=lambda idx: votes[idx])
        return action, best_dist, (
            f"kNN k={min(KNN_K, len(neighbours))} vote "
            f"(nearest {best_dist:.2f})")

    def _queue_key(self, change_queue) -> str:
        return change_queue.getPath()

    def _metrics_for(self, key: str) -> QueueMetrics:
        if key not in self._metrics:
            self._metrics[key] = QueueMetrics()
        return self._metrics[key]

    def record_cycle_outcome(self, change_queue, succeeded: bool):
        key = self._queue_key(change_queue)
        with self._lock:
            metrics = self._metrics_for(key)
            metrics.outcomes.append((time.time(), succeeded))

    def _rolling_failure_rate(self, metrics: QueueMetrics) -> float:
        """Legacy 30-min unweighted failure rate (kept for audit compat)."""
        cutoff = time.time() - ROLLING_WINDOW_SECONDS
        recent = [ok for ts, ok in metrics.outcomes if ts >= cutoff]
        if not recent:
            return 0.0
        failures = sum(1 for ok in recent if not ok)
        return failures / len(recent)

    def _recent_failure_rate(self, metrics: QueueMetrics,
                             now: Optional[float] = None) -> float:
        """Recency-weighted failure rate for demo-scale reaction times.

        Considers only the last FAILURE_MAX_CYCLES outcomes within
        FAILURE_WINDOW_SECONDS and decays each by age with
        FAILURE_HALF_LIFE_SECONDS — a failure 3 minutes ago matters far
        less than one 10 seconds ago. Replaces the legacy 30-minute
        unweighted window that let stale outcomes dominate a 5-min demo.
        """
        now = now if now is not None else time.time()
        cutoff = now - FAILURE_WINDOW_SECONDS
        recent = [(ts, ok) for ts, ok in metrics.outcomes if ts >= cutoff]
        recent = recent[-FAILURE_MAX_CYCLES:]
        if not recent:
            return 0.0
        weight_sum = 0.0
        fail_sum = 0.0
        for ts, ok in recent:
            age = max(0.0, now - ts)
            weight = 0.5 ** (age / max(FAILURE_HALF_LIFE_SECONDS, 1e-6))
            weight_sum += weight
            if not ok:
                fail_sum += weight
        if weight_sum <= 0.0:
            return 0.0
        return _clip01(fail_sum / weight_sum)

    def _success_streak(self, metrics: QueueMetrics) -> int:
        """Consecutive successful cycles counted back from the newest."""
        streak = 0
        for _, ok in reversed(metrics.outcomes):
            if not ok:
                break
            streak += 1
        return streak

    def _layout_slot_capacity(self, scheduler) -> Optional[int]:
        """Sum static-provider slots from the managed tenant layout.

        Demo providers.yaml: one static node × labels.slots (50).
        """
        try:
            tenants = getattr(getattr(scheduler, "abide", None), "tenants", None)
            if not isinstance(tenants, dict) and not hasattr(tenants, "get"):
                return None
            tenant = tenants.get(self._tenant)
            if tenant is None:
                return None
            providers = getattr(getattr(tenant, "layout", None), "providers", None)
            if isinstance(providers, dict):
                provider_list = list(providers.values())
            elif isinstance(providers, (list, tuple)):
                provider_list = list(providers)
            else:
                return None
            total = 0
            found = False
            for provider in provider_list:
                nodes = getattr(provider, "nodes", None)
                labels = getattr(provider, "labels", None)
                if isinstance(nodes, dict):
                    node_iter = nodes.values()
                elif isinstance(nodes, (list, tuple)):
                    node_iter = nodes
                else:
                    continue
                if not isinstance(labels, dict):
                    continue
                for node in node_iter:
                    label_name = getattr(node, "label", None)
                    if label_name is None:
                        continue
                    label = labels.get(label_name)
                    if label is None:
                        continue
                    try:
                        slots = int(getattr(label, "slots", 1) or 1)
                    except (TypeError, ValueError):
                        continue
                    total += max(slots, 1)
                    found = True
            return total if found else None
        except Exception:
            return None

    @staticmethod
    def _count_launcher_slots(summaries) -> Optional[Tuple[int, int, int]]:
        """Return (ready, in_use, building) job slots, skipping slot-hosts.

        None when the listing is empty or not a real sequence (mocks).
        """
        if not isinstance(summaries, (list, tuple)):
            return None
        ready = 0
        in_use = 0
        building = 0
        saw_node = False
        for raw in summaries:
            if not isinstance(raw, dict):
                continue
            saw_node = True
            state = raw.get("state")
            if state == _SLOT_HOST:
                continue
            if state == _SLOT_READY:
                ready += 1
            elif state == _SLOT_IN_USE:
                in_use += 1
            elif state in (_SLOT_BUILDING, _SLOT_REQUESTED):
                building += 1
        if not saw_node:
            return None
        return ready, in_use, building

    @staticmethod
    def _count_legacy_nodes(nodes) -> Optional[Tuple[int, int]]:
        if isinstance(nodes, tuple) and not nodes:
            return None
        if isinstance(nodes, list):
            seq = nodes
        elif type(nodes).__name__ == "generator":
            seq = list(nodes)
        elif isinstance(nodes, tuple):
            seq = list(nodes)
        else:
            return None
        if not seq:
            return None
        ready = 0
        in_use = 0
        for node in seq:
            state = getattr(node, "state", None)
            if state == _SLOT_READY:
                ready += 1
            elif state == _SLOT_IN_USE:
                in_use += 1
        if ready == 0 and in_use == 0:
            return None
        return ready, in_use

    def _observe_executors(self, scheduler) -> ExecutorSnapshot:
        """Count READY vs in-use job slots from launcher / nodepool.

        Observation order:
          1. In-Zuul launcher ProviderNodes (static slot-hosts + READY
             subnodes) — this is the demo stack.
          2. Legacy nodepool Node cache (``/nodepool/nodes``).
          3. Executor API running/paused build requests.
          4. ``executor.running_builds`` if a test double provides it.
          5. ``RL_WINDOW_EXECUTOR_CAPACITY`` with all slots treated free.
        """
        now = time.time()
        cached = self._executor_snapshot
        if (cached is not None and
                (now - self._executor_snapshot_at) < EXECUTOR_SNAPSHOT_TTL):
            return cached

        env_cap = _env_executor_capacity()
        layout_cap = self._layout_slot_capacity(scheduler)
        snapshot = None
        try:
            snapshot = self._observe_executors_inner(
                scheduler, env_cap, layout_cap)
        except Exception:
            log.debug("Executor capacity observation failed", exc_info=True)
        if snapshot is None:
            cap = layout_cap or env_cap
            snapshot = ExecutorSnapshot(
                capacity=cap, available=cap, in_use=0,
                available_ratio=1.0, occupancy=0.0, source="env")
        self._executor_snapshot = snapshot
        self._executor_snapshot_at = now
        return snapshot

    def _observe_executors_inner(
            self, scheduler, env_cap: int,
            layout_cap: Optional[int]) -> Optional[ExecutorSnapshot]:
        def _finish(ready, in_use, source, extra_capacity=0):
            live = ready + in_use
            if live <= 0:
                return None
            capacity = max(live, extra_capacity, layout_cap or 0, env_cap)
            available = max(0, int(ready))
            occupancy = _clip01(in_use / max(capacity, 1))
            available_ratio = _clip01(available / max(capacity, 1))
            return ExecutorSnapshot(
                capacity=int(capacity),
                available=available,
                in_use=int(in_use),
                available_ratio=available_ratio,
                occupancy=occupancy,
                source=source)

        launcher = getattr(scheduler, "launcher", None)
        lister = getattr(launcher, "listProviderNodeSummaries", None)
        if callable(lister):
            try:
                summaries = lister()
            except Exception:
                summaries = None
            counted = self._count_launcher_slots(summaries)
            if counted is not None:
                ready, in_use, building = counted
                # READY/IN_USE observed: real pool. If everything is still
                # building (demo warmup), fall through so we do not clamp
                # the window to the floor before slots exist.
                finished = _finish(
                    ready, in_use, "launcher", extra_capacity=building)
                if finished is not None:
                    return finished

        nodepool = getattr(scheduler, "nodepool", None)
        get_nodes = getattr(nodepool, "getNodes", None)
        if callable(get_nodes):
            try:
                nodes = get_nodes()
            except Exception:
                nodes = None
            counted = self._count_legacy_nodes(nodes)
            if counted is not None:
                ready, in_use = counted
                finished = _finish(ready, in_use, "nodepool")
                if finished is not None:
                    return finished

        executor = getattr(scheduler, "executor", None)
        api = getattr(executor, "executor_api", None)
        in_state = getattr(api, "inState", None)
        if callable(in_state):
            try:
                try:
                    from zuul.model import BuildRequest
                    reqs = in_state(BuildRequest.RUNNING, BuildRequest.PAUSED)
                except Exception:
                    reqs = in_state()
            except Exception:
                reqs = None
            if isinstance(reqs, (list, tuple)):
                in_use = len(reqs)
                cap = max(layout_cap or 0, env_cap, in_use, 1)
                available = max(0, cap - in_use)
                return ExecutorSnapshot(
                    capacity=cap, available=available, in_use=in_use,
                    available_ratio=_clip01(available / cap),
                    occupancy=_clip01(in_use / cap),
                    source="executor_api")

        running = getattr(executor, "running_builds", None)
        if isinstance(running, dict):
            in_use = len(running)
        elif isinstance(running, (list, tuple)):
            in_use = len(running)
        else:
            in_use = None
        if in_use is not None:
            cap = max(layout_cap or 0, env_cap, in_use, 1)
            available = max(0, cap - in_use)
            return ExecutorSnapshot(
                capacity=cap, available=available, in_use=in_use,
                available_ratio=_clip01(available / cap),
                occupancy=_clip01(in_use / cap),
                source="running_builds")
        return None

    def get_rl_state(self, scheduler, change_queue) -> List[float]:
        """Return six normalised state features, each clipped to [0, 1].

        See the module-level "State design" block for the feature list.
        """
        window = change_queue.window or change_queue.window_floor
        ceiling = change_queue.window_ceiling
        if ceiling in (None, math.inf):
            ceiling = max(window, 50)
        ceiling = max(float(ceiling), 1.0)
        norm_window = _clip01(window / ceiling)

        queue_depth = len(change_queue.queue)
        queue_saturation = _clip01(queue_depth / (2.0 * ceiling))
        queue_pressure = _clip01(queue_depth / max(float(window), 1.0))

        key = self._queue_key(change_queue)
        with self._lock:
            metrics = self._metrics_for(key)
            failure_rate = self._recent_failure_rate(metrics)
            streak = self._success_streak(metrics)
        success_streak = _clip01(streak / SUCCESS_STREAK_NORM)

        snapshot = self._observe_executors(scheduler)
        available_ratio = _clip01(snapshot.available_ratio)

        return [
            norm_window,
            queue_saturation,
            failure_rate,
            success_streak,
            available_ratio,
            queue_pressure,
        ]

    def _clamp_window(self, change_queue, size: int) -> int:
        floor = int(change_queue.window_floor or 1)
        ceiling = change_queue.window_ceiling
        if ceiling in (None, math.inf):
            ceiling = 50
        ceiling = int(ceiling)
        return max(floor, min(ceiling, int(size)))

    def _clamp_to_available(self, change_queue, size: int) -> int:
        """Cap the window at free slots, never below the pipeline floor.

        ``recommended_window = min(policy_window, max(floor, available))``.
        """
        floor = int(change_queue.window_floor or 1)
        snapshot = self._executor_snapshot
        if snapshot is None or snapshot.source == "env":
            return self._clamp_window(change_queue, size)
        cap = max(floor, int(snapshot.available))
        return self._clamp_window(change_queue, min(int(size), cap))

    def _enforce_rl_tcp_floor(self, change_queue, size: int, *,
                              queue_key: Optional[str] = None,
                              audit_source: Optional[str] = None,
                              apply_capacity: Optional[bool] = None,
                              enforce_tcp_floor: bool = True) -> Tuple[int, int]:
        """Clamp a requested window to pipeline bounds (and, optionally,
        floor it at the TCP shadow window).

        ``enforce_tcp_floor=True`` (default) preserves the historical
        demo/heuristic/kNN-table behaviour of never letting the applied
        window fall below the hypothetical TCP-only window. Pure PPO
        decisions (meta["source"] == "ppo") pass ``enforce_tcp_floor=
        False`` so the network's own action is what gets applied — the
        agent must be free to choose a window *smaller* than TCP would,
        or RQ2's comparison is no longer measuring the learned policy.

        Capacity clamp can still win when free slots are below the TCP
        shadow: applied ≤ max(floor, available) but never below floor.
        """
        key = queue_key or self._queue_key(change_queue)
        tcp_shadow = self._ensure_tcp_shadow(change_queue)
        requested = int(size)
        requested_clamped = self._clamp_window(change_queue, requested)
        if enforce_tcp_floor:
            enforced = self._clamp_window(
                change_queue, max(requested, tcp_shadow))
        else:
            enforced = requested_clamped
        if apply_capacity is None:
            apply_capacity = (audit_source == "agent")
        if apply_capacity:
            capped = self._clamp_to_available(change_queue, enforced)
            if capped != enforced:
                self._audit(key, {
                    "event": "executor_capacity_clamp",
                    "requested_window": requested,
                    "tcp_shadow_window": tcp_shadow,
                    "before_capacity_clamp": enforced,
                    "enforced_window": capped,
                    "available_slots": (
                        self._executor_snapshot.available
                        if self._executor_snapshot else None),
                    "source": audit_source,
                })
                enforced = capped
        if enforced > requested_clamped and audit_source:
            self._audit(key, {
                "event": "rl_floor_applied",
                "requested_window": requested,
                "tcp_shadow_window": tcp_shadow,
                "enforced_window": enforced,
                "source": audit_source,
            })
        return enforced, tcp_shadow

    def set_window_from_api(self, change_queue, size: int,
                            source: str = "api",
                            persist_seconds: Optional[int] = None,
                            context=None,
                            enforce_tcp_floor: bool = True):
        """Apply a bounded persistent window override.

        ``enforce_tcp_floor=False`` is used for pure-PPO agent decisions
        (see _enforce_rl_tcp_floor) so the network's chosen window is
        applied as-is, not floored at the TCP shadow window.
        """
        key = self._queue_key(change_queue)
        clamped, _ = self._enforce_rl_tcp_floor(
            change_queue, size, queue_key=key, audit_source=source,
            enforce_tcp_floor=enforce_tcp_floor)
        expires_at = None
        if persist_seconds is not None:
            expires_at = time.time() + persist_seconds
        override = QueueOverride(
            size=clamped, source=source,
            set_at=time.time(), expires_at=expires_at)
        with self._lock:
            self._overrides[key] = override
        # Skip the ZK write entirely when the window isn't actually
        # changing (e.g. every "hold" decision). Every entry into
        # activeContext is a real write to this ChangeQueue object,
        # contending with the core scheduler's own writes to the same
        # znode on every real gate event; writing the same value back
        # on every ~10s tick regardless of whether anything changed was
        # pure unforced contention. Observed live: a scheduler-side
        # "Exception saving ZKObject <ChangeQueue gate: ...>"
        # (BadVersionError) followed by the gate pipeline silently
        # stopping all further processing — jobs kept finishing but were
        # never reported or merged. Cutting out no-op writes reduces how
        # often the agent's tick and the scheduler's own save can land
        # on the same object at the same moment.
        if int(change_queue.window or 0) != clamped:
            ctx = context or change_queue.zk_context
            if ctx is None:
                raise RuntimeError(
                    "No ZK context available for window override")
            with change_queue.activeContext(ctx):
                change_queue.window = clamped
            log.info("RL window override for %s set to %s (%s)",
                     key, clamped, source)
        self._audit(key, {
            "event": "set_window",
            "size": clamped,
            "source": source,
        })
        return clamped

    def clear_override(self, change_queue):
        key = self._queue_key(change_queue)
        with self._lock:
            self._overrides.pop(key, None)

    def _active_override(self, key: str) -> Optional[QueueOverride]:
        override = self._overrides.get(key)
        if override is None:
            return None
        if override.expires_at and time.time() > override.expires_at:
            self._overrides.pop(key, None)
            return None
        return override

    def _ensure_tcp_shadow(self, change_queue) -> int:
        key = self._queue_key(change_queue)
        if key not in self._tcp_shadow:
            self._tcp_shadow[key] = DEFAULT_INITIAL_WINDOW
        return self._tcp_shadow[key]

    def _advance_tcp_shadow(self, change_queue, succeeded: bool) -> int:
        """Update hypothetical TCP-only window (independent of RL override)."""
        key = self._queue_key(change_queue)
        window = self._ensure_tcp_shadow(change_queue)
        floor = int(change_queue.window_floor or 1)
        ceiling = change_queue.window_ceiling
        if ceiling in (None, math.inf):
            ceiling = 50
        ceiling = int(ceiling)
        if succeeded:
            if change_queue.window_increase_type == 'linear':
                window = min(
                    ceiling,
                    window + int(change_queue.window_increase_factor or 1))
            elif change_queue.window_increase_type == 'exponential':
                window = min(
                    ceiling,
                    window * int(change_queue.window_increase_factor or 1))
        else:
            if change_queue.window_decrease_type == 'linear':
                window = max(
                    floor,
                    window - int(change_queue.window_decrease_factor or 1))
            elif change_queue.window_decrease_type == 'exponential':
                factor = int(change_queue.window_decrease_factor or 2)
                window = max(floor, int(window / factor))
        self._tcp_shadow[key] = window
        return window

    def _demo_reset_pending(self) -> bool:
        """True when a demo reset trigger file is waiting for the watcher."""
        path = self._demo_reset_path
        return bool(path) and os.path.isfile(path)

    def _check_demo_reset_request(self, scheduler) -> bool:
        """Process a demo reset trigger file written by rl-control."""
        path = self._demo_reset_path
        if not path or not os.path.isfile(path):
            return False
        payload = {}
        try:
            with open(path, encoding="utf-8") as handle:
                raw = handle.read().strip()
            if raw:
                payload = json.loads(raw)
        except (OSError, json.JSONDecodeError):
            log.debug("Unable to read demo reset trigger %s", path,
                      exc_info=True)
        try:
            os.remove(path)
        except OSError:
            log.debug("Unable to remove demo reset trigger %s", path,
                      exc_info=True)
        if payload.get("purge_only"):
            purged = self._purge_all_pipeline_queues(scheduler)
            log.info("RL demo queue purge-only removed %s item(s)", purged)
            return True
        self.begin_demo_session(scheduler)
        return True

    def _purge_all_pipeline_queues(self, scheduler) -> int:
        """Remove every queued item from every pipeline (demo fresh start)."""
        tenant = scheduler.abide.tenants.get(self._tenant)
        if tenant is None:
            log.warning(
                "Demo purge skipped: tenant %s not loaded", self._tenant)
            return 0
        removed = 0
        queued_before = 0
        with scheduler.createZKContext(None, log) as ctx:
            for pipeline_name, pipeline_manager in (
                    tenant.layout.pipeline_managers.items()):
                with pipeline_manager.currentContext(ctx):
                    for change_queue in list(pipeline_manager.state.queues):
                        queued_before += len(change_queue.queue)
                        # Dequeue tail-first so dependent chains collapse safely.
                        for item in reversed(list(change_queue.queue)):
                            try:
                                pipeline_manager.removeItem(item)
                                removed += 1
                            except Exception:
                                log.debug(
                                    "Demo reset: dequeue %s item skipped",
                                    item, exc_info=True)
                    try:
                        pipeline_manager.summary.update(
                            ctx, scheduler.globals)
                    except Exception:
                        log.debug(
                            "Demo purge: unable to refresh %s summary",
                            pipeline_name, exc_info=True)
        if queued_before and removed == 0:
            log.warning(
                "Demo purge found %s queued item(s) but removed 0 "
                "(tenant=%s)",
                queued_before, self._tenant)
        if removed:
            self._audit("_scheduler", {
                "event": "demo_queue_purged",
                "removed": removed,
                "pipelines": "all",
            })
        else:
            self._audit("_scheduler", {
                "event": "demo_queue_purged",
                "removed": 0,
                "pipelines": "all",
                "queued_before": queued_before,
            })
        return removed

    def _purge_gate_queues(self, scheduler) -> int:
        """Backward-compatible alias: purge all pipeline queues."""
        return self._purge_all_pipeline_queues(scheduler)

    def begin_demo_session(self, scheduler):
        """Reset RL/TCP shadow windows to the demo baseline for a new session."""
        with self._lock:
            self._tcp_shadow.clear()
            self._overrides.clear()
            self._recommendations.clear()
            self._metrics.clear()
            self._session_baseline_set = False
            self._pending_demo_baseline = False
            self._executor_snapshot = None
            self._executor_snapshot_at = 0.0
        self._truncate_audit_log()
        purged = self._purge_all_pipeline_queues(scheduler)
        baseline_ok = self._reset_session_baseline(scheduler)
        if baseline_ok:
            self._session_baseline_set = True
        else:
            self._pending_demo_baseline = True
        self._audit("_scheduler", {
            "event": "demo_session_reset",
            "purged": purged,
            "baseline_ok": baseline_ok,
            "baseline_window": DEFAULT_INITIAL_WINDOW,
        })
        log.info(
            "RL demo session reset (baseline window=%s, purged=%s, "
            "baseline_ok=%s)",
            DEFAULT_INITIAL_WINDOW, purged, baseline_ok)

    def _retry_pending_demo_baseline(self, scheduler) -> bool:
        """Retry baseline window reset when queues were not ready on first pass."""
        if not self._pending_demo_baseline:
            return False
        if self._reset_session_baseline(scheduler):
            self._pending_demo_baseline = False
            self._session_baseline_set = True
            self._audit("_scheduler", {
                "event": "demo_session_reset",
                "purged": 0,
                "baseline_ok": True,
                "baseline_window": DEFAULT_INITIAL_WINDOW,
                "retry": True,
            })
            log.info(
                "RL demo baseline retry succeeded (window=%s)",
                DEFAULT_INITIAL_WINDOW)
            return True
        return False

    def _demo_reset_watch(self, scheduler):
        """Fast poll for demo reset requests and deferred baseline retries."""
        if self._check_demo_reset_request(scheduler):
            return
        self._retry_pending_demo_baseline(scheduler)

    def _tick_queue_agent(self, scheduler, change_queue):
        """Run one RL decision for a queue and write agent_tick audit."""
        key = self._queue_key(change_queue)
        metrics = self._metrics_for(key)
        metrics.decision_count += 1
        apply_override = self._mode == "active"
        try:
            self._ensure_tcp_shadow(change_queue)
            state = self.get_rl_state(scheduler, change_queue)
            snapshot = self._executor_snapshot
            actual_window = int(
                change_queue.window or change_queue.window_floor)
            action_idx, decision_reason, decision_meta = \
                self._choose_action(
                    state, snapshot=snapshot,
                    current_window=actual_window)
            recommended = self._apply_action(
                change_queue, action_idx, apply=apply_override,
                decision_source=decision_meta.get("source"))
            if self._mode == "shadow":
                self.clear_override(change_queue)
            if apply_override:
                actual_window = int(
                    change_queue.window or change_queue.window_floor)
            tcp_shadow = self._tcp_shadow.get(key, actual_window)
            if (snapshot is not None and snapshot.source != "env" and
                    "executor" not in decision_reason.lower() and
                    snapshot.available < actual_window):
                decision_reason += (
                    f" — only {snapshot.available} executors free")
            if actual_window != tcp_shadow:
                # Always show the real comparison, ahead or behind — this
                # is the factual "RL vs TCP" figure, not a guess about
                # what TCP hypothetically "would have" done.
                gap = actual_window - tcp_shadow
                sign = "+" if gap > 0 else ""
                decision_reason += (
                    f" · RL {actual_window} vs TCP {tcp_shadow}"
                    f" ({sign}{gap})")
            self._record_recommendation(
                change_queue, recommended, action_idx,
                actual_window=actual_window,
                tcp_shadow_window=tcp_shadow,
                decision_reason=decision_reason,
                decision_source=decision_meta.get("source"),
                decision_detail=decision_meta.get("policy_detail"))
            self._audit(key, {
                "event": "agent_tick",
                "state": state,
                "state_labels": list(STATE_LABELS),
                "action_idx": action_idx,
                "action_delta": ACTION_DELTAS[action_idx],
                "actual_window": actual_window,
                "recommended_window": recommended,
                "tcp_shadow_window": tcp_shadow,
                "rl_vs_tcp_delta": recommended - tcp_shadow,
                "decision_reason": decision_reason,
                "decision_source": decision_meta.get("source"),
                "decision_detail": decision_meta.get("policy_detail"),
                "decision_confidence": decision_meta.get("confidence"),
                "knn_distance": decision_meta.get("knn_distance"),
                "guardrail": decision_meta.get("guardrail"),
                **_executor_audit_fields(snapshot),
                "mode": self._mode,
                "policy": self._policy_kind,
                "cycle_triggered": True,
            })
        except Exception:
            metrics.fallback_count += 1
            log.exception(
                "RL window agent tick failed for %s; TCP fallback kept", key)
            self._audit(key, {"event": "fallback"})

    def adjust_window_after_cycle(self, change_queue, succeeded: bool):
        """Adjust window after a gate cycle, honouring RL override when active.

        Runs inside pipeline processing, so it must never perform the demo
        reset (queue purge + baseline write across all pipelines) inline —
        that work re-enters ZK pipeline contexts and can deadlock the
        scheduler. When a reset request is pending we simply skip this
        stale cycle; the 0.5s apscheduler watcher thread consumes the
        trigger safely outside pipeline processing.
        """
        if self._demo_reset_pending():
            return
        self.record_cycle_outcome(change_queue, succeeded)
        key = self._queue_key(change_queue)
        tcp_before = self._ensure_tcp_shadow(change_queue)
        tcp_after = self._advance_tcp_shadow(change_queue, succeeded)
        self._audit(key, {
            "event": "tcp_shadow",
            "succeeded": succeeded,
            "window_before": tcp_before,
            "window_after": tcp_after,
        })
        self._sync_tcp_shadow_recommendation(change_queue, tcp_after)
        scheduler = self._scheduler
        if scheduler is not None and self._enabled:
            self._tick_queue_agent(scheduler, change_queue)
            return
        override = self._active_override(key)
        if override and self._mode in ("active", "shadow"):
            if self._mode == "active":
                enforced, _ = self._enforce_rl_tcp_floor(
                    change_queue, override.size, queue_key=key,
                    audit_source="cycle")
                if enforced != override.size:
                    override = QueueOverride(
                        size=enforced, source=override.source,
                        set_at=override.set_at,
                        expires_at=override.expires_at)
                    with self._lock:
                        self._overrides[key] = override
                with change_queue.activeContext(change_queue.zk_context):
                    change_queue.window = enforced
                log.debug("%s window held at RL override %s",
                          change_queue, enforced)
                return
        self._apply_tcp_adjustment(change_queue, succeeded)

    def _apply_tcp_adjustment(self, change_queue, succeeded: bool):
        before = change_queue.window
        if succeeded:
            change_queue.increaseWindowSize()
            log.debug("%s window size increased to %s",
                      change_queue, change_queue.window)
        else:
            change_queue.decreaseWindowSize()
            log.debug("%s window size decreased to %s",
                      change_queue, change_queue.window)
        self._audit(self._queue_key(change_queue), {
            "event": "tcp_adjust",
            "succeeded": succeeded,
            "window_before": before,
            "window_after": change_queue.window,
        })

    @staticmethod
    def _format_decision_reason(
            action_idx: int,
            state: List[float],
            *,
            kind: str = "policy",
            snapshot: Optional[ExecutorSnapshot] = None) -> str:
        """Plain-English primary UI reason for a window action.

        Keep this short and non-technical. Lookup detail (kNN distance,
        etc.) stays in meta["policy_detail"] for tooltips/debug only.
        """
        delta = ACTION_DELTAS[action_idx]
        failure_rate = state[2] if len(state) > 2 else 0.0
        queue_saturation = state[1] if len(state) > 1 else 0.0
        success_streak = state[3] if len(state) > 3 else 0.0
        fr_pct = int(round(failure_rate * 100))
        streak_n = int(success_streak * SUCCESS_STREAK_NORM)
        quiet = failure_rate < 0.1 and queue_saturation < 0.4
        step = abs(delta)
        free_n = int(snapshot.available) if snapshot is not None else None
        if free_n is None and len(state) > 4:
            # Approximate from the state ratio when no live snapshot.
            free_n = int(round(float(state[4]) * _env_executor_capacity()))

        if kind == "low_executors":
            n = 0 if free_n is None else free_n
            if delta < 0:
                return (
                    f"Reduced the window by {step} — only {n} "
                    f"executors free.")
            return f"Held the window — only {n} executors free."
        if kind == "executors_free":
            n = 0 if free_n is None else free_n
            return (
                f"Increased the window by {step} — {n} executors free "
                f"and the queue is deep.")
        if kind == "hold_burst":
            extra = ""
            if free_n is not None and free_n <= 4:
                extra = f" Only {free_n} executors free."
            return (
                f"Held the window — holding through failures "
                f"({fr_pct}% rate). TCP would have cut capacity; "
                f"RL keeps it steady.{extra}")
        if kind == "ramp_streak":
            return (
                f"Increased the window by {step} after {streak_n} "
                f"consecutive successes (faster than TCP's +1).")
        if delta < 0:
            # step is 1 or 2 (ACTION_DELTAS max magnitude); label the
            # size honestly rather than always calling it "gentle" — a
            # full -2 step at a high failure rate is the agent's
            # strongest shrink action, not a small trim. The real
            # comparison against TCP's actual current window (not a
            # guess about what TCP "would" do) is appended separately in
            # _tick_queue_agent, since TCP's rule reacts to whether the
            # single last cycle failed, which this rolling failure_rate
            # feature does not directly tell us — claiming "TCP would
            # have halved" here was not reliably true.
            size_word = "small trim" if step == 1 else "sharp cut"
            if quiet:
                return (
                    f"Reduced the window by {step} ({size_word}), even "
                    f"though failures are currently low.")
            return (
                f"Reduced the window by {step} ({size_word}) — "
                f"failure rate {fr_pct}%.")
        if delta > 0:
            if quiet or failure_rate < 0.1:
                return (
                    f"Increased the window by {step}. "
                    f"Queue is quiet and failures are low.")
            if failure_rate < 0.4:
                return (
                    f"Increased the window by {step}. "
                    f"Failure rate {fr_pct}% is manageable so far.")
            # Above 40% failure, growing is a real bet the policy is
            # making, not a safe default — say so plainly instead of
            # reassuring the viewer the same way regardless of severity.
            return (
                f"Increased the window by {step} despite a {fr_pct}% "
                f"failure rate — the policy is betting this still beats "
                f"TCP's slower reaction; watch this one.")
        if quiet:
            return (
                f"Held the window — queue quiet, failures low "
                f"({fr_pct}%), steady state.")
        return (
            f"Held the window — failure rate {fr_pct}%, "
            f"queue {queue_saturation:.0%} saturated.")

    @staticmethod
    def _executor_scarce(state: List[float],
                         snapshot: Optional[ExecutorSnapshot],
                         current_window: Optional[int]) -> bool:
        ratio = float(state[4]) if len(state) > 4 else 1.0
        if ratio <= EXECUTOR_HOLD_AVAILABLE:
            return True
        if snapshot is not None:
            if snapshot.available <= 0:
                return True
            if (current_window is not None and
                    snapshot.available < int(current_window)):
                return True
        return False

    @staticmethod
    def _executor_plentiful(state: List[float],
                            snapshot: Optional[ExecutorSnapshot],
                            current_window: Optional[int]) -> bool:
        ratio = float(state[4]) if len(state) > 4 else 0.0
        if ratio < EXECUTOR_GROW_AVAILABLE:
            return False
        if snapshot is not None and current_window is not None:
            if snapshot.available <= int(current_window):
                return False
        return True

    def _heuristic_action(self, state: List[float], *,
                          snapshot: Optional[ExecutorSnapshot] = None,
                          current_window: Optional[int] = None
                          ) -> Tuple[int, str]:
        """Rule-based fallback when no policy applies (low kNN confidence,
        no table loaded, or PPO unavailable)."""
        failure_rate = state[2] if len(state) > 2 else 0.0
        queue_saturation = state[1] if len(state) > 1 else 0.0
        success_streak = state[3] if len(state) > 3 else 0.0
        queue_pressure = state[5] if len(state) > 5 else 0.0
        available_ratio = state[4] if len(state) > 4 else 1.0
        scarce = self._executor_scarce(state, snapshot, current_window)
        plentiful = self._executor_plentiful(
            state, snapshot, current_window)

        if scarce:
            # Extreme occupancy and a non-tiny window → trim; otherwise hold.
            if available_ratio < 0.08 and (state[0] if state else 0.0) > 0.2:
                return 1, self._format_decision_reason(
                    1, state, kind="low_executors", snapshot=snapshot)
            return 2, self._format_decision_reason(
                2, state, kind="low_executors", snapshot=snapshot)
        if failure_rate > DEMO_HOLD_FAILURE_RATE:
            return 2, self._format_decision_reason(
                2, state, kind="hold_burst", snapshot=snapshot)
        if success_streak >= 0.3:
            return 4, self._format_decision_reason(
                4, state, kind="ramp_streak", snapshot=snapshot)
        queue_deep = queue_pressure >= 0.5 or queue_saturation >= 0.3
        if plentiful and failure_rate < 0.1 and queue_deep:
            action = 4 if (available_ratio >= 0.6 and
                           queue_pressure >= 0.8) else 3
            return action, self._format_decision_reason(
                action, state, kind="executors_free", snapshot=snapshot)
        if failure_rate < 0.1 and queue_saturation < 0.4:
            return 3, self._format_decision_reason(
                3, state, snapshot=snapshot)
        return 2, self._format_decision_reason(
            2, state, snapshot=snapshot)

    def _choose_action(self, state: List[float], *,
                       snapshot: Optional[ExecutorSnapshot] = None,
                       current_window: Optional[int] = None
                       ) -> Tuple[int, str, dict]:
        """Pick a window action, explain it, and report the source.

        Decision pipeline:
          1. Pure PPO: when a trained Stable-Baselines3 PPO network is
             loaded (``self._policy``), its ``predict()`` output IS the
             action — no guardrails, no kNN table, no TCP-shadow floor.
             This is required for RQ2 (research proposal Section 3.2.3 /
             thesis Section 3.2.3): the statistical comparison against
             the TCP rule and the other baselines must evaluate the
             learned policy on its own merits, not a policy blended with
             hand-tuned demo rules. Only the physical bounds clamp
             (window_floor/window_ceiling, applied later in
             set_window_from_api) and, optionally, the executor-capacity
             clamp — a hard resource constraint, not a heuristic — still
             apply after this method returns.
          2. Policy table (used only when no PPO network is loaded, e.g.
             the lightweight offline-search table for a dependency-free
             demo): table exact match → weighted kNN vote (k=KNN_K over
             the exported grid, per-feature weights, hour slots weight
             0). kNN falls back to the heuristic when the nearest
             neighbour is farther than KNN_MAX_DISTANCE (state out of
             the table's training distribution). This coarse
             approximation is still guarded (see _guarded below), since
             it is not the trained network.
          3. Heuristic fallback (no policy loaded at all), also guarded.

        Guardrails (table/heuristic paths only):
             - hold instead of growing when few executors are free;
             - hold instead of shrinking while the recency-weighted
               failure rate exceeds DEMO_HOLD_FAILURE_RATE (a failure
               burst must not halve throughput — TCP's mistake);
             - shrinks bounded to −2 per tick by ACTION_DELTAS;
             - upgrade a hold to +2 after a sustained success streak
               only when executors are plentiful;
             - later the applied window is floored at the TCP shadow,
               clamped to the pipeline floor/ceiling, then capped at
               available slots.

        Returns (action_idx, human-readable reason, meta) where meta has
        "source" (ppo | table-exact | knn | heuristic), "confidence"
        (0..1, from kNN distance when applicable), and optional
        "policy_detail" for tooltips/debug (kNN distance, table match, …).
        """
        if snapshot is None:
            snapshot = self._executor_snapshot

        if self._policy is not None and np is not None:
            obs = np.array(state, dtype=np.float32)
            action, _ = self._policy.predict(obs, deterministic=True)
            action_idx = int(action)
            return action_idx, self._format_decision_reason(
                action_idx, state, snapshot=snapshot), {
                "source": "ppo",
                "confidence": None,
                "knn_distance": None,
                "policy_detail": "Decided by the trained PPO model "
                                  "itself — no override rules or manual "
                                  "safety floor involved.",
            }

        failure_rate = state[2] if len(state) > 2 else 0.0
        success_streak = state[3] if len(state) > 3 else 0.0
        scarce = self._executor_scarce(state, snapshot, current_window)
        plentiful = self._executor_plentiful(
            state, snapshot, current_window)

        def _guarded(action: int, meta: dict) -> Tuple[int, str, dict]:
            delta = ACTION_DELTAS[action]
            if delta > 0 and scarce:
                meta = dict(meta, guardrail="hold_on_low_executors")
                return 2, self._format_decision_reason(
                    2, state, kind="low_executors",
                    snapshot=snapshot), meta
            if failure_rate > DEMO_HOLD_FAILURE_RATE and delta < 0:
                meta = dict(meta, guardrail="hold_on_failure_burst")
                return 2, self._format_decision_reason(
                    2, state, kind="hold_burst",
                    snapshot=snapshot), meta
            if (delta == 0 and failure_rate < 0.05 and
                    success_streak >= 0.5):
                if scarce:
                    meta = dict(meta, guardrail="hold_on_low_executors")
                    return 2, self._format_decision_reason(
                        2, state, kind="low_executors",
                        snapshot=snapshot), meta
                if plentiful:
                    meta = dict(meta, guardrail="ramp_on_success_streak")
                    return 4, self._format_decision_reason(
                        4, state, kind="ramp_streak",
                        snapshot=snapshot), meta
            return action, self._format_decision_reason(
                action, state, snapshot=snapshot), meta

        if self._policy_table is not None or self._policy_entries:
            action, distance, detail = self._lookup_table_action(state)
            if action is not None:
                confidence = _clip01(1.0 - distance / max(
                    KNN_MAX_DISTANCE, 1e-6))
                source = ("table-exact" if detail == "table exact match"
                          else "knn")
                return _guarded(action, {
                    "source": source,
                    "confidence": round(confidence, 3),
                    "knn_distance": round(distance, 4),
                    "policy_detail": detail,
                })
            # Low confidence: fall through to heuristic; keep lookup detail
            # in meta for debug, not in the primary reason string.
            h_action, h_reason = self._heuristic_action(
                state, snapshot=snapshot, current_window=current_window)
            return h_action, h_reason, {
                "source": "heuristic",
                "confidence": 0.0,
                "knn_distance": (round(distance, 4)
                                 if math.isfinite(distance) else None),
                "policy_detail": detail,
            }
        h_action, h_reason = self._heuristic_action(
            state, snapshot=snapshot, current_window=current_window)
        return h_action, h_reason, {
            "source": "heuristic",
            "confidence": None,
            "knn_distance": None,
            "policy_detail": None,
        }

    def _recommend_window(self, change_queue, action_idx: int) -> int:
        delta = ACTION_DELTAS[action_idx]
        return (change_queue.window or change_queue.window_floor) + delta

    def _apply_action(self, change_queue, action_idx: int, apply: bool = True,
                      decision_source: Optional[str] = None):
        new_size = self._recommend_window(change_queue, action_idx)
        # Pure PPO decisions are applied as-is (see _choose_action /
        # _enforce_rl_tcp_floor) — only table/heuristic decisions get the
        # "never below TCP shadow" demo floor.
        enforce_tcp_floor = decision_source != "ppo"
        if apply:
            return self.set_window_from_api(
                change_queue, new_size, source="agent",
                enforce_tcp_floor=enforce_tcp_floor)
        enforced, _ = self._enforce_rl_tcp_floor(
            change_queue, new_size, audit_source="agent",
            enforce_tcp_floor=enforce_tcp_floor)
        return enforced

    def _sync_tcp_shadow_recommendation(self, change_queue, tcp_window: int):
        """Keep status API tcp_shadow_window fresh between agent ticks."""
        key = self._queue_key(change_queue)
        with self._lock:
            rec = self._recommendations.get(key)
            if rec is None:
                return
            rec["tcp_shadow_window"] = int(tcp_window)
            rec["updated_at"] = time.time()

    def _record_recommendation(self, change_queue, recommended: int,
                               action_idx: int, *,
                               actual_window: Optional[int] = None,
                               tcp_shadow_window: Optional[int] = None,
                               decision_reason: Optional[str] = None,
                               decision_source: Optional[str] = None,
                               decision_detail: Optional[str] = None):
        key = self._queue_key(change_queue)
        if actual_window is None:
            actual_window = int(
                change_queue.window or change_queue.window_floor)
        if tcp_shadow_window is None:
            tcp_shadow_window = self._tcp_shadow.get(key, actual_window)
        snap = self._executor_snapshot
        exec_fields = _executor_audit_fields(snap)
        with self._lock:
            self._recommendations[key] = {
                "queue_uuid": change_queue.uuid,
                "queue_name": change_queue.name or change_queue.uuid[:8],
                "recommended_window": recommended,
                "current_window": actual_window,
                "tcp_shadow_window": int(tcp_shadow_window),
                "action_delta": ACTION_DELTAS[action_idx],
                "decision_reason": decision_reason or "",
                "decision_source": decision_source or "",
                "decision_detail": decision_detail or "",
                "mode": self._mode,
                "updated_at": time.time(),
                "available_slots": exec_fields["available_slots"],
                "executor_capacity": exec_fields["executor_capacity"],
                "executor_in_use": exec_fields["executor_in_use"],
                "executor_available": exec_fields["executor_available"],
            }

    def _is_managed_pipeline(self, tenant_name: str,
                             pipeline_name: str) -> bool:
        return (self._enabled and tenant_name == self._tenant and
                pipeline_name == self._pipeline)

    def get_queue_status_json(self, tenant_name: str, pipeline_name: str,
                              change_queue) -> dict:
        if not self._is_managed_pipeline(tenant_name, pipeline_name):
            return {}
        key = self._queue_key(change_queue)
        with self._lock:
            rec = self._recommendations.get(key)
            tcp_shadow = self._tcp_shadow.get(key)
        if rec is None:
            return {}
        if tcp_shadow is not None:
            tcp_shadow_window = int(tcp_shadow)
        else:
            tcp_shadow_window = rec.get("tcp_shadow_window")
        return {
            "rl_recommended_window": rec["recommended_window"],
            "rl_current_window": rec["current_window"],
            "rl_tcp_shadow_window": tcp_shadow_window,
            "rl_action_delta": rec["action_delta"],
            "rl_decision_reason": rec.get("decision_reason", ""),
            "rl_decision_source": rec.get("decision_source", ""),
            "rl_decision_detail": rec.get("decision_detail", ""),
            "rl_mode": rec["mode"],
            "rl_updated_at": int(rec["updated_at"] * 1000),
        }

    def get_pipeline_status_json(self, tenant_name: str,
                                 pipeline_name: str) -> Optional[dict]:
        if not self._is_managed_pipeline(tenant_name, pipeline_name):
            return None
        with self._lock:
            queues = list(self._recommendations.values())
        return {
            "enabled": True,
            "mode": self._mode,
            "tick_mode": "cycle_driven",
            "policy": self._policy_kind,
            "queues": [
                {
                    "name": q["queue_name"],
                    "uuid": q["queue_uuid"],
                    "recommended_window": q["recommended_window"],
                    "current_window": q["current_window"],
                    "tcp_shadow_window": q.get("tcp_shadow_window"),
                    "action_delta": q["action_delta"],
                    "decision_reason": q.get("decision_reason", ""),
                    "decision_source": q.get("decision_source", ""),
                    "decision_detail": q.get("decision_detail", ""),
                    "available_slots": q.get("available_slots"),
                    "executor_capacity": q.get("executor_capacity"),
                    "executor_in_use": q.get("executor_in_use"),
                    "executor_available": q.get("executor_available"),
                    "updated_at": int(q["updated_at"] * 1000),
                }
                for q in queues
            ],
        }

    def _audit(self, queue_key: str, payload: dict):
        record = {
            "timestamp": time.time(),
            "queue": queue_key,
            **payload,
        }
        try:
            os.makedirs(os.path.dirname(self._audit_path), exist_ok=True)
            with open(self._audit_path, "a", encoding="utf-8") as audit_file:
                audit_file.write(json.dumps(record) + "\n")
        except Exception:
            log.debug("Unable to write RL audit record", exc_info=True)

    def _truncate_audit_log(self):
        """Start a fresh audit log for a new demo session.

        The live file is reset (not appended to) so the demo dashboard only
        ever shows the current session's trace. Previous sessions are not
        discarded: the outgoing file is archived alongside it first, so
        multi-session analysis (e.g. comparing several chained demo runs)
        remains possible from disk.
        """
        try:
            os.makedirs(os.path.dirname(self._audit_path), exist_ok=True)
            if os.path.isfile(self._audit_path) and os.path.getsize(self._audit_path) > 0:
                archive_path = "%s.session-%d" % (
                    self._audit_path, int(time.time()))
                try:
                    os.replace(self._audit_path, archive_path)
                except OSError:
                    log.debug(
                        "Unable to archive previous RL audit log at %s",
                        self._audit_path, exc_info=True)
            with open(self._audit_path, "w", encoding="utf-8"):
                pass
            self._audit("_scheduler", {
                "event": "agent_started",
                "tenant": self._tenant,
                "pipeline": self._pipeline,
                "mode": self._mode,
                "baseline_window": DEFAULT_INITIAL_WINDOW,
            })
        except Exception:
            log.warning(
                "Unable to truncate RL audit log at %s",
                self._audit_path, exc_info=True)

    def _init_audit_log(self):
        """Create the shared audit file for volume-mounted readers."""
        try:
            os.makedirs(os.path.dirname(self._audit_path), exist_ok=True)
            if not os.path.isfile(self._audit_path):
                self._truncate_audit_log()
            else:
                self._audit("_scheduler", {
                    "event": "agent_started",
                    "tenant": self._tenant,
                    "pipeline": self._pipeline,
                    "mode": self._mode,
                    "baseline_window": DEFAULT_INITIAL_WINDOW,
                })
        except Exception:
            log.warning(
                "Unable to initialize RL audit log at %s",
                self._audit_path, exc_info=True)

    def _reset_session_baseline(self, scheduler) -> bool:
        """Reset RL and TCP shadow windows to the demo default before diverging."""
        tenant = scheduler.abide.tenants.get(self._tenant)
        if tenant is None:
            return False
        pipeline_manager = tenant.layout.pipeline_managers.get(
            self._pipeline)
        if pipeline_manager is None:
            return False
        queues = list(pipeline_manager.state.queues)
        if not queues:
            return False
        with scheduler.createZKContext(None, log) as ctx:
            with pipeline_manager.currentContext(ctx):
                for change_queue in queues:
                    key = self._queue_key(change_queue)
                    baseline = self._clamp_window(
                        change_queue, DEFAULT_INITIAL_WINDOW)
                    self._overrides.pop(key, None)
                    self._tcp_shadow[key] = baseline
                    with change_queue.activeContext(ctx):
                        change_queue.window = baseline
                    self._audit(key, {
                        "event": "agent_tick",
                        "state": self.get_rl_state(scheduler, change_queue),
                        "action_idx": 2,
                        "action_delta": 0,
                        "actual_window": baseline,
                        "recommended_window": baseline,
                        "tcp_shadow_window": baseline,
                        "rl_vs_tcp_delta": 0,
                        "decision_reason": (
                            f"demo session reset — both windows at "
                            f"baseline {baseline}"),
                        "mode": self._mode,
                        "policy": self._policy_kind,
                        "baseline": True,
                        **_executor_audit_fields(self._executor_snapshot),
                    })
                    self._record_recommendation(
                        change_queue, baseline, 2,
                        actual_window=baseline,
                        tcp_shadow_window=baseline,
                        decision_reason=(
                            f"demo session reset — both windows at "
                            f"baseline {baseline}"))
        return True

    def start_agent(self, scheduler):
        self._scheduler = scheduler
        self.configure(scheduler.config)
        if not self._enabled:
            log.info("RL window agent disabled")
            return
        from datetime import datetime

        from apscheduler.triggers.interval import IntervalTrigger
        self._tcp_shadow.clear()
        self._recommendations.clear()
        self._session_baseline_set = False
        self._init_audit_log()
        scheduler.apsched.add_job(
            lambda: self._demo_reset_watch(scheduler),
            trigger=IntervalTrigger(seconds=0.5),
            id="rl_demo_reset_watch",
            replace_existing=True,
            max_instances=1,
            next_run_time=datetime.now(),
        )
        # No independent periodic decision tick: RL and TCP now both only
        # re-evaluate on a genuine gate-cycle completion (see
        # adjust_window_after_cycle), so the two are compared on the same
        # event cadence instead of RL updating far more often than TCP.
        log.info(
            "RL window agent started (tenant=%s pipeline=%s mode=%s)",
            self._tenant, self._pipeline, self._mode)


CONTROLLER = WindowController()


def configure(config):
    CONTROLLER.configure(config)


def get_rl_state(scheduler, change_queue):
    return CONTROLLER.get_rl_state(scheduler, change_queue)


def set_window_from_api(change_queue, size, source="api", persist_seconds=None):
    return CONTROLLER.set_window_from_api(
        change_queue, size, source=source,
        persist_seconds=persist_seconds)


def adjust_window_after_cycle(change_queue, succeeded):
    CONTROLLER.adjust_window_after_cycle(change_queue, succeeded)


def begin_demo_session(scheduler):
    CONTROLLER.begin_demo_session(scheduler)


def start_rl_window_agent(scheduler):
    CONTROLLER.start_agent(scheduler)


def get_queue_status_json(tenant_name, pipeline_name, change_queue):
    return CONTROLLER.get_queue_status_json(
        tenant_name, pipeline_name, change_queue)


def get_pipeline_status_json(tenant_name, pipeline_name):
    return CONTROLLER.get_pipeline_status_json(tenant_name, pipeline_name)
