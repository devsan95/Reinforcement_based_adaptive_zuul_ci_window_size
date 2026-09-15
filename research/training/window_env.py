#!/usr/bin/env python3
"""Gymnasium environment shim for offline PPO training on window decisions."""

from __future__ import annotations

import random
from typing import Optional, Tuple

import gymnasium as gym
import numpy as np
from gymnasium import spaces

ACTION_DELTAS = np.array([-2, -1, 0, 1, 2], dtype=np.int32)

# Must match zuul.rl_window.STATE_LABELS exactly — this is the live
# scheduler's actual observation layout (get_rl_state), not the original
# proposal's hour_sin/hour_cos design. Training on a state vector that
# does not match what the network is fed at inference time is a
# train/serve skew bug in its own right, independent of the reward fix
# below (an earlier revision of this file trained on constant
# hour_sin/hour_cos placeholders in the dims where the live scheduler
# actually feeds live success_streak/executor_available).
STATE_LABELS = (
    "norm_window", "queue_saturation", "failure_rate",
    "success_streak", "executor_available", "queue_pressure",
)
SUCCESS_STREAK_NORM = 10.0

# TCP shadow defaults, matching the live pipeline config (gate-pipeline.yaml)
# and zuul.rl_window._advance_tcp_shadow: linear +1 on success, exponential
# /2 on failure, floor 3.
TCP_INCREASE_STEP = 1
TCP_DECREASE_FACTOR = 2


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _symmetric_pct_diff(agent_val: float, tcp_val: float,
                        eps: float = 1e-6) -> float:
    """Bounded "percentage change vs TCP" that stays well-defined near zero.

    A literal ``(agent - tcp) / tcp * 100`` explodes whenever tcp_val is
    small (a near-zero denominator), which is common here since merge_rate
    and waste_rate are both naturally close to zero much of the time.
    Dividing by the *average* of the two instead of tcp_val alone gives a
    symmetric measure bounded to [-200, 200] (100% at either extreme,
    0% when the two match) while still capturing "how much better/worse
    is the agent than TCP right now" — the proposal's actual comparison.
    """
    denom = (agent_val + tcp_val) / 2.0
    if denom <= eps:
        return 0.0
    return (agent_val - tcp_val) / denom * 100.0


class GateWindowEnv(gym.Env):
    """Simplified statistical simulation of gate-pipeline window dynamics.

    Reward implements the research proposal's actual specification
    (Table 7.2 / thesis Section 3.2.7): ``reward = delta_merge_rate -
    lam * delta_waste_rate``, both expressed as the agent's percentage
    difference from a TCP-only shadow window run in parallel within the
    same episode, subject to the *same* pass/fail outcome stream (paired
    "common random numbers", matching how the live scheduler's TCP shadow
    is computed from the same succeeded flag as the real cycle). This
    replaces an earlier absolute-reward formula
    (``merge_rate - waste_rate``, no TCP comparison at all) that made
    "always shrink to the floor" the reward-maximising policy regardless
    of state once failure rates were randomised across a wide range —
    a real, observed mode collapse, not a hypothetical concern.

    ``failure_rate`` is the ambient flakiness level for one *episode*. If a
    fixed ``failure_rate`` is given, every episode uses that same value
    (matches the historical behaviour, used by evaluate_baselines.py to
    test a policy at one specific rate). Otherwise each ``reset()`` draws a
    fresh rate uniformly from [min_failure_rate, max_failure_rate], so a
    single training run sees both quiet and sustained-failure-burst
    episodes — required for the agent to ever observe, and learn from,
    high-failure states instead of extrapolating into them blindly at
    inference time.
    """

    metadata = {"render_modes": []}

    def __init__(self, failure_rate: Optional[float] = None,
                min_failure_rate: Optional[float] = None,
                max_failure_rate: Optional[float] = None,
                lam: float = 1.0,
                max_steps: int = 300,
                executor_capacity: int = 50,
                seed: Optional[int] = None):
        super().__init__()
        # Matches RL_WINDOW_EXECUTOR_CAPACITY's live default (50). Used
        # only to turn the [0,1] executor_available ratio into a slot
        # count for the capacity-overrun waste term below.
        self.executor_capacity = executor_capacity
        if failure_rate is not None:
            self._min_failure_rate = failure_rate
            self._max_failure_rate = failure_rate
        else:
            self._min_failure_rate = (
                0.0 if min_failure_rate is None else min_failure_rate)
            self._max_failure_rate = (
                0.9 if max_failure_rate is None else max_failure_rate)
        self.lam = lam
        self.max_steps = max_steps
        self.failure_rate = self._min_failure_rate
        self.window = 8
        self.window_floor = 2
        self.window_ceiling = 25
        self.queue_depth = 0
        self.success_streak = 0
        self.executor_available = 1.0
        # TCP-only shadow, evolves independently of the agent's actions
        # (mirrors zuul.rl_window._advance_tcp_shadow) — used only to
        # compute the relative reward, not exposed in the observation.
        self.tcp_window = 8
        self.tcp_queue_depth = 0
        self._steps = 0
        self._rng = random.Random(seed)
        self.observation_space = spaces.Box(
            low=0.0, high=1.0, shape=(6,), dtype=np.float32)
        self.action_space = spaces.Discrete(len(ACTION_DELTAS))

    def _obs(self) -> np.ndarray:
        return np.array([
            _clip01(self.window / self.window_ceiling),
            _clip01(self.queue_depth / (2.0 * self.window_ceiling)),
            _clip01(self.failure_rate),
            _clip01(self.success_streak / SUCCESS_STREAK_NORM),
            _clip01(self.executor_available),
            _clip01(self.queue_depth / max(self.window, 1)),
        ], dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        if seed is not None:
            self._rng.seed(seed)
        self.window = 8
        self.tcp_window = 8
        self.queue_depth = self._rng.randint(0, 30)
        self.tcp_queue_depth = self.queue_depth
        self.success_streak = 0
        # Randomised per episode, same reasoning as failure_rate below: a
        # fixed start (previously always 1.0 = full availability) means
        # scarce-executor states are rarely visited during training, so
        # the network never gets a reward signal for them.
        self.executor_available = self._rng.uniform(0.0, 1.0)
        self.failure_rate = self._rng.uniform(
            self._min_failure_rate, self._max_failure_rate)
        self._steps = 0
        return self._obs(), {}

    def step(self, action: int):
        delta = int(ACTION_DELTAS[action])
        self.window = max(
            self.window_floor,
            min(self.window_ceiling, self.window + delta))

        # Shared outcome stream: the agent and the TCP shadow face the
        # same pipeline conditions this tick, so the only thing that can
        # differ between them is the window size each chose — an honest,
        # paired counterfactual instead of two independently-noisy runs.
        succeeded = self._rng.random() >= self.failure_rate
        queue_growth = self._rng.randint(1, 5)
        # Executor availability drifts slowly (simplified stand-in for
        # real nodepool/launcher occupancy dynamics).
        self.executor_available = _clip01(
            self.executor_available + self._rng.uniform(-0.05, 0.05))

        if succeeded:
            self.queue_depth = max(0, self.queue_depth - self.window)
            self.success_streak += 1
        else:
            self.queue_depth += queue_growth
            self.success_streak = 0

        # Requesting a bigger window than there is executor capacity to
        # run it is wasteful regardless of whether the cycle itself
        # succeeds or fails — those extra "slots" of window were never
        # actually actionable. Without this term the agent has the
        # executor_available feature as an input but no reward incentive
        # to ever act on it (confirmed by a live sanity check: predictions
        # were identical across the full [0,1] executor_available range
        # before this fix).
        available_slots = self.executor_available * self.executor_capacity

        agent_merge_rate = 1.0 / max(1, self.queue_depth + 1)
        agent_capacity_overrun = (
            max(0.0, self.window - available_slots) / self.window_ceiling)
        agent_waste_rate = (
            0.0 if succeeded else self.window / self.window_ceiling
        ) + agent_capacity_overrun

        # TCP-only shadow: same outcome stream, TCP's own fixed rule.
        if succeeded:
            self.tcp_window = min(
                self.window_ceiling, self.tcp_window + TCP_INCREASE_STEP)
            self.tcp_queue_depth = max(
                0, self.tcp_queue_depth - self.tcp_window)
        else:
            self.tcp_window = max(
                self.window_floor, self.tcp_window // TCP_DECREASE_FACTOR)
            self.tcp_queue_depth += queue_growth
        tcp_merge_rate = 1.0 / max(1, self.tcp_queue_depth + 1)
        tcp_capacity_overrun = (
            max(0.0, self.tcp_window - available_slots) / self.window_ceiling)
        tcp_waste_rate = (
            0.0 if succeeded else self.tcp_window / self.window_ceiling
        ) + tcp_capacity_overrun

        delta_merge_pct = _symmetric_pct_diff(agent_merge_rate, tcp_merge_rate)
        delta_waste_pct = _symmetric_pct_diff(agent_waste_rate, tcp_waste_rate)
        # Scaled to keep typical per-step reward magnitude O(1) rather than
        # O(100) — a plain "% change vs TCP baseline" as literally specified
        # blows up whenever the TCP-side rate is near zero (a raw percentage
        # of a near-zero denominator), which made early training unstable
        # (observed reward swings of +2000%/-500% on adjacent steps). The
        # symmetric-difference formula above already bounds each term to
        # [-200, 200]; dividing by 100 here keeps the reward's *relative*
        # meaning (still Delta_merge - lambda * Delta_waste, still directly
        # comparing the agent to the TCP shadow every step) while giving PPO
        # a well-scaled training signal.
        reward = (delta_merge_pct - self.lam * delta_waste_pct) / 100.0

        self._steps += 1
        terminated = self.queue_depth == 0
        truncated = self._steps >= self.max_steps
        return self._obs(), reward, terminated, truncated, {
            "succeeded": succeeded,
            "window": self.window,
            "tcp_window": self.tcp_window,
            "delta_merge_pct": delta_merge_pct,
            "delta_waste_pct": delta_waste_pct,
        }


gym.register(
    id="GateWindow-v0",
    entry_point="training.window_env:GateWindowEnv",
)
