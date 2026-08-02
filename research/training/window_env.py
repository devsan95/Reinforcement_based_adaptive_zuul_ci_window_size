#!/usr/bin/env python3
"""Gymnasium environment shim for offline PPO training on window decisions."""

from __future__ import annotations

import math
import random
from typing import Optional, Tuple

import gymnasium as gym
import numpy as np
from gymnasium import spaces

ACTION_DELTAS = np.array([-2, -1, 0, 1, 2], dtype=np.int32)


class GateWindowEnv(gym.Env):
    metadata = {"render_modes": []}

    def __init__(self, failure_rate: float = 0.2, seed: Optional[int] = None):
        super().__init__()
        self.failure_rate = failure_rate
        self.window = 20
        self.window_floor = 3
        self.window_ceiling = 50
        self.queue_depth = 0
        self._rng = random.Random(seed)
        self.observation_space = spaces.Box(
            low=0.0, high=1.0, shape=(6,), dtype=np.float32)
        self.action_space = spaces.Discrete(len(ACTION_DELTAS))

    def _obs(self) -> np.ndarray:
        hour = 12.0
        hour_rad = 2 * math.pi * hour / 24.0
        return np.array([
            self.window / self.window_ceiling,
            min(1.0, self.queue_depth / self.window_ceiling),
            self.failure_rate,
            math.sin(hour_rad),
            math.cos(hour_rad),
            min(1.0, self.queue_depth / max(self.window, 1)),
        ], dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        if seed is not None:
            self._rng.seed(seed)
        self.window = 20
        self.queue_depth = self._rng.randint(0, 30)
        return self._obs(), {}

    def step(self, action: int):
        delta = int(ACTION_DELTAS[action])
        self.window = max(
            self.window_floor,
            min(self.window_ceiling, self.window + delta))

        succeeded = self._rng.random() >= self.failure_rate
        if succeeded:
            self.queue_depth = max(0, self.queue_depth - self.window)
        else:
            self.queue_depth += self._rng.randint(1, 5)

        merge_rate = 1.0 / max(1, self.queue_depth + 1)
        waste_rate = 0.0 if succeeded else self.window / self.window_ceiling
        reward = merge_rate - waste_rate

        terminated = self.queue_depth == 0
        truncated = False
        return self._obs(), reward, terminated, truncated, {
            "succeeded": succeeded,
            "window": self.window,
        }


gym.register(
    id="GateWindow-v0",
    entry_point="training.window_env:GateWindowEnv",
)
