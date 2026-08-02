#!/usr/bin/env python3
"""Build a JSON policy table without SB3 (offline / air-gapped friendly).

Uses a multi-rule policy trained via random search on the GateWindowEnv
reward — no pip dependencies beyond the stdlib.
"""

from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path

ACTION_DELTAS = (-2, -1, 0, 1, 2)


class SimpleGateEnv:
    """Minimal copy of GateWindowEnv for policy search."""

    def __init__(self, failure_rate=0.2, seed=0):
        self.failure_rate = failure_rate
        self.window_floor = 3
        self.window_ceiling = 50
        self._rng = random.Random(seed)
        self.reset()

    def reset(self):
        self.window = 20
        self.queue_depth = self._rng.randint(0, 30)
        return self._obs()

    def _obs(self):
        hour_rad = 2 * math.pi * 12.0 / 24.0
        return [
            self.window / self.window_ceiling,
            min(1.0, self.queue_depth / self.window_ceiling),
            self.failure_rate,
            math.sin(hour_rad),
            math.cos(hour_rad),
            min(1.0, self.queue_depth / max(self.window, 1)),
        ]

    def step(self, action_idx):
        delta = ACTION_DELTAS[action_idx]
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
        done = self.queue_depth == 0
        return self._obs(), reward, done


def policy_from_weights(state, weights):
    """Linear score per action; pick argmax."""
    norm_w, norm_d, fail, sin_h, cos_h, util = state
    features = [
        1.0, norm_w, norm_d, fail, sin_h, cos_h, util,
        norm_w * norm_d, fail * norm_d, util * norm_d,
        fail * fail, norm_d * norm_d,
    ]
    scores = []
    for action_idx, delta in enumerate(ACTION_DELTAS):
        w = weights[action_idx]
        scores.append(sum(f * wf for f, wf in zip(features, w)))
    return max(range(len(ACTION_DELTAS)), key=lambda i: scores[i])


def random_weights(rng):
    return [[rng.uniform(-1, 1) for _ in range(12)] for _ in range(5)]


def train_weights(episodes=400, seed=0):
    rng = random.Random(seed)
    best_weights = random_weights(rng)
    best_score = float("-inf")
    for _ in range(120):
        weights = random_weights(rng)
        total = 0.0
        for ep in range(episodes):
            env = SimpleGateEnv(
                failure_rate=rng.choice([0.05, 0.15, 0.25, 0.35]),
                seed=rng.randint(0, 99999))
            obs = env.reset()
            for _ in range(150):
                action = policy_from_weights(obs, weights)
                obs, reward, done = env.step(action)
                total += reward
                if done:
                    break
        if total > best_score:
            best_score = total
            best_weights = weights
    return best_weights, best_score


def export_grid(weights, precision=2):
    entries = []
    seen = set()
    for norm_w in [i / 10 for i in range(2, 11)]:
        for norm_d in [i / 20 for i in range(0, 21)]:
            for fail in [0.0, 0.05, 0.15, 0.25, 0.35, 0.5]:
                for util in [0.0, 0.25, 0.5, 0.75, 1.0]:
                    state = [norm_w, norm_d, fail, 0.0, 1.0, util]
                    key = tuple(round(x, precision) for x in state)
                    if key in seen:
                        continue
                    seen.add(key)
                    action_idx = policy_from_weights(state, weights)
                    entries.append({
                        "state": list(key),
                        "action_idx": action_idx,
                        "action_delta": ACTION_DELTAS[action_idx],
                    })
    rng = random.Random(0)
    for _ in range(2000):
        env = SimpleGateEnv(failure_rate=rng.choice([0.1, 0.2, 0.3]), seed=rng.randint(0, 9999))
        obs = env.reset()
        for _ in range(40):
            key = tuple(round(x, precision) for x in obs)
            if key not in seen:
                seen.add(key)
                action_idx = policy_from_weights(obs, weights)
                entries.append({
                    "state": list(key),
                    "action_idx": action_idx,
                    "action_delta": ACTION_DELTAS[action_idx],
                })
            action_idx = policy_from_weights(obs, weights)
            obs, _, done = env.step(action_idx)
            if done:
                break
    return entries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path,
                        default=Path("models/ppo_gate_window_table.json"))
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    print("Training lightweight policy weights (stdlib only)...")
    weights, score = train_weights(seed=args.seed)
    print(f"  best reward score: {score:.2f}")

    entries = export_grid(weights)
    payload = {
        "version": 1,
        "action_deltas": list(ACTION_DELTAS),
        "policy_type": "linear_search",
        "entries": entries,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    deltas = [e["action_delta"] for e in entries]
    dist = {d: deltas.count(d) for d in sorted(set(deltas))}
    print(f"Exported {len(entries)} entries to {args.output}")
    print(f"  delta distribution: {dist}")


if __name__ == "__main__":
    main()
