#!/usr/bin/env python3
"""Export a trained PPO policy to a JSON lookup table for the scheduler."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
from stable_baselines3 import PPO

from training.window_env import ACTION_DELTAS, GateWindowEnv


def round_state(state, precision: int = 2):
    return [round(float(x), precision) for x in state]


def export_table(model: PPO, samples: int, seed: int) -> list:
    env = GateWindowEnv(seed=seed)
    table = {}
    rng = np.random.default_rng(seed)

    for _ in range(samples):
        obs, _ = env.reset(seed=int(rng.integers(0, 1_000_000)))
        for _ in range(50):
            action, _ = model.predict(obs, deterministic=True)
            key = tuple(round_state(obs))
            table[key] = int(action)
            action = int(action)
            obs, _, terminated, truncated, _ = env.step(action)
            if terminated or truncated:
                break

    # Grid over representative states
    for norm_window in np.linspace(0.1, 1.0, 8):
        for norm_depth in np.linspace(0.0, 1.0, 11):
            for failure in (0.0, 0.1, 0.2, 0.35, 0.5):
                for util in (0.0, 0.25, 0.5, 0.75, 1.0):
                    obs = np.array([
                        norm_window, norm_depth, failure,
                        0.0, 1.0, util,
                    ], dtype=np.float32)
                    action, _ = model.predict(obs, deterministic=True)
                    key = tuple(round_state(obs))
                    table[key] = int(action)

    entries = [
        {"state": list(k), "action_idx": v, "action_delta": int(ACTION_DELTAS[v])}
        for k, v in sorted(table.items())
    ]
    return entries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=Path,
                        default=Path("models/ppo_gate_window.zip"))
    parser.add_argument("--output", type=Path,
                        default=Path("models/ppo_gate_window_table.json"))
    parser.add_argument("--samples", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    model = PPO.load(str(args.model))
    entries = export_table(model, args.samples, args.seed)
    payload = {
        "version": 1,
        "action_deltas": list(map(int, ACTION_DELTAS)),
        "entries": entries,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    deltas = [e["action_delta"] for e in entries]
    print(f"Exported {len(entries)} states to {args.output}")
    print(f"  action delta distribution: "
          f"{ {d: deltas.count(d) for d in sorted(set(deltas))} }")


if __name__ == "__main__":
    main()
