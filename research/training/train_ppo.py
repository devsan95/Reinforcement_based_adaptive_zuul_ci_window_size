#!/usr/bin/env python3
"""Train a PPO gate-window policy (Phase 3)."""

from __future__ import annotations

import argparse
from pathlib import Path

from stable_baselines3 import PPO
from stable_baselines3.common.env_util import make_vec_env

from training.window_env import GateWindowEnv


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--steps", type=int, default=200_000)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--failure-rate", type=float, default=0.2)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("models/ppo_gate_window.zip"))
    args = parser.parse_args()

    def make_env():
        return GateWindowEnv(failure_rate=args.failure_rate, seed=args.seed)

    env = make_vec_env(make_env, n_envs=1, seed=args.seed)
    model = PPO(
        "MlpPolicy",
        env,
        verbose=1,
        seed=args.seed,
        n_steps=2048,
        batch_size=64,
        learning_rate=3e-4,
    )
    model.learn(total_timesteps=args.steps)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    model.save(str(args.output))
    print(f"Saved policy to {args.output}")


if __name__ == "__main__":
    main()
