#!/usr/bin/env python3
"""Compare RL/heuristic policies against TCP and fixed-window baselines."""

from __future__ import annotations

import argparse
import random
from statistics import mean

from training.window_env import ACTION_DELTAS, GateWindowEnv


def simulate(env: GateWindowEnv, policy, episodes: int = 100):
    rewards = []
    for _ in range(episodes):
        obs, _ = env.reset()
        total = 0.0
        for _ in range(200):
            action = policy(obs, env)
            obs, reward, terminated, truncated, _ = env.step(action)
            total += reward
            if terminated or truncated:
                break
        rewards.append(total)
    return rewards


def tcp_policy(obs, env):
    failure_rate = obs[2]
    return 0 if failure_rate > 0.25 else 4


def fixed_policy(obs, env):
    target = 20
    delta = target - env.window
    return int(min(max(delta, -2), 2) + 2)


def random_policy(obs, env):
    return random.randrange(len(ACTION_DELTAS))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--episodes", type=int, default=200)
    parser.add_argument("--failure-rate", type=float, default=0.2)
    args = parser.parse_args()

    baselines = {
        "tcp_heuristic": tcp_policy,
        "fixed_20": fixed_policy,
        "random": random_policy,
    }
    for name, policy in baselines.items():
        env = GateWindowEnv(failure_rate=args.failure_rate)
        rewards = simulate(env, policy, episodes=args.episodes)
        print(f"{name:15s} mean_reward={mean(rewards):.4f}")


if __name__ == "__main__":
    main()
