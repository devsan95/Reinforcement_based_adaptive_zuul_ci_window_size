#!/usr/bin/env python3
"""Compare the trained PPO policy against TCP and other baselines (Phase 3).

Implements the proposal's RQ2 evaluation (Section 3.2.7 / Table 7.1): run
each candidate policy over the same paired set of episodes (common random
numbers — every policy faces the identical failure-rate/queue-growth draw
per episode index, so differences in mean reward reflect the policy, not
noise), then report mean/stdev and a Mann-Whitney U test between the PPO
policy and the TCP baseline.
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path
from statistics import mean, pstdev

from training.window_env import ACTION_DELTAS, GateWindowEnv

try:
    from scipy.stats import mannwhitneyu
except ImportError:  # pragma: no cover - optional
    mannwhitneyu = None


def simulate(env: GateWindowEnv, policy, episodes: int, base_seed: int,
            max_steps: int = 200):
    rewards = []
    for i in range(episodes):
        obs, _ = env.reset(seed=base_seed + i)
        total = 0.0
        for _ in range(max_steps):
            action = policy(obs, env)
            obs, reward, terminated, truncated, _ = env.step(action)
            total += reward
            if terminated or truncated:
                break
        rewards.append(total)
    return rewards


def tcp_policy(obs, env):
    """Crude bang-bang proxy: always the maximum-magnitude action in the
    direction a hard failure_rate threshold suggests. NOT a faithful TCP
    replica — kept for continuity with earlier runs, but genuine_tcp_policy
    below is the correct "does PPO beat real TCP" comparison."""
    failure_rate = obs[2]
    return 0 if failure_rate > 0.25 else 4


def genuine_tcp_policy(obs, env):
    """Chase env.tcp_window — the environment's own internal TCP shadow,
    computed with the same linear-increase/exponential-decrease rule as
    the live scheduler's _advance_tcp_shadow. This is the actual "do what
    TCP does" baseline. It can still lag a real TCP jump (e.g. a halving
    on failure) for a tick or two, because the action space (proposal
    Table 7.2: discrete {-2,-1,0,+1,+2}) bounds how fast *any* policy,
    including this one, can move the window per tick — the same
    constraint the PPO agent operates under, which is what makes this a
    fair, apples-to-apples comparison target."""
    delta = env.tcp_window - env.window
    return int(min(max(delta, -2), 2) + 2)


def fixed_policy(obs, env):
    target = 20
    delta = target - env.window
    return int(min(max(delta, -2), 2) + 2)


def random_policy(obs, env):
    return random.randrange(len(ACTION_DELTAS))


def make_ppo_policy(model_path: Path):
    from stable_baselines3 import PPO
    import numpy as np
    model = PPO.load(str(model_path))

    def ppo_policy(obs, env):
        action, _ = model.predict(np.asarray(obs), deterministic=True)
        return int(action)

    return ppo_policy


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--episodes", type=int, default=500)
    parser.add_argument("--min-failure-rate", type=float, default=0.0)
    parser.add_argument("--max-failure-rate", type=float, default=0.9)
    parser.add_argument("--seed", type=int, default=12345)
    parser.add_argument(
        "--model", type=Path, default=Path("models/ppo_gate_window.zip"))
    parser.add_argument("--alpha", type=float, default=0.05)
    args = parser.parse_args()

    baselines = {
        "tcp_heuristic": tcp_policy,
        "tcp_genuine": genuine_tcp_policy,
        "fixed_20": fixed_policy,
        "random": random_policy,
    }
    if args.model.exists():
        baselines["ppo"] = make_ppo_policy(args.model)
    else:
        print(f"(no PPO checkpoint at {args.model}, skipping ppo baseline)")

    results = {}
    for name, policy in baselines.items():
        env = GateWindowEnv(
            min_failure_rate=args.min_failure_rate,
            max_failure_rate=args.max_failure_rate)
        rewards = simulate(env, policy, episodes=args.episodes,
                           base_seed=args.seed)
        results[name] = rewards
        print(f"{name:15s} mean_reward={mean(rewards):+.4f}  "
             f"stdev={pstdev(rewards):.4f}  n={len(rewards)}")

    if "ppo" in results:
        ppo_rewards = results["ppo"]
        print()
        print("PPO vs every other baseline (paired, same episode draws):")
        for name, other_rewards in results.items():
            if name == "ppo":
                continue
            diff = mean(ppo_rewards) - mean(other_rewards)
            wins = sum(1 for p, o in zip(ppo_rewards, other_rewards) if p > o)
            line = (f"  vs {name:15s} mean_diff={diff:+.4f} "
                   f"win_rate={100 * wins / len(ppo_rewards):.1f}%")
            if mannwhitneyu is not None:
                stat, pvalue = mannwhitneyu(
                    ppo_rewards, other_rewards, alternative="two-sided")
                significant = pvalue < args.alpha
                line += (f"  MWU p={pvalue:.4g} "
                        f"({'SIGNIFICANT' if significant else 'not significant'})")
            print(line)


if __name__ == "__main__":
    main()
