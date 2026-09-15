#!/usr/bin/env python3
"""Train a PPO gate-window policy (Phase 3)."""

from __future__ import annotations

import argparse
from pathlib import Path

from stable_baselines3 import PPO
from stable_baselines3.common.env_util import make_vec_env
from stable_baselines3.common.vec_env import VecNormalize

from training.window_env import GateWindowEnv


def main():
    parser = argparse.ArgumentParser()
    # Proposal Section 7.3 / Table 9.1: "1,000,000 steps" per training run.
    parser.add_argument("--steps", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--failure-rate", type=float, default=None,
        help="Fixed failure rate for every episode (legacy behaviour). "
             "Omit to randomise per-episode across "
             "[--min-failure-rate, --max-failure-rate] instead, which "
             "is required for the agent to see and learn from "
             "high-failure states.")
    parser.add_argument("--min-failure-rate", type=float, default=0.0)
    parser.add_argument("--max-failure-rate", type=float, default=0.9)
    parser.add_argument(
        "--lam", type=float, default=1.0,
        help="Reward sensitivity weight on the waste term "
             "(proposal Table 7.2: lambda in {0.5, 1.0, 2.0}).")
    parser.add_argument("--n-envs", type=int, default=4)
    parser.add_argument(
        "--ent-coef", type=float, default=0.01,
        help="Entropy bonus. SB3's PPO default is 0.0, which let earlier "
             "runs converge to a near-deterministic (and, in one case, "
             "fully collapsed) policy before exploring enough of the "
             "state space. A small positive value keeps exploration "
             "alive longer.")
    parser.add_argument(
        "--net-arch", type=int, default=128,
        help="Hidden layer width (two hidden layers) for the policy/value "
             "MLP. SB3 default is 64; widened given the richer 6-feature, "
             "TCP-relative reward landscape introduced today.")
    parser.add_argument(
        "--norm-reward", action="store_true", default=True,
        help="Wrap the vec env in VecNormalize(norm_obs=False, "
             "norm_reward=True). Per-episode reward here varies hugely "
             "(episodes range from a couple of steps to hundreds, with "
             "waste penalties scaling with both window size and failure "
             "severity), which is a well-known PPO destabiliser — "
             "reward normalisation is the standard fix, not something "
             "specific to this project. norm_obs stays False: "
             "observations are already hand-clipped to [0,1] per feature "
             "(see window_env._clip01), and the live scheduler "
             "(zuul/rl_window.py) feeds the raw, unnormalised "
             "get_rl_state() output straight to model.predict() with no "
             "normalisation layer — normalising observations at train "
             "time without replicating that exact transform at serve "
             "time would be a new, worse train/serve mismatch than any "
             "fixed so far.")
    parser.add_argument("--no-norm-reward", dest="norm_reward",
                        action="store_false")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("models/ppo_gate_window.zip"))
    args = parser.parse_args()

    def make_env():
        if args.failure_rate is not None:
            return GateWindowEnv(
                failure_rate=args.failure_rate, lam=args.lam, seed=args.seed)
        return GateWindowEnv(
            min_failure_rate=args.min_failure_rate,
            max_failure_rate=args.max_failure_rate,
            lam=args.lam,
            seed=args.seed)

    env = make_vec_env(make_env, n_envs=args.n_envs, seed=args.seed)
    if args.norm_reward:
        env = VecNormalize(env, norm_obs=False, norm_reward=True, gamma=0.99)

    model = PPO(
        "MlpPolicy",
        env,
        verbose=1,
        seed=args.seed,
        n_steps=2048,
        batch_size=64,
        learning_rate=3e-4,
        ent_coef=args.ent_coef,
        policy_kwargs=dict(net_arch=[args.net_arch, args.net_arch]),
    )
    model.learn(total_timesteps=args.steps)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    model.save(str(args.output))
    print(f"Saved policy to {args.output}")


if __name__ == "__main__":
    main()
