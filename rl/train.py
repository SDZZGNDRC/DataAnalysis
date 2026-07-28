r"""Phase RL2/RL3: train a PPO policy on BTCUSDSwapMapsEnv.

Usage:
  python rl/train.py --manifest E:\tmp\npz\manifest.csv ^
      --train-split train --val-split val --total-timesteps 5000000 ^
      --out-dir E:\tmp\rl\ckpt --tensorboard-log E:\tmp\rl\tb
"""
import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import SubprocVecEnv
from stable_baselines3.common.callbacks import EvalCallback, CheckpointCallback
from stable_baselines3.common.utils import set_random_seed

from rl.gym_env import BTCUSDSwapMapsEnv, load_manifest, make_env


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--manifest", required=True)
    p.add_argument("--train-split", default="train")
    p.add_argument("--val-split", default="val")
    p.add_argument("--contract", default=str(PROJECT_ROOT / "contracts" / "btc_usdt_swap.json"))
    p.add_argument("--total-timesteps", type=int, default=5_000_000)
    p.add_argument("--n-envs", type=int, default=4)
    p.add_argument("--n-steps", type=int, default=4096)
    p.add_argument("--batch-size", type=int, default=256)
    p.add_argument("--n-epochs", type=int, default=10)
    p.add_argument("--learning-rate", type=float, default=3e-4)
    p.add_argument("--gamma", type=float, default=0.99)
    p.add_argument("--gae-lambda", type=float, default=0.95)
    p.add_argument("--ent-coef", type=float, default=0.01)
    p.add_argument("--clip-range", type=float, default=0.2)
    p.add_argument("--max-seg-hours", type=float, default=8.0)
    p.add_argument("--step-ns", type=int, default=500_000_000)
    p.add_argument("--max-position-lots", type=float, default=10.0)
    p.add_argument("--order-qty-lots", type=float, default=1.0)
    p.add_argument("--reward-churn-penalty", type=float, default=0.0)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--tensorboard-log", default=None)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--resume", default=None, help="path to a .zip to resume from")
    args = p.parse_args()

    contract = json.load(open(args.contract))
    train_paths = load_manifest(args.manifest, args.train_split)
    val_paths = load_manifest(args.manifest, args.val_split)
    print(f"train segs: {len(train_paths)} | val segs: {len(val_paths)}")

    env_kwargs = dict(
        max_seg_hours=args.max_seg_hours,
        step_ns=args.step_ns,
        max_position_lots=args.max_position_lots,
        order_qty_lots=args.order_qty_lots,
        reward_churn_penalty=args.reward_churn_penalty,
    )

    # SubprocVecEnv: each worker reloads npz on reset
    venv = SubprocVecEnv([
        make_env(train_paths, contract, seed=args.seed + i, **env_kwargs)
        for i in range(args.n_envs)
    ])
    # eval env: single env, val split
    eval_env = BTCUSDSwapMapsEnv(val_paths, contract, seed=args.seed + 999, **env_kwargs)

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    callbacks = [
        EvalCallback(
            eval_env,
            best_model_save_path=args.out_dir,
            log_path=args.out_dir,
            eval_freq=max(args.n_steps, 50_000) // args.n_envs,  # in terms of n_envs steps
            n_eval_episodes=1,
            deterministic=True,
            render=False,
        ),
        CheckpointCallback(
            save_freq=max(100_000 // args.n_envs, 1),
            save_path=str(Path(args.out_dir) / "checkpoints"),
            name_prefix="ppo_btc",
        ),
    ]

    if args.resume:
        print(f"resuming from {args.resume}")
        model = PPO.load(args.resume, env=venv, device="cpu",
                         tensorboard_log=args.tensorboard_log)
        model.n_steps = args.n_steps
        reset_ts = False
    else:
        model = PPO(
            "MlpPolicy", venv,
            n_steps=args.n_steps,
            batch_size=args.batch_size,
            n_epochs=args.n_epochs,
            learning_rate=args.learning_rate,
            gamma=args.gamma,
            gae_lambda=args.gae_lambda,
            ent_coef=args.ent_coef,
            clip_range=args.clip_range,
            verbose=1,
            seed=args.seed,
            device="cpu",
            tensorboard_log=args.tensorboard_log,
        )
        reset_ts = True

    model.learn(
        total_timesteps=args.total_timesteps,
        callback=callbacks,
        reset_num_timesteps=reset_ts,
        progress_bar=False,
    )
    model.save(Path(args.out_dir) / "ppo_final.zip")
    print("TRAIN DONE ->", Path(args.out_dir) / "ppo_final.zip")
    venv.close()
    eval_env.close()


if __name__ == "__main__":
    main()