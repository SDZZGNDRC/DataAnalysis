"""Run a trained PPO model through the same core used during training."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rl.policy_core import N_ACTIONS, POLICY_SCHEMA_VERSION, RLPolicyCore


def rl_policy_strategy(
    hbt,
    recorder,
    model_path,
    step_ns=500_000_000,
    max_position_lots=10.0,
    order_qty_lots=1.0,
    contract=None,
    deterministic=True,
    max_steps=2_000_000,
    warmup_steps=None,
):
    """Drive hftbacktest with a PPO policy and record each decision state."""
    from stable_baselines3 import PPO

    if contract is None:
        contract = {
            "initial_balance": 100_000.0,
            "report_notional_usdt": 100_000.0,
        }

    model = PPO.load(model_path, device="cpu")
    model_schema = getattr(model, "rl_policy_schema_version", None)
    if model_schema != POLICY_SCHEMA_VERSION:
        raise ValueError(
            f"incompatible RL model schema {model_schema!r}; "
            f"expected {POLICY_SCHEMA_VERSION!r}. Retrain the policy."
        )
    trained_config = getattr(model, "rl_policy_env_config", {})
    for key, actual in (
        ("step_ns", step_ns),
        ("max_position_lots", max_position_lots),
        ("order_qty_lots", order_qty_lots),
    ):
        if key in trained_config and float(trained_config[key]) != float(actual):
            raise ValueError(
                f"model was trained with {key}={trained_config[key]!r}, "
                f"but backtest requested {actual!r}"
            )
    report_notional = float(
        contract.get(
            "report_notional_usdt",
            contract.get("initial_balance", 100_000.0),
        )
    )
    if (
        "report_notional_usdt" in trained_config
        and float(trained_config["report_notional_usdt"]) != report_notional
    ):
        raise ValueError(
            "model/report capital mismatch: "
            f"{trained_config['report_notional_usdt']} vs {report_notional}"
        )

    asset_no = 0
    depth = hbt.depth(asset_no)
    lot_size = float(depth.lot_size)
    core = RLPolicyCore(
        step_ns=step_ns,
        max_position=float(max_position_lots) * lot_size,
        order_qty=float(order_qty_lots) * lot_size,
        report_notional=report_notional,
        asset_no=asset_no,
    )
    core.reset(hbt.state_values(asset_no))
    if warmup_steps is None:
        warmup_steps = int(
            trained_config.get("warmup_steps", core.recommended_warmup_steps)
        )
    elif (
        "warmup_steps" in trained_config
        and int(trained_config["warmup_steps"]) != int(warmup_steps)
    ):
        raise ValueError(
            f"model was trained with warmup_steps={trained_config['warmup_steps']}, "
            f"but backtest requested {warmup_steps}"
        )

    for _ in range(max(0, int(warmup_steps))):
        exit_code = hbt.elapse(step_ns)
        if exit_code != 0:
            return exit_code
        hbt.clear_inactive_orders(asset_no)
        core.observe(hbt)

    exit_code = 0
    decisions = 0
    while decisions < int(max_steps):
        exit_code = hbt.elapse(step_ns)
        if exit_code != 0:
            break
        hbt.clear_inactive_orders(asset_no)
        snapshot = core.observe(hbt)
        action, _ = model.predict(snapshot.obs, deterministic=deterministic)
        action = int(action.item()) if hasattr(action, "item") else int(action)
        if action < 0 or action >= N_ACTIONS:
            action = 2
        submit_rc = core.apply_action(hbt, action)
        if submit_rc != 0:
            return submit_rc
        recorder.record(hbt)
        decisions += 1

    hbt.clear_inactive_orders(asset_no)
    return exit_code


def is_success(exit_code):
    return exit_code in (1, 2, 15)
