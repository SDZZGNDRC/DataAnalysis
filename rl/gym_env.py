"""Gymnasium environment for the BTC-USDT-SWAP RL policy."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional

import gymnasium as gym
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest

from rl.policy_core import (
    ACTION_TARGET_FRACTIONS,
    N_ACTIONS,
    OBS_DIM,
    RLPolicyCore,
    resolve_mid,
    terminal_liquidation_cost,
)

# Backwards-compatible public names used by older scripts.
ACTION_DELTA = ACTION_TARGET_FRACTIONS
_resolve_mid = resolve_mid


def activity_gate_penalty(
    num_trades: float,
    min_trades: float,
    ineligible_penalty: float,
) -> float:
    """Evaluation-only checkpoint eligibility penalty."""
    if min_trades <= 0 or num_trades >= min_trades:
        return 0.0
    return max(0.0, float(ineligible_penalty))


def load_manifest(
    manifest_csv: str,
    split: Optional[str] = None,
    *,
    max_events: int = 0,
    max_segments: int = 0,
) -> List[str]:
    manifest = pd.read_csv(manifest_csv)
    if (
        "schema_version" not in manifest
        or not manifest["schema_version"].eq("exact-segment-v2").all()
    ):
        raise ValueError(
            "RL training requires an exact-segment-v2 manifest; "
            "rebuild NPZ files and run scripts/make_manifest.py"
        )
    if split is not None:
        manifest = manifest[manifest["split"] == split]
    if "actual_start_ns" in manifest:
        manifest = manifest.sort_values(["actual_start_ns", "seg_index"])
    if max_events > 0:
        manifest = manifest[
            manifest["event_count"].astype(int) <= int(max_events)
        ]
    if max_segments > 0 and len(manifest) > max_segments:
        indices = np.linspace(
            0, len(manifest) - 1, num=max_segments, dtype=int
        )
        manifest = manifest.iloc[np.unique(indices)]
    if manifest.empty:
        raise ValueError(
            f"manifest split {split!r} empty after resource filters"
        )
    return manifest["npz_path"].tolist()


class BTCUSDSwapMapsEnv(gym.Env):
    """One exact session segment per episode with passive discrete actions."""

    metadata = {"render_modes": []}

    def __init__(
        self,
        seg_npz_paths: List[str],
        contract: dict,
        max_position_lots: float = 10.0,
        order_qty_lots: float = 1.0,
        step_ns: int = 2_000_000_000,
        max_seg_hours: float = 8.0,
        warmup_steps: Optional[int] = None,
        reward_scale: float = 1.0,
        reward_churn_penalty: float = 0.0,
        min_order_lifetime_ns: int = 5_000_000_000,
        max_order_lifetime_ns: int = 15_000_000_000,
        reprice_threshold_ticks: float = 1.0,
        eval_min_trades: float = 0.0,
        eval_ineligible_penalty: float = 0.0,
        selection_mode: str = "random",
        seed: Optional[int] = None,
    ):
        super().__init__()
        if not seg_npz_paths:
            raise ValueError("seg_npz_paths empty")
        self.seg_paths = list(seg_npz_paths)
        self.contract = dict(contract)
        self.max_position = float(max_position_lots) * self.contract["lot_size"]
        self.order_qty = float(order_qty_lots) * self.contract["lot_size"]
        self.step_ns = int(step_ns)
        self.max_seg_hours = float(max_seg_hours)
        self.reward_scale = float(reward_scale)
        self.reward_churn_penalty = float(reward_churn_penalty)
        self.eval_min_trades = max(0.0, float(eval_min_trades))
        self.eval_ineligible_penalty = max(
            0.0, float(eval_ineligible_penalty)
        )
        if selection_mode not in ("random", "cycle"):
            raise ValueError("selection_mode must be 'random' or 'cycle'")
        self.selection_mode = selection_mode
        self._next_seg_idx = 0
        self.report_notional = float(
            self.contract.get("report_notional_usdt", self.contract["initial_balance"])
        )

        self.action_space = gym.spaces.Discrete(N_ACTIONS)
        self.observation_space = gym.spaces.Box(
            low=-np.inf, high=np.inf, shape=(OBS_DIM,), dtype=np.float32
        )

        self._rng = np.random.default_rng(seed)
        self._hbt = None
        self._data = None
        self._prev_equity = 0.0
        self._terminated = False
        self._core = RLPolicyCore(
            step_ns=self.step_ns,
            max_position=self.max_position,
            order_qty=self.order_qty,
            report_notional=self.report_notional,
            min_order_lifetime_ns=min_order_lifetime_ns,
            max_order_lifetime_ns=max_order_lifetime_ns,
            reprice_threshold_ticks=reprice_threshold_ticks,
        )
        self.warmup_steps = (
            self._core.recommended_warmup_steps
            if warmup_steps is None
            else max(0, int(warmup_steps))
        )

    def _build_hbt(self, seg_path: str) -> None:
        with np.load(seg_path) as source:
            data = source["data"]
        if self.max_seg_hours > 0:
            first_ts = int(data["exch_ts"].min())
            cutoff = first_ts + int(self.max_seg_hours * 3600 * 1_000_000_000)
            # Event arrays are ordered by the exchange/local event merge, not by
            # exch_ts alone.  Boolean filtering preserves that event order.
            data = data[data["exch_ts"] <= cutoff]
        self._data = data
        asset = (
            BacktestAsset()
            .data([data])
            .linear_asset(1.0)
            .constant_order_latency(10_000_000, 10_000_000)
            .risk_adverse_queue_model()
            .no_partial_fill_exchange()
            .trading_value_fee_model(
                self.contract["maker_fee"], self.contract["taker_fee"]
            )
            .tick_size(self.contract["tick_size"])
            .lot_size(self.contract["lot_size"])
            .last_trades_capacity(1_000_000)
        )
        self._hbt = HashMapMarketDepthBacktest([asset])

    def _close_hbt(self) -> None:
        if self._hbt is not None:
            try:
                self._hbt.close()
            except Exception:
                pass
            self._hbt = None

    def _info(
        self,
        equity: float,
        liquidation_cost: float = 0.0,
        activity_penalty: float = 0.0,
    ) -> dict:
        state = self._hbt.state_values(0)
        return {
            "equity": float(equity),
            "position": float(state.position),
            "fee": float(state.fee),
            "liquidation_cost": float(liquidation_cost),
            "n_trades": float(state.num_trades),
            "trading_volume": float(state.trading_volume),
            "activity_eligible": bool(
                self.eval_min_trades <= 0
                or float(state.num_trades) >= self.eval_min_trades
            ),
            "activity_penalty": float(activity_penalty),
        }

    def reset(self, *, seed: Optional[int] = None, options: Optional[dict] = None):
        super().reset(seed=seed)
        if seed is not None:
            self._rng = np.random.default_rng(seed)
        self._close_hbt()
        if self.selection_mode == "cycle":
            seg = self.seg_paths[self._next_seg_idx % len(self.seg_paths)]
            self._next_seg_idx += 1
        else:
            seg = self.seg_paths[int(self._rng.integers(0, len(self.seg_paths)))]
        self._build_hbt(seg)
        self._core.reset(self._hbt.state_values(0))
        self._terminated = False

        snapshot = None
        for _ in range(self.warmup_steps):
            rc = self._hbt.elapse(self.step_ns)
            if rc != 0:
                raise RuntimeError(
                    f"segment exhausted during {self.warmup_steps}-step warmup: {seg}"
                )
            self._hbt.clear_inactive_orders(0)
            snapshot = self._core.observe(self._hbt)
        if snapshot is None:
            rc = self._hbt.elapse(self.step_ns)
            if rc != 0:
                raise RuntimeError(f"segment has no usable market interval: {seg}")
            snapshot = self._core.observe(self._hbt)

        self._prev_equity = snapshot.equity
        return snapshot.obs, self._info(snapshot.equity)

    def step(self, action):
        if self._terminated:
            raise RuntimeError("step() called after termination; call reset()")

        submit_rc = self._core.apply_action(self._hbt, int(action))
        if submit_rc != 0:
            raise RuntimeError(f"RL order request failed with code {submit_rc}")
        rc = self._hbt.elapse(self.step_ns)
        terminated = rc != 0
        snapshot = self._core.observe(self._hbt)
        equity = snapshot.equity
        reward = (equity - self._prev_equity) * self.reward_scale
        if self.reward_churn_penalty > 0:
            reward -= self.reward_churn_penalty * snapshot.traded_qty

        liquidation_cost = 0.0
        activity_penalty = 0.0
        if terminated:
            liquidation_cost = terminal_liquidation_cost(
                self._hbt.state_values(0),
                self._hbt.depth(0),
                self.contract["taker_fee"],
            )
            equity -= liquidation_cost
            reward -= liquidation_cost * self.reward_scale
            activity_penalty = activity_gate_penalty(
                snapshot.num_trades,
                self.eval_min_trades,
                self.eval_ineligible_penalty,
            )
            reward -= activity_penalty
            self._terminated = True

        self._prev_equity = equity
        return (
            snapshot.obs,
            float(reward),
            terminated,
            False,
            self._info(equity, liquidation_cost, activity_penalty),
        )

    def close(self):
        self._close_hbt()


def make_env(seg_paths: List[str], contract: dict, seed: int, **env_kwargs):
    def _fn():
        return BTCUSDSwapMapsEnv(seg_paths, contract, seed=seed, **env_kwargs)

    return _fn
