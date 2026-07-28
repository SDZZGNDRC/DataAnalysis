r"""Phase RL1: gymnasium environment for BTC-USDT-SWAP using hftbacktest.

BTCUSDSwapMapsEnv: one episode = one (truncated) segment; per step the agent
takes a discrete action; reward = change in account equity (already net of fees).

Action space: Discrete(5)
  0 STRONG_BUY (+2 lots)   1 BUY (+1 lot)   2 HOLD   3 SELL (-1 lot)
  4 STRONG_SELL (-2 lots)
Actions are passive LIMIT @ best bid/ask with GTX (post-only); position clipped
to ±max_position; "strong" actions place TWO orders separated if room remains.

Observation Box(-inf, +inf, OBS_DIM):
  [0:3]  multi-horizon mid returns (1m / 5m / 15m) standardised by recent vol
  [3]    L1 imbalance = (bb_qty-ba_qty)/(bb_qty+ba_qty)
  [4:14] L1..L5 cumulative ask/bid qty (normalised by mid*lot*10)
  [14]   current position / max_position  (in [-1, +1])
  [15]   unrealised_pnl / notional       (scale proxy)
  [16]   fee / notional                  (cumulative cost)
  [17]   ms since last trade / step_ns   [0,1]
  [18:22] recent volatility proxies (1m high-low / mid normalised) x4
  [22]   buy/sell fill imbalance last step (reward shaping proxy, 0 init)
  [23]   step count normalised
  [24]   last action one-hot buy (1) / hold (0) / sell (-1)
Total = 25-dim.

Pure-Python (non-numba) env. Backtest is driven by hbt.elapse(step_ns).
"""
import sys
from pathlib import Path
from typing import List, Optional

import gymnasium as gym
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest, BUY, SELL, GTX, LIMIT

OBS_DIM = 25
N_ACTIONS = 5

# Action -> signed target delta (in lots)
ACTION_DELTA = {0: +2.0, 1: +1.0, 2: 0.0, 3: -1.0, 4: -2.0}


def _resolve_mid(bb, ba):
    if np.isfinite(bb) and np.isfinite(ba) and bb > 0 and ba > 0:
        return 0.5 * (bb + ba)
    if np.isfinite(bb) and bb > 0:
        return bb
    if np.isfinite(ba) and ba > 0:
        return ba
    return 0.0


def load_manifest(manifest_csv: str, split: Optional[str] = None) -> List[str]:
    m = pd.read_csv(manifest_csv)
    if split is not None:
        m = m[m["split"] == split]
    return m["npz_path"].tolist()


class BTCUSDSwapMapsEnv(gym.Env):
    metadata = {"render_modes": []}

    def __init__(
        self,
        seg_npz_paths: List[str],
        contract: dict,
        max_position_lots: float = 10.0,
        order_qty_lots: float = 1.0,
        step_ns: int = 500_000_000,
        max_seg_hours: float = 8.0,
        warmup_steps: int = 5,
        reward_scale: float = 1.0,
        reward_churn_penalty: float = 0.0,
        seed: Optional[int] = None,
    ):
        super().__init__()
        if not seg_npz_paths:
            raise ValueError("seg_npz_paths empty")
        self.seg_paths = list(seg_npz_paths)
        self.contract = dict(contract)
        self.max_position = max_position_lots * self.contract["lot_size"]
        self.order_qty = order_qty_lots * self.contract["lot_size"]
        self.step_ns = int(step_ns)
        self.max_seg_hours = float(max_seg_hours)
        self.warmup_steps = int(warmup_steps)
        self.reward_scale = float(reward_scale)
        self.reward_churn_penalty = float(reward_churn_penalty)

        self.action_space = gym.spaces.Discrete(N_ACTIONS)
        self.observation_space = gym.spaces.Box(low=-np.inf, high=np.inf,
                                                shape=(OBS_DIM,), dtype=np.float32)

        self._rng = np.random.default_rng(seed)
        self._hbt = None
        self._step_idx = 0
        self._prev_equity = 0.0
        self._last_trade_ts = 0
        self._mid_history = []      # windowed mid prices for multi-horizon returns
        self._high_window = []      # rolling highs/lows for volatility proxies
        self._low_window = []
        self._last_action_signed = 0.0
        self._notional = float(self.contract.get("report_notional_usdt", 10_000.0))
        self._data = None          # cached npz array of current seg
        self._data_cut = None

    # ---------- internal helpers ----------

    def _build_hbt(self, seg_path: str):
        with np.load(seg_path) as d:
            data = d["data"]
        if self.max_seg_hours and self.max_seg_hours > 0:
            first_ts = int(data["exch_ts"][0])
            cutoff = first_ts + int(self.max_seg_hours * 3600 * 1_000_000_000)
            idx = int(np.searchsorted(data["exch_ts"], cutoff, side="right"))
            if 0 < idx < len(data):
                data = data[:idx]
        self._data = data
        asset = (
            BacktestAsset()
            .data([data])
            .linear_asset(1.0)
            .constant_order_latency(10_000_000, 10_000_000)
            .risk_adverse_queue_model()
            .no_partial_fill_exchange()
            .trading_value_fee_model(self.contract["maker_fee"], self.contract["taker_fee"])
            .tick_size(self.contract["tick_size"])
            .lot_size(self.contract["lot_size"])
            .last_trades_capacity(1_000_000)
        )
        self._hbt = HashMapMarketDepthBacktest([asset])

    def _cur_equity(self):
        depth = self._hbt.depth(0)
        state = self._hbt.state_values(0)
        mid = _resolve_mid(depth.best_bid, depth.best_ask)
        eq = float(state.balance + state.position * mid - state.fee)
        return eq, state, mid

    def _place_passive(self, side: str, qty: float):
        depth = self._hbt.depth(0)
        if side == "buy":
            px = depth.best_bid
            if not (px > 0 and np.isfinite(px)):
                return
            # clip qty by remaining room
            pos = self._hbt.state_values(0).position
            room = self.max_position - pos
            q = min(qty, max(0.0, room))
            if q <= 0:
                return
            self._hbt.submit_buy_order(0, 11, px, q, GTX, LIMIT, False)
        else:
            px = depth.best_ask
            if not (px > 0 and np.isfinite(px)):
                return
            pos = self._hbt.state_values(0).position
            room = self.max_position + pos   # pos<=0 helps; pos>0 reduces short room
            q = min(qty, max(0.0, room))
            if q <= 0:
                return
            self._hbt.submit_sell_order(0, 22, px, q, GTX, LIMIT, False)

    def _apply_action(self, action: int):
        # cancel resting orders each step (keep simple; passive-only)
        self._hbt.clear_inactive_orders(0)
        signed = ACTION_DELTA[int(action)]
        if signed == 0:
            return 0.0
        n_lots = abs(signed)
        before = self._hbt.state_values(0).position
        for k in range(int(n_lots)):
            self._place_passive("buy" if signed > 0 else "sell", self.order_qty)
        after = self._hbt.state_values(0).position
        delta_pos = after - before
        if abs(delta_pos) > 0:
            self._last_trade_ts = self._hbt.current_timestamp
        return delta_pos

    def _build_obs(self, state, mid):
        obs = np.zeros(OBS_DIM, dtype=np.float32)
        # multi-horizon mid returns
        hist = self._mid_history
        for i, lag in enumerate((1, 5, 15)):  # 1m/5m/15m proxy by step count
            if len(hist) > lag:
                prev = hist[-1 - lag]
                if prev > 0 and mid > 0:
                    vol = np.std(np.diff(np.asarray(hist[-30:]))) if len(hist) > 5 else (mid * 1e-4)
                    denom = vol if vol > 1e-9 else (mid * 1e-4)
                    obs[i] = float(np.clip((mid - prev) / denom, -5.0, 5.0))
        # L1 imbalance
        depth = self._hbt.depth(0)
        bbq = depth.best_bid_qty; baq = depth.best_ask_qty
        obs[3] = float((bbq - baq) / (bbq + baq + 1e-9))
        # L1..L5 cumulative bid/ask qty (use tick ladder if available)
        lot = depth.lot_size if depth.lot_size > 0 else 1e-9
        denom = (mid * lot * 10.0) if mid > 0 else 1.0
        try:
            bt = depth.best_bid_tick
            at = depth.best_ask_tick
            cum_ask = 0.0; cum_bid = 0.0
            for k in range(1, 6):
                cum_ask += float(depth.ask_qty_at_tick(at + k))
                cum_bid += float(depth.bid_qty_at_tick(bt - k))
            obs[4:9] = np.array([cum_ask, cum_bid, cum_ask - cum_bid,
                                 cum_ask - cum_bid, 0.0], dtype=np.float32) / denom
            obs[9:14] = np.array([cum_ask, cum_bid, cum_ask - cum_bid,
                                  cum_ask, cum_bid], dtype=np.float32) / denom
        except Exception:
            pass
        # position normalised
        obs[14] = float(np.clip(state.position / self.max_position, -1.0, 1.0))
        obs[15] = float(state.balance / self._notional)  # realised pnl proxy
        obs[16] = float(state.fee / self._notional)
        if self._last_trade_ts and self._hbt.current_timestamp:
            dt = (self._hbt.current_timestamp - self._last_trade_ts) / self.step_ns
            obs[17] = float(np.clip(dt, 0.0, 1.0))
        # vol proxies from rolling high/low
        if self._high_window and self._low_window and mid > 0:
            hi = np.max(self._high_window[-30:]); lo = np.min(self._low_window[-30:])
            obs[18] = float((hi - lo) / mid)
            obs[19] = float((hi - mid) / mid) if hi > 0 else 0.0
            obs[20] = float((mid - lo) / mid) if lo > 0 else 0.0
            if len(self._mid_history) > 15:
                obs[21] = float(np.std(np.diff(np.asarray(self._mid_history[-30:]))) / mid)
        obs[22] = 0.0  # fill imbalance last step (kept 0; could be wired from last_trades)
        obs[23] = float(min(self._step_idx, 10_000) / 10_000.0)
        obs[24] = float(self._last_action_signed)
        return obs

    # ---------- gym API ----------

    def reset(self, *, seed: Optional[int] = None, options: Optional[dict] = None):
        super().reset(seed=seed)
        if seed is not None:
            self._rng = np.random.default_rng(seed)
        seg = self.seg_paths[int(self._rng.integers(0, len(self.seg_paths)))]
        # if a previous hbt exists, close first
        if self._hbt is not None:
            try:
                self._hbt.close()
            except Exception:
                pass
            self._hbt = None
        self._build_hbt(seg)
        self._step_idx = 0
        self._prev_equity = 0.0
        self._last_trade_ts = self._hbt.current_timestamp or 0
        self._mid_history = []
        self._high_window = []
        self._low_window = []
        self._last_action_signed = 0.0

        # warmup: advance a few steps to populate history
        obs_eq, _, _ = self._cur_equity()
        self._prev_equity = obs_eq
        for _ in range(max(1, self.warmup_steps)):
            self._hbt.elapse(self.step_ns)
            self._hbt.clear_inactive_orders(0)
            depth = self._hbt.depth(0)
            mid = _resolve_mid(depth.best_bid, depth.best_ask)
            self._mid_history.append(mid)
            self._high_window.append(depth.best_ask)
            self._low_window.append(depth.best_bid)
            self._step_idx += 1

        eq, state, mid = self._cur_equity()
        self._prev_equity = eq
        obs = self._build_obs(state, mid)
        info = {"equity": eq, "position": float(state.position), "fee": float(state.fee)}
        return obs, info

    def step(self, action):
        self._step_idx += 1
        rc = self._hbt.elapse(self.step_ns)
        terminated = (rc != 0)
        if not terminated:
            delta_pos = self._apply_action(int(action))
            self._last_action_signed = float(np.sign(ACTION_DELTA[int(action)]))
            self._hbt.clear_inactive_orders(0)
        else:
            delta_pos = 0.0

        depth = self._hbt.depth(0)
        mid = _resolve_mid(depth.best_bid, depth.best_ask)
        if mid > 0:
            self._mid_history.append(mid)
            self._high_window.append(depth.best_ask)
            self._low_window.append(depth.best_bid)
            if len(self._mid_history) > 100:
                self._mid_history = self._mid_history[-100:]
                self._high_window = self._high_window[-100:]
                self._low_window = self._low_window[-100:]

        eq, state, mid_now = self._cur_equity()
        reward = (eq - self._prev_equity) * self.reward_scale
        if self.reward_churn_penalty > 0.0:
            reward -= self.reward_churn_penalty * abs(delta_pos) * self.order_qty
        self._prev_equity = eq
        obs = self._build_obs(state, mid_now)
        info = {"equity": eq, "position": float(state.position), "fee": float(state.fee)}
        if terminated:
            # append a final realisation mark; do not add extra reward beyond equity move
            pass
        truncated = False  # seg exhaust naturally terminates; cap by max_seg_hours done at build
        return obs, float(reward), terminated, truncated, info

    def close(self):
        if self._hbt is not None:
            try:
                self._hbt.close()
            except Exception:
                pass
            self._hbt = None


def make_env(seg_paths: List[str], contract: dict, seed: int, **env_kwargs):
    def _fn():
        env = BTCUSDSwapMapsEnv(seg_paths, contract, seed=seed, **env_kwargs)
        return env
    return _fn