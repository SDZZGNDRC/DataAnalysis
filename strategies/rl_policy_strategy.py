r"""Phase RL4: load a trained PPO model and use it as an hftbacktest strategy.

Non-@njit function call形态（与其它策略通过同一个 worker_grid 调用）。
策略逐 step：
  1. elapse(step_ns)
  2. 构建 25 维 obs（与 gym_env 相同口径）
  3. model.predict(obs) -> 离散动作
  4. action -> 被动 GTX 限价单（与 env._apply_action 相同）
  5. recorder.record(hbt)

复用 rl.gym_env 中的 obs 构建与 action 映射逻辑，保证训练/策略评估口径一致。
"""
import sys
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    proj = Path(__file__).resolve().parents[1]
    if str(proj) not in sys.path:
        sys.path.insert(0, str(proj))

from hftbacktest import GTX, LIMIT

from rl.gym_env import (
    BTCUSDSwapMapsEnv, OBS_DIM, N_ACTIONS, ACTION_DELTA, _resolve_mid,
)


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
):
    """Run inference loop driving hbt + recorder using a PPO policy."""
    from stable_baselines3 import PPO

    if contract is None:
        contract = {"tick_size": 0.1, "lot_size": 0.01,
                    "maker_fee": 0.0002, "taker_fee": 0.0007,
                    "report_notional_usdt": 10000.0}

    model = PPO.load(model_path, device="cpu")
    depth0 = hbt.depth(0)
    lot = depth0.lot_size
    max_position = max_position_lots * lot
    order_qty = order_qty_lots * lot
    notional = float(contract.get("report_notional_usdt", 1000.0))

    asset_no = 0
    mid_history = []
    high_window = []; low_window = []
    last_trade_ts = hbt.current_timestamp or 0
    last_action_signed = 0.0
    step_idx = 0
    prev_eq = 0.0

    def build_obs(dd, mid, st, signed):
        obs = np.zeros(OBS_DIM, dtype=np.float32)
        for i, lag in enumerate((1, 5, 15)):
            if len(mid_history) > lag:
                prev = mid_history[-1 - lag]
                if prev > 0 and mid > 0:
                    vol = np.std(np.diff(np.asarray(mid_history[-30:]))) if len(mid_history) > 5 else (mid * 1e-4)
                    denom = vol if vol > 1e-9 else (mid * 1e-4)
                    obs[i] = float(np.clip((mid - prev) / denom, -5.0, 5.0))
        bbq = dd.best_bid_qty; baq = dd.best_ask_qty
        obs[3] = float((bbq - baq) / (bbq + baq + 1e-9))
        lot_l = dd.lot_size if dd.lot_size > 0 else 1e-9
        denom = (mid * lot_l * 10.0) if mid > 0 else 1.0
        try:
            bt = dd.best_bid_tick; at = dd.best_ask_tick
            cum_ask = float(dd.ask_qty_at_tick(at + 1)) + float(dd.ask_qty_at_tick(at + 2)) + float(dd.ask_qty_at_tick(at + 3)) + float(dd.ask_qty_at_tick(at + 4)) + float(dd.ask_qty_at_tick(at + 5))
            cum_bid = float(dd.bid_qty_at_tick(bt - 1)) + float(dd.bid_qty_at_tick(bt - 2)) + float(dd.bid_qty_at_tick(bt - 3)) + float(dd.bid_qty_at_tick(bt - 4)) + float(dd.bid_qty_at_tick(bt - 5))
            obs[4] = cum_ask / denom; obs[5] = cum_bid / denom; obs[6] = (cum_ask - cum_bid) / denom
            obs[7] = (cum_ask - cum_bid) / denom; obs[8] = 0.0
            obs[9] = cum_ask / denom; obs[10] = cum_bid / denom; obs[11] = (cum_ask - cum_bid) / denom
            obs[12] = cum_ask / denom; obs[13] = cum_bid / denom
        except Exception:
            pass
        obs[14] = float(np.clip(st.position / max_position, -1.0, 1.0))
        obs[15] = float(st.balance / notional)
        obs[16] = float(st.fee / notional)
        if hbt.current_timestamp and last_trade_ts:
            dt = (hbt.current_timestamp - last_trade_ts) / step_ns
            obs[17] = float(np.clip(dt, 0.0, 1.0))
        if high_window and low_window and mid > 0:
            hi = np.max(high_window[-30:]); lo = np.min(low_window[-30:])
            obs[18] = float((hi - lo) / mid)
            obs[19] = float((hi - mid) / mid) if hi > 0 else 0.0
            obs[20] = float((mid - lo) / mid) if lo > 0 else 0.0
            if len(mid_history) > 15:
                obs[21] = float(np.std(np.diff(np.asarray(mid_history[-30:]))) / mid)
        obs[23] = float(min(step_idx, 10000) / 10000.0)
        obs[24] = float(signed)
        return obs

    exit_code = 0
    while step_idx < max_steps:
        exit_code = hbt.elapse(step_ns)
        if exit_code != 0:
            break
        hbt.clear_inactive_orders(asset_no)
        depth = hbt.depth(asset_no)
        state = hbt.state_values(asset_no)
        mid = _resolve_mid(depth.best_bid, depth.best_ask)
        if mid <= 0:
            continue
        obs = build_obs(depth, mid, state, last_action_signed)
        action, _ = model.predict(obs[None, :], deterministic=deterministic)
        action = int(action.item()) if hasattr(action, "item") else int(action)
        # clamp to valid actions
        if action < 0 or action >= N_ACTIONS:
            action = 2
        signed = ACTION_DELTA[action]
        if signed != 0:
            n = int(abs(signed))
            for _k in range(n):
                if signed > 0:
                    px = depth.best_bid
                    if px > 0 and np.isfinite(px):
                        pos = hbt.state_values(asset_no).position
                        room = max_position - pos
                        q = min(order_qty, max(0.0, room))
                        if q > 0:
                            hbt.submit_buy_order(asset_no, 11, px, q, GTX, LIMIT, False)
                else:
                    px = depth.best_ask
                    if px > 0 and np.isfinite(px):
                        pos = hbt.state_values(asset_no).position
                        room = max_position + pos
                        q = min(order_qty, max(0.0, room))
                        if q > 0:
                            hbt.submit_sell_order(asset_no, 22, px, q, GTX, LIMIT, False)
            last_trade_ts = hbt.current_timestamp
        last_action_signed = float(np.sign(signed))
        hbt.clear_inactive_orders(asset_no)

        # update history
        mid_history.append(mid)
        high_window.append(depth.best_ask)
        low_window.append(depth.best_bid)
        if len(mid_history) > 100:
            mid_history = mid_history[-100:]; high_window = high_window[-100:]; low_window = low_window[-100:]

        recorder.record(hbt)
        step_idx += 1

    hbt.clear_inactive_orders(asset_no)
    return exit_code


def is_success(exit_code):
    return exit_code in (1, 2, 15)