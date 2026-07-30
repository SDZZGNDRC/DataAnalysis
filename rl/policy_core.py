"""Shared RL observation and order-execution logic.

Both the Gym environment and the inference strategy use this module.  Keeping
the state machine in one place prevents training/backtest drift.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np

from hftbacktest import BUY, SELL, GTX, LIMIT

OBS_DIM = 25
N_ACTIONS = 5
POLICY_SCHEMA_VERSION = "rl-policy-v2"
ACTION_DELTA_LOTS = {0: 2.0, 1: 1.0, 2: 0.0, 3: -1.0, 4: -2.0}


def resolve_mid(best_bid: float, best_ask: float) -> float:
    if np.isfinite(best_bid) and np.isfinite(best_ask) and best_bid > 0 and best_ask > 0:
        return 0.5 * (best_bid + best_ask)
    if np.isfinite(best_bid) and best_bid > 0:
        return float(best_bid)
    if np.isfinite(best_ask) and best_ask > 0:
        return float(best_ask)
    return 0.0


def terminal_liquidation_cost(state, depth, taker_fee: float) -> float:
    """Cost versus mid of flattening the terminal position immediately."""
    position = float(state.position)
    if position == 0:
        return 0.0
    mid = resolve_mid(depth.best_bid, depth.best_ask)
    if mid <= 0:
        return 0.0
    exit_px = float(depth.best_bid if position > 0 else depth.best_ask)
    if not np.isfinite(exit_px) or exit_px <= 0:
        exit_px = mid
    spread_cost = abs(position) * abs(mid - exit_px)
    fee = abs(position * exit_px) * float(taker_fee)
    return float(spread_cost + fee)


@dataclass
class StepSnapshot:
    obs: np.ndarray
    equity: float
    mid: float
    traded_qty: float
    num_trades: float


class RLPolicyCore:
    """Stateful feature builder and asynchronous passive-order manager."""

    def __init__(
        self,
        *,
        step_ns: int,
        max_position: float,
        order_qty: float,
        report_notional: float,
        asset_no: int = 0,
    ):
        if step_ns <= 0 or max_position <= 0 or order_qty <= 0:
            raise ValueError("step_ns, max_position and order_qty must be positive")
        self.step_ns = int(step_ns)
        self.max_position = float(max_position)
        self.order_qty = float(order_qty)
        self.report_notional = float(report_notional)
        self.asset_no = int(asset_no)
        self._lags = tuple(
            max(1, int(round(minutes * 60 * 1_000_000_000 / self.step_ns)))
            for minutes in (1, 5, 15)
        )
        self._one_minute_steps = self._lags[0]
        self._five_minute_steps = self._lags[1]
        self._max_history = self._lags[-1] + 2
        self.reset()

    @property
    def recommended_warmup_steps(self) -> int:
        return self._lags[-1] + 1

    def reset(self, state=None) -> None:
        self.mid_history: list[float] = []
        self.last_action_signed = 0.0
        self.last_fill_ts: Optional[int] = None
        self.prev_position = float(state.position) if state is not None else 0.0
        self.prev_num_trades = float(state.num_trades) if state is not None else 0.0
        self.prev_trading_volume = float(state.trading_volume) if state is not None else 0.0
        self.step_idx = 0
        self._next_order_id = 1_000_000

    @staticmethod
    def equity(state, mid: float) -> float:
        return float(state.balance + state.position * mid - state.fee)

    def _standardised_return(self, mid: float, lag: int) -> float:
        if mid <= 0 or len(self.mid_history) < lag:
            return 0.0
        prev = self.mid_history[-lag]
        if prev <= 0:
            return 0.0
        ret = mid / prev - 1.0
        hist = np.asarray(self.mid_history[-max(self._one_minute_steps, 3):], dtype=np.float64)
        if len(hist) >= 3:
            one_step = np.diff(hist) / hist[:-1]
            vol = float(np.nanstd(one_step))
        else:
            vol = 0.0
        denom = max(vol * np.sqrt(lag), 1e-6)
        return float(np.clip(ret / denom, -5.0, 5.0))

    def _range_and_vol(self, mid: float, steps: int) -> tuple[float, float]:
        values = np.asarray((self.mid_history + [mid])[-steps:], dtype=np.float64)
        values = values[np.isfinite(values) & (values > 0)]
        if len(values) < 2 or mid <= 0:
            return 0.0, 0.0
        price_range = float((values.max() - values.min()) / mid)
        returns = np.diff(values) / values[:-1]
        return price_range, float(np.nanstd(returns))

    def observe(self, hbt) -> StepSnapshot:
        depth = hbt.depth(self.asset_no)
        state = hbt.state_values(self.asset_no)
        mid = resolve_mid(depth.best_bid, depth.best_ask)
        now = int(hbt.current_timestamp)

        num_trades = float(state.num_trades)
        traded_qty = max(0.0, float(state.trading_volume) - self.prev_trading_volume)
        delta_position = float(state.position) - self.prev_position
        fill_signal = 0.0
        if num_trades > self.prev_num_trades:
            self.last_fill_ts = now
            if self.order_qty > 0:
                fill_signal = float(np.clip(delta_position / self.order_qty, -1.0, 1.0))
        self.prev_position = float(state.position)
        self.prev_num_trades = num_trades
        self.prev_trading_volume = float(state.trading_volume)

        obs = np.zeros(OBS_DIM, dtype=np.float32)
        for i, lag in enumerate(self._lags):
            obs[i] = self._standardised_return(mid, lag)

        bbq = float(depth.best_bid_qty)
        baq = float(depth.best_ask_qty)
        obs[3] = float((bbq - baq) / (bbq + baq + 1e-9))

        qty_scale = max(self.order_qty * 10.0, float(depth.lot_size), 1e-9)
        try:
            bid_tick = int(depth.best_bid_tick)
            ask_tick = int(depth.best_ask_tick)
            cumulative_ask = 0.0
            cumulative_bid = 0.0
            for level in range(5):
                cumulative_ask += float(depth.ask_qty_at_tick(ask_tick + level))
                cumulative_bid += float(depth.bid_qty_at_tick(bid_tick - level))
                obs[4 + 2 * level] = cumulative_ask / qty_scale
                obs[5 + 2 * level] = cumulative_bid / qty_scale
        except Exception:
            pass

        obs[14] = float(np.clip(state.position / self.max_position, -1.0, 1.0))
        equity = self.equity(state, mid)
        obs[15] = equity / self.report_notional
        obs[16] = float(state.fee) / self.report_notional
        if self.last_fill_ts is None:
            obs[17] = 1.0
        else:
            obs[17] = float(np.clip((now - self.last_fill_ts) / 60_000_000_000, 0.0, 1.0))

        obs[18], obs[19] = self._range_and_vol(mid, self._one_minute_steps)
        obs[20], obs[21] = self._range_and_vol(mid, self._five_minute_steps)
        obs[22] = fill_signal
        obs[23] = 0.0  # Do not leak a deterministic episode clock into the policy.
        obs[24] = self.last_action_signed

        if mid > 0:
            self.mid_history.append(mid)
            if len(self.mid_history) > self._max_history:
                del self.mid_history[:-self._max_history]
        self.step_idx += 1
        return StepSnapshot(obs, equity, mid, traded_qty, num_trades)

    def _cancel_active_orders(self, hbt) -> tuple[float, float]:
        pending_buy = 0.0
        pending_sell = 0.0
        values = hbt.orders(self.asset_no).values()
        while values.has_next():
            order = values.get()
            if order.side == BUY:
                pending_buy += float(order.leaves_qty)
            elif order.side == SELL:
                pending_sell += float(order.leaves_qty)
            if order.cancellable:
                cancel_rc = int(hbt.cancel(
                    self.asset_no, order.order_id, False
                ))
                if cancel_rc != 0:
                    raise RuntimeError(
                        f"cancel order {order.order_id} failed with code {cancel_rc}"
                    )
        return pending_buy, pending_sell

    def apply_action(self, hbt, action: int) -> int:
        action = int(action)
        if action not in ACTION_DELTA_LOTS:
            action = 2
        signed_lots = ACTION_DELTA_LOTS[action]
        self.last_action_signed = float(np.sign(signed_lots))

        hbt.clear_inactive_orders(self.asset_no)
        pending_buy, pending_sell = self._cancel_active_orders(hbt)
        if signed_lots == 0:
            return 0

        depth = hbt.depth(self.asset_no)
        position = float(hbt.state_values(self.asset_no).position)
        requested = abs(signed_lots) * self.order_qty
        order_id = self._next_order_id
        self._next_order_id += 1

        if signed_lots > 0:
            room = self.max_position - position - pending_buy
            qty = min(requested, max(0.0, room))
            price = float(depth.best_bid)
            if qty <= 0 or not np.isfinite(price) or price <= 0:
                return 0
            return int(hbt.submit_buy_order(
                self.asset_no, order_id, price, qty, GTX, LIMIT, False
            ))

        room = self.max_position + position - pending_sell
        qty = min(requested, max(0.0, room))
        price = float(depth.best_ask)
        if qty <= 0 or not np.isfinite(price) or price <= 0:
            return 0
        return int(hbt.submit_sell_order(
            self.asset_no, order_id, price, qty, GTX, LIMIT, False
        ))
