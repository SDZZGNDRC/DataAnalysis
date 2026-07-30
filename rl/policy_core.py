"""Shared RL observation and order-execution logic.

Both the Gym environment and the inference strategy use this module.  Keeping
the state machine in one place prevents training/backtest drift.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np

from hftbacktest import (
    BUY,
    BUY_EVENT,
    SELL,
    SELL_EVENT,
    GTX,
    LIMIT,
)

OBS_DIM = 43
N_ACTIONS = 5
POLICY_SCHEMA_VERSION = "rl-policy-v3-ob43"
ACTION_TARGET_FRACTIONS = {
    0: 1.0,
    1: 0.5,
    2: 0.0,
    3: -0.5,
    4: -1.0,
}
# Backwards-compatible import name. Values are target-position fractions in v3.
ACTION_DELTA_LOTS = ACTION_TARGET_FRACTIONS
OBSERVATION_NAMES = (
    "return_1m_z",
    "return_5m_z",
    "return_15m_z",
    "best_level_imbalance",
    "cumulative_ask_l1",
    "cumulative_bid_l1",
    "cumulative_ask_l2",
    "cumulative_bid_l2",
    "cumulative_ask_l3",
    "cumulative_bid_l3",
    "cumulative_ask_l4",
    "cumulative_bid_l4",
    "cumulative_ask_l5",
    "cumulative_bid_l5",
    "position_fraction",
    "equity_fraction",
    "fee_fraction",
    "fill_recency",
    "range_1m",
    "volatility_1m",
    "range_5m",
    "volatility_5m",
    "fill_signal",
    "reserved",
    "last_action_sign",
    "pending_buy_fraction",
    "pending_sell_fraction",
    "oldest_order_age_fraction",
    "target_position_fraction",
    "spread_ticks",
    "microprice_offset_ticks",
    "trade_imbalance_step",
    "trade_intensity_log_step",
    "trade_imbalance_10s",
    "trade_intensity_log_10s",
    "trade_imbalance_60s",
    "trade_intensity_log_60s",
    "book_ofi_step",
    "book_ofi_10s",
    "book_ofi_60s",
    "return_1s_bps",
    "return_10s_bps",
    "return_30s_bps",
)
MARKET_FEATURE_INDICES = (
    tuple(range(14))
    + tuple(range(18, 22))
    + tuple(range(29, 43))
)
RL_DIAGNOSTIC_FIELDS = (
    "decision_count",
    "invalid_action_count",
    "submit_attempts",
    "submit_successes",
    "buy_submit_successes",
    "sell_submit_successes",
    "cancel_attempts",
    "cancel_successes",
    "signal_cancel_successes",
    "reprice_cancel_successes",
    "expiry_cancel_successes",
    "fill_events",
    "fill_qty",
    "peak_active_orders",
    "mean_cancelled_order_age_ms",
    "max_cancelled_order_age_ms",
    "mean_fill_latency_ms",
    "max_fill_latency_ms",
) + tuple(
    field
    for action in range(N_ACTIONS)
    for field in (f"action_count_{action}", f"action_fraction_{action}")
)


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
        min_order_lifetime_ns: int = 5_000_000_000,
        max_order_lifetime_ns: int = 15_000_000_000,
        reprice_threshold_ticks: float = 1.0,
        asset_no: int = 0,
    ):
        if step_ns <= 0 or max_position <= 0 or order_qty <= 0:
            raise ValueError("step_ns, max_position and order_qty must be positive")
        if min_order_lifetime_ns < 0:
            raise ValueError("min_order_lifetime_ns must be non-negative")
        if max_order_lifetime_ns <= 0:
            raise ValueError("max_order_lifetime_ns must be positive")
        if min_order_lifetime_ns > max_order_lifetime_ns:
            raise ValueError(
                "min_order_lifetime_ns cannot exceed max_order_lifetime_ns"
            )
        if reprice_threshold_ticks < 0:
            raise ValueError("reprice_threshold_ticks must be non-negative")
        self.step_ns = int(step_ns)
        self.max_position = float(max_position)
        self.order_qty = float(order_qty)
        self.report_notional = float(report_notional)
        self.min_order_lifetime_ns = int(min_order_lifetime_ns)
        self.max_order_lifetime_ns = int(max_order_lifetime_ns)
        self.reprice_threshold_ticks = float(reprice_threshold_ticks)
        self.asset_no = int(asset_no)
        self._lags = tuple(
            max(1, int(round(minutes * 60 * 1_000_000_000 / self.step_ns)))
            for minutes in (1, 5, 15)
        )
        self._one_minute_steps = self._lags[0]
        self._five_minute_steps = self._lags[1]
        self._ten_second_steps = max(
            1, int(round(10 * 1_000_000_000 / self.step_ns))
        )
        self._sixty_second_steps = max(
            1, int(round(60 * 1_000_000_000 / self.step_ns))
        )
        self._short_return_lags = tuple(
            max(1, int(round(seconds * 1_000_000_000 / self.step_ns)))
            for seconds in (1, 10, 30)
        )
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
        self._action_counts = np.zeros(N_ACTIONS, dtype=np.int64)
        self._invalid_action_count = 0
        self._submit_attempts = 0
        self._submit_successes = 0
        self._buy_submit_successes = 0
        self._sell_submit_successes = 0
        self._cancel_attempts = 0
        self._cancel_successes = 0
        self._signal_cancel_successes = 0
        self._reprice_cancel_successes = 0
        self._expiry_cancel_successes = 0
        self._fill_events = 0.0
        self._fill_qty = 0.0
        self._peak_active_orders = 0
        self._cancelled_order_age_ms_sum = 0.0
        self._cancelled_order_age_samples = 0
        self._cancelled_order_age_ms_max = 0.0
        self._fill_latency_ms_sum = 0.0
        self._fill_latency_samples = 0
        self._fill_latency_ms_max = 0.0
        self._order_submitted_at_ns: dict[int, int] = {}
        self._last_submission_at_ns = {BUY: None, SELL: None}
        self.target_position = 0.0
        self._trade_buy_history: list[float] = []
        self._trade_sell_history: list[float] = []
        self._book_ofi_history: list[float] = []
        self._previous_best_book: Optional[tuple[float, float, float, float]] = None

    def diagnostics(self) -> dict[str, float]:
        """Return JSON-safe counters for policy-degeneration analysis."""
        decisions = int(self._action_counts.sum())
        result = {
            "decision_count": float(decisions),
            "invalid_action_count": float(self._invalid_action_count),
            "submit_attempts": float(self._submit_attempts),
            "submit_successes": float(self._submit_successes),
            "buy_submit_successes": float(self._buy_submit_successes),
            "sell_submit_successes": float(self._sell_submit_successes),
            "cancel_attempts": float(self._cancel_attempts),
            "cancel_successes": float(self._cancel_successes),
            "signal_cancel_successes": float(
                self._signal_cancel_successes
            ),
            "reprice_cancel_successes": float(
                self._reprice_cancel_successes
            ),
            "expiry_cancel_successes": float(
                self._expiry_cancel_successes
            ),
            "fill_events": float(self._fill_events),
            "fill_qty": float(self._fill_qty),
            "peak_active_orders": float(self._peak_active_orders),
            "mean_cancelled_order_age_ms": (
                self._cancelled_order_age_ms_sum
                / self._cancelled_order_age_samples
                if self._cancelled_order_age_samples
                else 0.0
            ),
            "max_cancelled_order_age_ms": float(
                self._cancelled_order_age_ms_max
            ),
            "mean_fill_latency_ms": (
                self._fill_latency_ms_sum / self._fill_latency_samples
                if self._fill_latency_samples
                else 0.0
            ),
            "max_fill_latency_ms": float(self._fill_latency_ms_max),
        }
        for action, count in enumerate(self._action_counts):
            result[f"action_count_{action}"] = float(count)
            result[f"action_fraction_{action}"] = (
                float(count / decisions) if decisions else 0.0
            )
        return result

    def _record_fill_latency(self, now: int, delta_position: float) -> None:
        if delta_position > 0:
            submitted_at = self._last_submission_at_ns[BUY]
        elif delta_position < 0:
            submitted_at = self._last_submission_at_ns[SELL]
        else:
            submitted_at = None
        if submitted_at is None:
            return
        latency_ms = max(0.0, (now - submitted_at) / 1_000_000)
        self._fill_latency_ms_sum += latency_ms
        self._fill_latency_samples += 1
        self._fill_latency_ms_max = max(
            self._fill_latency_ms_max, latency_ms
        )

    def _active_orders(self, hbt) -> list[dict]:
        now = int(hbt.current_timestamp)
        active = []
        active_ids = set()
        values = hbt.orders(self.asset_no).values()
        while values.has_next():
            order = values.get()
            order_id = int(order.order_id)
            active_ids.add(order_id)
            submitted_at = self._order_submitted_at_ns.get(order_id, now)
            active.append({
                "order": order,
                "order_id": order_id,
                "side": int(order.side),
                "leaves_qty": float(order.leaves_qty),
                "price": float(getattr(order, "price", np.nan)),
                "age_ns": max(0, now - submitted_at),
            })
        self._peak_active_orders = max(
            self._peak_active_orders, len(active)
        )
        for order_id in set(self._order_submitted_at_ns) - active_ids:
            self._order_submitted_at_ns.pop(order_id, None)
        return active

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

    def _trade_flow(self, hbt, qty_scale: float) -> tuple[float, ...]:
        trades = hbt.last_trades(self.asset_no)
        if trades is None or len(trades) == 0:
            buy_qty = 0.0
            sell_qty = 0.0
        else:
            events = trades["ev"]
            quantities = np.maximum(
                trades["qty"].astype(np.float64), 0.0
            )
            buy_qty = float(
                quantities[(events & BUY_EVENT) != 0].sum()
            )
            sell_qty = float(
                quantities[(events & SELL_EVENT) != 0].sum()
            )
        hbt.clear_last_trades(self.asset_no)
        self._trade_buy_history.append(buy_qty)
        self._trade_sell_history.append(sell_qty)
        if len(self._trade_buy_history) > self._sixty_second_steps:
            del self._trade_buy_history[:-self._sixty_second_steps]
            del self._trade_sell_history[:-self._sixty_second_steps]

        def aggregate(steps: int) -> tuple[float, float]:
            buys = sum(self._trade_buy_history[-steps:])
            sells = sum(self._trade_sell_history[-steps:])
            total = buys + sells
            imbalance = (buys - sells) / (total + 1e-12)
            scaled_intensity = total / (
                qty_scale * min(steps, len(self._trade_buy_history))
                + 1e-12
            )
            intensity = np.log1p(scaled_intensity)
            return (
                float(np.clip(imbalance, -1.0, 1.0)),
                float(np.clip(intensity, 0.0, 10.0)),
            )

        step_imbalance, step_intensity = aggregate(1)
        ten_imbalance, ten_intensity = aggregate(
            self._ten_second_steps
        )
        sixty_imbalance, sixty_intensity = aggregate(
            self._sixty_second_steps
        )
        return (
            step_imbalance,
            step_intensity,
            ten_imbalance,
            ten_intensity,
            sixty_imbalance,
            sixty_intensity,
        )

    def _book_flow(self, depth) -> tuple[float, float, float]:
        current = (
            float(depth.best_bid),
            float(depth.best_ask),
            max(0.0, float(depth.best_bid_qty)),
            max(0.0, float(depth.best_ask_qty)),
        )
        previous = self._previous_best_book
        self._previous_best_book = current
        if previous is None:
            step_ofi = 0.0
        else:
            bid, ask, bid_qty, ask_qty = current
            prev_bid, prev_ask, prev_bid_qty, prev_ask_qty = previous
            if bid > prev_bid:
                bid_flow = bid_qty
            elif bid == prev_bid:
                bid_flow = bid_qty - prev_bid_qty
            else:
                bid_flow = -prev_bid_qty
            if ask < prev_ask:
                ask_flow = ask_qty
            elif ask == prev_ask:
                ask_flow = ask_qty - prev_ask_qty
            else:
                ask_flow = -prev_ask_qty
            step_ofi = (bid_flow - ask_flow) / (
                bid_qty + ask_qty + prev_bid_qty + prev_ask_qty + 1e-12
            )
        step_ofi = float(np.clip(step_ofi, -5.0, 5.0))
        self._book_ofi_history.append(step_ofi)
        if len(self._book_ofi_history) > self._sixty_second_steps:
            del self._book_ofi_history[:-self._sixty_second_steps]
        ten_ofi = float(np.mean(
            self._book_ofi_history[-self._ten_second_steps:]
        ))
        sixty_ofi = float(np.mean(
            self._book_ofi_history[-self._sixty_second_steps:]
        ))
        return step_ofi, ten_ofi, sixty_ofi

    def _short_returns_bps(self, mid: float) -> tuple[float, ...]:
        clips = (20.0, 50.0, 100.0)
        values = []
        for lag, clip_value in zip(self._short_return_lags, clips):
            if mid <= 0 or len(self.mid_history) < lag:
                values.append(0.0)
                continue
            previous = self.mid_history[-lag]
            if previous <= 0:
                values.append(0.0)
                continue
            return_bps = (mid / previous - 1.0) * 10_000
            values.append(float(np.clip(
                return_bps, -clip_value, clip_value
            )))
        return tuple(values)

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
            self._fill_events += num_trades - self.prev_num_trades
            self._fill_qty += traded_qty
            self._record_fill_latency(now, delta_position)
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

        active_orders = self._active_orders(hbt)
        pending_buy = sum(
            order["leaves_qty"]
            for order in active_orders
            if order["side"] == BUY
        )
        pending_sell = sum(
            order["leaves_qty"]
            for order in active_orders
            if order["side"] == SELL
        )
        obs[25] = float(np.clip(
            pending_buy / self.max_position, 0.0, 1.0
        ))
        obs[26] = float(np.clip(
            pending_sell / self.max_position, 0.0, 1.0
        ))
        oldest_age_ns = max(
            (order["age_ns"] for order in active_orders),
            default=0,
        )
        obs[27] = float(np.clip(
            oldest_age_ns / self.max_order_lifetime_ns, 0.0, 1.0
        ))
        obs[28] = float(np.clip(
            self.target_position / self.max_position, -1.0, 1.0
        ))
        tick_size = max(float(depth.tick_size), 1e-12)
        if depth.best_ask > 0 and depth.best_bid > 0:
            obs[29] = float(np.clip(
                (depth.best_ask - depth.best_bid) / tick_size,
                0.0,
                20.0,
            ))
        depth_qty = bbq + baq
        if depth_qty > 0 and mid > 0:
            microprice = (
                float(depth.best_ask) * bbq
                + float(depth.best_bid) * baq
            ) / depth_qty
            obs[30] = float(np.clip(
                (microprice - mid) / tick_size, -5.0, 5.0
            ))
        obs[31:37] = self._trade_flow(hbt, qty_scale)
        obs[37:40] = self._book_flow(depth)
        obs[40:43] = self._short_returns_bps(mid)

        if mid > 0:
            self.mid_history.append(mid)
            if len(self.mid_history) > self._max_history:
                del self.mid_history[:-self._max_history]
        self.step_idx += 1
        return StepSnapshot(obs, equity, mid, traded_qty, num_trades)

    def _cancel_order(self, hbt, order: dict, reason: str) -> None:
        now = int(hbt.current_timestamp)
        raw_order = order["order"]
        if not raw_order.cancellable:
            return
        self._cancel_attempts += 1
        cancel_rc = int(hbt.cancel(
            self.asset_no, raw_order.order_id, False
        ))
        if cancel_rc != 0:
            raise RuntimeError(
                f"cancel order {raw_order.order_id} failed "
                f"with code {cancel_rc}"
            )
        self._cancel_successes += 1
        if reason == "signal":
            self._signal_cancel_successes += 1
        elif reason == "reprice":
            self._reprice_cancel_successes += 1
        elif reason == "expiry":
            self._expiry_cancel_successes += 1
        submitted_at = self._order_submitted_at_ns.pop(
            int(raw_order.order_id), None
        )
        if submitted_at is not None:
            age_ms = max(0.0, (now - submitted_at) / 1_000_000)
            self._cancelled_order_age_ms_sum += age_ms
            self._cancelled_order_age_samples += 1
            self._cancelled_order_age_ms_max = max(
                self._cancelled_order_age_ms_max, age_ms
            )

    def apply_action(self, hbt, action: int) -> int:
        action = int(action)
        if action not in ACTION_TARGET_FRACTIONS:
            self._invalid_action_count += 1
            action = 2
        self._action_counts[action] += 1
        target_fraction = ACTION_TARGET_FRACTIONS[action]
        self.last_action_signed = float(np.sign(target_fraction))

        hbt.clear_inactive_orders(self.asset_no)
        depth = hbt.depth(self.asset_no)
        position = float(hbt.state_values(self.asset_no).position)
        lot_size = max(float(depth.lot_size), 1e-12)
        target = target_fraction * self.max_position
        target = round(target / lot_size) * lot_size
        self.target_position = float(np.clip(
            target, -self.max_position, self.max_position
        ))
        position_gap = self.target_position - position
        desired_side = (
            BUY if position_gap > lot_size / 2
            else SELL if position_gap < -lot_size / 2
            else None
        )

        active_orders = self._active_orders(hbt)
        if active_orders:
            best_price = (
                float(depth.best_bid)
                if desired_side == BUY
                else float(depth.best_ask)
            )
            tick_size = max(float(depth.tick_size), 1e-12)
            for order in active_orders:
                if desired_side is None or order["side"] != desired_side:
                    self._cancel_order(hbt, order, "signal")
                    continue
                age_ns = order["age_ns"]
                if age_ns >= self.max_order_lifetime_ns:
                    self._cancel_order(hbt, order, "expiry")
                    continue
                price_distance_ticks = (
                    abs(order["price"] - best_price) / tick_size
                    if np.isfinite(order["price"])
                    and np.isfinite(best_price)
                    else float("inf")
                )
                if (
                    age_ns >= self.min_order_lifetime_ns
                    and price_distance_ticks
                    >= self.reprice_threshold_ticks
                ):
                    self._cancel_order(hbt, order, "reprice")
            # Wait for asynchronous cancels/fills before submitting another
            # child order. This prevents overlapping exposure.
            return 0

        if desired_side is None:
            return 0

        requested = min(abs(position_gap), self.order_qty)
        order_id = self._next_order_id
        self._next_order_id += 1

        if desired_side == BUY:
            room = self.max_position - position
            qty = min(requested, max(0.0, room))
            price = float(depth.best_bid)
            if qty <= 0 or not np.isfinite(price) or price <= 0:
                return 0
            self._submit_attempts += 1
            submit_rc = int(hbt.submit_buy_order(
                self.asset_no, order_id, price, qty, GTX, LIMIT, False
            ))
            if submit_rc == 0:
                now = int(hbt.current_timestamp)
                self._submit_successes += 1
                self._buy_submit_successes += 1
                self._order_submitted_at_ns[order_id] = now
                self._last_submission_at_ns[BUY] = now
            return submit_rc

        room = self.max_position + position
        qty = min(requested, max(0.0, room))
        price = float(depth.best_ask)
        if qty <= 0 or not np.isfinite(price) or price <= 0:
            return 0
        self._submit_attempts += 1
        submit_rc = int(hbt.submit_sell_order(
            self.asset_no, order_id, price, qty, GTX, LIMIT, False
        ))
        if submit_rc == 0:
            now = int(hbt.current_timestamp)
            self._submit_successes += 1
            self._sell_submit_successes += 1
            self._order_submitted_at_ns[order_id] = now
            self._last_submission_at_ns[SELL] = now
        return submit_rc
