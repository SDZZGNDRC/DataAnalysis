"""Simple mean reversion strategy demo built on top of hftbacktest.

The strategy keeps track of an exponentially weighted moving average (EMA) of
the mid price and places passive orders whenever the live mid price deviates
from the EMA by a configurable number of ticks.
"""
import numpy as np
from numba import njit

from hftbacktest import BUY, SELL, GTX, LIMIT


BUY_ORDER_ID = 1
SELL_ORDER_ID = 2


@njit
def _resolve_mid(best_bid, best_ask):
    """Return a mid price even when only one side of the book is available."""
    if np.isfinite(best_bid) and np.isfinite(best_ask):
        return 0.5 * (best_bid + best_ask)
    if np.isfinite(best_bid):
        return best_bid
    if np.isfinite(best_ask):
        return best_ask
    return 0.0


@njit
def mean_reversion_strategy(
    hbt,
    step_ns=50_000_000,
    ema_alpha=0.2,
    threshold_ticks=1.5,
    max_position_lots=10.0,
    order_qty_lots=2.0,
):
    """
    Execute a simple mean reversion strategy inside an hftbacktest loop.

    Args:
        hbt: Compiled HashMapMarketDepthBacktest instance.
        step_ns: Simulation step size in nanoseconds.
        ema_alpha: EMA smoothing factor used for the reference mid price.
        threshold_ticks: Signal threshold expressed in ticks.
        max_position_lots: Maximum absolute inventory in multiples of lot_size.
        order_qty_lots: Order quantity in multiples of lot_size.

    Returns:
        The exit code returned by the last hbt.elapse call.
    """
    asset_no = 0
    depth = hbt.depth(asset_no)
    tick_size = depth.tick_size
    lot_size = depth.lot_size

    if tick_size <= 0.0 or lot_size <= 0.0:
        return -1

    order_qty = order_qty_lots * lot_size
    if order_qty <= 0.0:
        order_qty = lot_size

    max_position = max_position_lots * lot_size
    if max_position <= lot_size:
        max_position = lot_size

    ema_mid = 0.0
    initialized = False
    exit_code = 0

    while True:
        exit_code = hbt.elapse(step_ns)
        if exit_code != 0:
            break

        depth = hbt.depth(asset_no)
        mid_price = _resolve_mid(depth.best_bid, depth.best_ask)
        if not np.isfinite(mid_price) or mid_price <= 0.0:
            hbt.clear_inactive_orders(asset_no)
            continue

        if not initialized:
            ema_mid = mid_price
            initialized = True
        else:
            ema_mid = ema_mid + ema_alpha * (mid_price - ema_mid)

        hbt.clear_inactive_orders(asset_no)

        state = hbt.state_values(asset_no)
        position = state.position

        deviation_ticks = (mid_price - ema_mid) / tick_size
        buy_signal = deviation_ticks <= -threshold_ticks
        sell_signal = deviation_ticks >= threshold_ticks

        allow_buy = position < max_position
        allow_sell = position > -max_position

        orders = hbt.orders(asset_no)
        values = orders.values()
        has_buy_order = False
        has_sell_order = False

        bid_tick = depth.best_bid_tick
        ask_tick = depth.best_ask_tick

        while values.has_next():
            order = values.get()
            if order.side == BUY:
                if buy_signal and allow_buy and order.price_tick == bid_tick:
                    has_buy_order = True
                elif order.cancellable:
                    hbt.cancel(asset_no, order.order_id, False)
            elif order.side == SELL:
                if sell_signal and allow_sell and order.price_tick == ask_tick:
                    has_sell_order = True
                elif order.cancellable:
                    hbt.cancel(asset_no, order.order_id, False)

        if buy_signal and allow_buy and bid_tick >= 0 and not has_buy_order:
            buy_price = bid_tick * tick_size
            if np.isfinite(buy_price):
                hbt.submit_buy_order(
                    asset_no,
                    BUY_ORDER_ID,
                    buy_price,
                    order_qty,
                    GTX,
                    LIMIT,
                    False,
                )

        if sell_signal and allow_sell and ask_tick >= 0 and not has_sell_order:
            sell_price = ask_tick * tick_size
            if np.isfinite(sell_price):
                hbt.submit_sell_order(
                    asset_no,
                    SELL_ORDER_ID,
                    sell_price,
                    order_qty,
                    GTX,
                    LIMIT,
                    False,
                )

    orders = hbt.orders(asset_no)
    values = orders.values()
    while values.has_next():
        order = values.get()
        if order.cancellable:
            hbt.cancel(asset_no, order.order_id, False)

    return exit_code


def is_success(exit_code):
    """Return True when the strategy finished because the data stream ended."""
    return exit_code in (1, 2, 15)