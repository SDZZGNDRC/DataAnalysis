"""Queue-imbalance market-making strategy for hftbacktest (Phase 4).

L1 queue imbalance (best bid/ask qty) adjusts fair price; Avellaneda-style
symmetric spread with inventory skew; passive GTX quotes on both sides; periodic
refresh + cancel-replace. Records state via recorder so LinearAssetRecord can
produce SR/Sortino/MDD.

This is the project's fourth (newly developed) strategy, distinct from the three
existing ones (mean reversion / order flow imbalance / rejection) which use only
price / last_trades signals and trade one side at a time.
"""
import numpy as np
from numba import njit

from hftbacktest import BUY, SELL, GTC, GTX, LIMIT

BUY_ORDER_ID = 1
SELL_ORDER_ID = 2


@njit
def queue_imbalance_mm_strategy(
    hbt,
    recorder,
    step_ns=50_000_000,        # refresh interval (ns). 50ms.
    gamma=0.5,                 # imbalance coefficient (fair = mid + gamma*imb*tick)
    inv_penalty=1.0,           # inventory skew (per unit position, in ticks)
    half_spread_ticks=2.0,     # half spread baseline (ticks)
    max_position_lots=10.0,
    order_qty_lots=1.0,
    skew_floor_ticks=0.5,       # min skew added by imbalance/inv in ticks (avoid zero-edge)
    record_every=1,
):
    """Market-making loop driving hbt + recorder."""
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

    half_spread = half_spread_ticks * tick_size
    floor = skew_floor_ticks * tick_size
    exit_code = 0
    it = 0

    while True:
        exit_code = hbt.elapse(step_ns)
        if exit_code != 0:
            break

        hbt.clear_inactive_orders(asset_no)
        depth = hbt.depth(asset_no)
        state = hbt.state_values(asset_no)
        position = state.position
        bb = depth.best_bid
        ba = depth.best_ask
        if not np.isfinite(bb) or not np.isfinite(ba) or bb <= 0 or ba <= 0:
            continue
        bbq = depth.best_bid_qty
        baq = depth.best_ask_qty
        imb = (bbq - baq) / (bbq + baq + 1e-9)

        mid = 0.5 * (bb + ba)
        # fair price shift from imbalance (positive imbalance -> price up)
        fair = mid + gamma * imb * tick_size
        # reservation price shifted by inventory (long position -> sell lower / buy lower)
        reservation = fair - inv_penalty * position * tick_size / lot_size

        # ensure at least floor + half_spread on each side of mid, plus skew
        # spread is symmetric around reservation
        target_bid = reservation - half_spread - abs(gamma * imb * tick_size)
        target_ask = reservation + half_spread + abs(gamma * imb * tick_size)
        # subtract floor to keep edge positive against fees even when imb~0
        if target_bid > mid:
            target_bid = mid - floor
        if target_ask < mid:
            target_ask = mid + floor
        # keep target_bid below bb (passive buy) and target_ask above ba (passive sell)
        if target_bid >= bb:
            target_bid = bb - (tick_size if bb - tick_size > 0 else 0.0)
        if target_ask <= ba:
            target_ask = ba + tick_size

        target_bid_tick = int(np.round(target_bid / tick_size))
        target_ask_tick = int(np.round(target_ask / tick_size))

        # position limits (notional-style: stay within ±max_position qty)
        buy_ok = position < max_position
        sell_ok = position > -max_position

        # cancel-replace
        orders = hbt.orders(asset_no)
        values = orders.values()
        has_buy = False
        has_sell = False
        while values.has_next():
            o = values.get()
            if o.side == BUY:
                if (not buy_ok) or o.price_tick != target_bid_tick:
                    if o.cancellable:
                        hbt.cancel(asset_no, o.order_id, False)
                else:
                    has_buy = True
            elif o.side == SELL:
                if (not sell_ok) or o.price_tick != target_ask_tick:
                    if o.cancellable:
                        hbt.cancel(asset_no, o.order_id, False)
                else:
                    has_sell = True

        if buy_ok and not has_buy and target_bid_tick > 0:
            hbt.submit_buy_order(asset_no, BUY_ORDER_ID, target_bid_tick * tick_size,
                                 order_qty, GTX, LIMIT, False)
        if sell_ok and not has_sell and target_ask_tick > 0:
            hbt.submit_sell_order(asset_no, SELL_ORDER_ID, target_ask_tick * tick_size,
                                  order_qty, GTX, LIMIT, False)

        it += 1
        if it % record_every == 0:
            recorder.record(hbt)

    # clean up
    hbt.clear_inactive_orders(asset_no)
    return exit_code


def is_success(exit_code):
    return exit_code in (1, 2, 15)