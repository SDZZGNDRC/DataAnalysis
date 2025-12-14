"""Order Flow Imbalance Strategy for hftbacktest.

This strategy triggers trades based on significant order flow imbalance and uses
fixed percentage take-profit/stop-loss for exits, with transaction fees.

The strategy monitors trade data for buy/sell volume imbalance and enters positions
when the imbalance exceeds a threshold, then exits based on TP/SL conditions.
"""
import numpy as np
from numba import njit

from hftbacktest import BUY, SELL, GTC, LIMIT


@njit
def order_flow_imbalance_strategy(
    hbt,
    step_ns=50_000_000,
    volume_threshold=100.0,
    take_profit_pct=0.002,
    stop_loss_pct=0.001,
    fee_rate=0.0008,
    max_position_lots=10.0,
    order_qty_lots=1.0,
):
    """
    Execute an order flow imbalance strategy inside an hftbacktest loop.

    Args:
        hbt: Compiled HashMapMarketDepthBacktest instance.
        step_ns: Simulation step size in nanoseconds.
        volume_threshold: The net volume difference required to trigger a trade.
        take_profit_pct: The percentage gain at which to close a position (gross profit).
        stop_loss_pct: The percentage loss at which to close a position (gross loss).
        fee_rate: The transaction fee rate for a single trade.
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

    # Strategy state
    position = 'flat'  # 'flat', 'long', or 'short'
    entry_price = 0.0
    entry_time = 0
    exit_code = 0

    while True:
        exit_code = hbt.elapse(step_ns)
        if exit_code != 0:
            break

        depth = hbt.depth(asset_no)
        state = hbt.state_values(asset_no)
        current_position = state.position
        current_time = hbt.current_timestamp

        # Convert position state from numeric to string
        if current_position > 0:
            position = 'long'
        elif current_position < 0:
            position = 'short'
        else:
            position = 'flat'

        # 1. Check for exit conditions if we are currently in a position
        if position != 'flat':
            mid_price = (depth.best_bid + depth.best_ask) / 2.0
            if mid_price <= 0.0:
                continue

            # Calculate gross PnL percentage
            if position == 'long':
                gross_pnl_pct = (mid_price / entry_price - 1)
            else:  # short
                gross_pnl_pct = (entry_price / mid_price - 1)

            # Check for Take Profit or Stop Loss
            if gross_pnl_pct >= take_profit_pct or gross_pnl_pct <= -stop_loss_pct:
                exit_reason = 'TP' if gross_pnl_pct > 0 else 'SL'
                
                # Calculate net PnL with fees
                if position == 'long':
                    effective_entry = entry_price * (1 + fee_rate)
                    effective_exit = mid_price * (1 - fee_rate)
                    pnl = effective_exit - effective_entry
                else:  # short
                    effective_entry = entry_price * (1 - fee_rate)
                    effective_exit = mid_price * (1 + fee_rate)
                    pnl = effective_entry - effective_exit
                
                # Close position by placing market order
                if position == 'long':
                    # Sell to close long position
                    hbt.submit_sell_order(
                        asset_no,
                        2,  # order_id
                        depth.best_bid,  # market sell at best bid
                        abs(current_position),
                        GTC,
                        LIMIT,
                        False,
                    )
                else:  # short
                    # Buy to close short position
                    hbt.submit_buy_order(
                        asset_no,
                        1,  # order_id
                        depth.best_ask,  # market buy at best ask
                        abs(current_position),
                        GTC,
                        LIMIT,
                        False,
                    )
                
                # Reset state
                position = 'flat'
                entry_price = 0.0
                entry_time = 0

        # 2. If we are flat, check for new entry signals
        if position == 'flat':
            # Get recent trades - hbt.last_trades returns a numpy array
            trades = hbt.last_trades(asset_no)
            if trades is None or len(trades) == 0:
                continue

            # Calculate buy and sell volume from recent trades
            buy_volume = 0.0
            sell_volume = 0.0
            
            # Iterate through trades array
            for i in range(len(trades)):
                trade = trades[i]
                # Check if trade is buy (side == 1) or sell (side == -1)
                if trade[2] == 1:  # BUY side
                    buy_volume += trade[1]  # qty
                elif trade[2] == -1:  # SELL side
                    sell_volume += trade[1]  # qty

            net_buy_volume = buy_volume - sell_volume

            # Check position limits
            allow_buy = current_position < max_position
            allow_sell = current_position > -max_position

            # Entry Logic
            if net_buy_volume > volume_threshold and buy_volume > 0 and allow_buy:
                # Strong buy signal - enter long position
                entry_price = depth.best_ask  # Market buy at best ask
                hbt.submit_buy_order(
                    asset_no,
                    1,  # order_id
                    entry_price,
                    order_qty,
                    GTC,
                    LIMIT,
                    False,
                )
                position = 'long'
                entry_time = current_time

            elif -net_buy_volume > volume_threshold and sell_volume > 0 and allow_sell:
                # Strong sell signal - enter short position
                entry_price = depth.best_bid  # Market sell at best bid
                hbt.submit_sell_order(
                    asset_no,
                    2,  # order_id
                    entry_price,
                    order_qty,
                    GTC,
                    LIMIT,
                    False,
                )
                position = 'short'
                entry_time = current_time

        # Clear any inactive orders
        hbt.clear_inactive_orders(asset_no)

    # Clean up any remaining orders at the end
    hbt.clear_inactive_orders(asset_no)
    return exit_code


def is_success(exit_code):
    """Return True when the strategy finished because the data stream ended."""
    return exit_code in (1, 2, 15)