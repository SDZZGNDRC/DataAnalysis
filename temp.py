from numba import njit
import numpy as np

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest, BUY_EVENT

@njit
def print_bbo(hbt):
    # Iterating until hftbacktest reaches the end of data.
    # Elapses 60-sec every iteration.
    # Time unit is the same as data's timestamp's unit.
    # Timestamp of the sample data is in nanoseconds.
    while hbt.elapse(60 * 1e9) == 0:
        # Gets the market depth for the first asset.
        depth = hbt.depth(0)

        # Prints the best bid and the best offer.
        if not np.isnan(np.round(depth.best_bid, 1)):
            print(
                'current_timestamp:', hbt.current_timestamp,
                ', best_bid:', np.round(depth.best_bid, 1),
                ', best_bid_qty:', depth.best_bid_qty,
                ', best_ask:', np.round(depth.best_ask, 1),
                ', best_ask_qty:', depth.best_ask_qty,
            )
    
    return True

@njit
def print_3depth(hbt: HashMapMarketDepthBacktest):
    while hbt.elapse(60 * 1e9) == 0:
        print('current_timestamp:', hbt.current_timestamp)

        # Gets the market depth for the first asset, in the same order as when you created the backtest.
        depth = hbt.depth(0)

        # a key of bid_depth or ask_depth is price in ticks.
        # (integer) price_tick = price / tick_size
        i = 0
        for tick_price in range(depth.best_ask_tick, depth.best_ask_tick + 100):
            qty = depth.ask_qty_at_tick(tick_price)
            if qty > 0:
                print(
                    'ask: ',
                    qty,
                    '@',
                    np.round(tick_price * depth.tick_size, 1)
                )

                i += 1
                if i == 3:
                    break
        i = 0
        for tick_price in range(depth.best_bid_tick, max(depth.best_bid_tick - 100, 0), -1):
            qty = depth.bid_qty_at_tick(tick_price)
            if qty > 0:
                print(
                    'bid: ',
                    qty,
                    '@',
                    np.round(tick_price * depth.tick_size, 1)
                )

                i += 1
                if i == 3:
                    break
    return True

@njit
def plot_bbo(hbt, local_timestamp, best_bid, best_ask):
    while hbt.elapse(1 * 1e9) == 0:
        # Records data points
        local_timestamp.append(hbt.current_timestamp)

        depth = hbt.depth(0)

        best_bid.append(depth.best_bid)
        best_ask.append(depth.best_ask)
    return True

@njit
def print_trades(hbt):
    while hbt.elapse(60 * 1e9) == 0:

        # Gets the last trades occurring in the market, not the trades of our orders.
        last_trades = hbt.last_trades(0)
        
        if len(last_trades) == 0:
            continue
        
        print('-------------------------------------------------------------------------------')
        print('current_timestamp:', hbt.current_timestamp)
        
        num = 0
        for last_trade in last_trades:
            if num > 10:
                print('...')
                break
            print(
                'exch_timestamp:',
                last_trade.exch_ts,
                'buy' if (last_trade.ev & BUY_EVENT) == BUY_EVENT else 'sell',
                last_trade.qty,
                '@',
                last_trade.px
            )
            num += 1

        # To prevent accumulating all last trades, which may cause a slowdown,
        # clear_last_trades needs to be called.
        # After this, accessing `last_trades` will cause a crash.
        hbt.clear_last_trades(0)
    return True

# trades_data = np.load(r"D:\Project\DataAnalysis\OKX-Trades-BTC-USDT-2025-03-02.npz")['data']
hft_data = np.load(r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-02.npz")['data']

# print('trades_data: ', trades_data)
print('hft_data: ', hft_data)

asset = (
    BacktestAsset()
        .data([hft_data])
        .linear_asset(1.0)
        .constant_order_latency(10_000_000, 10_000_000)
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
        .trading_value_fee_model(0.0002, 0.0007)
        .tick_size(0.1)
        .lot_size(0.001)
        .last_trades_capacity(1000000)
)

hbt = HashMapMarketDepthBacktest([asset])

print_bbo(hbt)
# print_trades(hbt)
