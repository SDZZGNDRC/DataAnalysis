# -*- coding: utf-8 -*-
"""
OKX 深度 + 成交数据 → hftbacktest 做市 Demo
author : you
python : 3.9+  , hftbacktest>=2.0
run    : python okx_mm_demo.py
"""
import numpy as np
from numba import njit
from hftbacktest import HashMapMarketDepthBacktest, BacktestAsset, Recorder, GTC, LIMIT, BUY, SELL
from hftbacktest.stats import LinearAssetRecord

# ---------- 参数区 ----------
TICK_SIZE   = 0.1        # 价格最小跳动（例：BTC-USDT 0.1）
LOT_SIZE    = 0.00001       # 最小下单量
MIN_SPREAD  = 2          # 最小 spread (tick 数)
GAMMA       = 0.2        # imbalance 放大系数
INV_PENALTY = 0.5        # 库存惩罚
MAX_POS     = 1.0        # 最大净持仓（绝对值）
Q_NOTIONAL  = 100.       # 每边挂单名义价值（USDT）
REFRESH_US  = 100_000_000    # 100 ms (us)
# ----------------------------

@njit
def okx_mm_demo(hbt, recorder):
    asset = 0
    tick  = TICK_SIZE
    lot   = LOT_SIZE
    while hbt.elapse(REFRESH_US) == 0:
        hbt.clear_inactive_orders(asset)
        depth = hbt.depth(asset)
        pos   = hbt.position(asset)

        # 1. 计算 imbalance
        bb_qty = depth.best_bid_qty
        ba_qty = depth.best_ask_qty
        imb = (bb_qty - ba_qty) / (bb_qty + ba_qty + 1e-6)

        # 2. fair price + inventory skew
        mid = (depth.best_bid + depth.best_ask) / 2.0
        fair_price = mid + GAMMA * imb * tick
        reservation = fair_price - INV_PENALTY * pos * tick

        # 3. 目标报价
        half_spread = MIN_SPREAD * tick / 2.0
        target_bid  = reservation - half_spread
        target_ask  = reservation + half_spread
        # 对齐到 tick
        target_bid_tick = int(np.round(target_bid / tick))
        target_ask_tick = int(np.round(target_ask / tick))

        # 4. 下单量（按名义价值折算）
        mid_price = mid
        order_qty = int(np.round(Q_NOTIONAL / mid_price / lot)) * lot
        # 仓位保护
        buy_ok  = pos * mid_price <  MAX_POS * Q_NOTIONAL
        sell_ok = pos * mid_price > -MAX_POS * Q_NOTIONAL

        # 5. 撤旧单
        orders = hbt.orders(asset)
        values = orders.values()
        while True:
            o = values.next()
            if o is None:
                break
            if o.side == BUY and (o.price_tick != target_bid_tick or not buy_ok):
                hbt.cancel(asset, o.order_id, False)
            if o.side == SELL and (o.price_tick != target_ask_tick or not sell_ok):
                hbt.cancel(asset, o.order_id, False)

        # 6. 挂新单
        if buy_ok:
            hbt.submit_buy_order(asset, target_bid_tick,
                                 target_bid_tick * tick, order_qty,
                                 GTC, LIMIT, False)
        if sell_ok:
            hbt.submit_sell_order(asset, target_ask_tick,
                                  target_ask_tick * tick, order_qty,
                                  GTC, LIMIT, False)

        # 7. 记录状态
        recorder.record(hbt)

        # 8. 等待一次事件响应（防止空转）
        hbt.wait_next_feed(False, 5_000_000)   # 5 ms 超时，不包含订单响应
    return True

# ---------- 回测入口 ----------
if __name__ == '__main__':
    # 1. 准备数据：加载 HFT 数据
    # 注意：这里需要替换为实际的数据文件路径
    hft_data = np.load(r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-02.npz")['data']

    # 2. 配置资产
    asset = (
        BacktestAsset()
            .data([hft_data])
            .linear_asset(1.0)
            .constant_order_latency(200_000, 200_000)  # 200 us 报单延迟
            .risk_adverse_queue_model()
            .no_partial_fill_exchange()
            .trading_value_fee_model(0.0008, 0.001)  # maker_fee, taker_fee
            .tick_size(TICK_SIZE)
            .lot_size(LOT_SIZE)
            .last_trades_capacity(1000000)
    )

    # 3. 创建记录器
    recorder = Recorder(num_assets=1, record_size=1000000)

    # 4. 启动回测
    hbt = HashMapMarketDepthBacktest([asset])
    okx_mm_demo(hbt, recorder.recorder)
    
    # 5. 保存回测记录
    recorder.to_npz('backtest_result.npz')
    
    # 6. 显示回测结果
    print("回测完成")
    print("=" * 50)
    print("回测结果统计:")
    print("=" * 50)
    
    # 加载回测记录并生成统计
    asset0_record = recorder.get(0)
    stats = (
        LinearAssetRecord(asset0_record)
            .resample('10s')
            .monthly()
            .stats(book_size=Q_NOTIONAL)
    )
    
    # 显示统计摘要
    summary_df = stats.summary()
    print(summary_df)
    
    # 显示关键指标
    print("\n关键指标:")
    print("-" * 30)
    
    # 计算权益（balance + position * price）
    final_balance = asset0_record['balance'][-1]
    final_position = asset0_record['position'][-1]
    final_price = asset0_record['price'][-1]
    final_equity = final_balance + final_position * final_price
    
    initial_equity = Q_NOTIONAL
    total_return = (final_equity - initial_equity) / initial_equity * 100
    total_fee = asset0_record['fee'][-1]
    total_trades = asset0_record['num_trades'][-1]
    
    print(f"初始资金: {initial_equity:.2f} USDT")
    print(f"最终资金: {final_equity:.2f} USDT")
    print(f"总收益率: {total_return:.2f}%")
    print(f"总手续费: {total_fee:.2f} USDT")
    print(f"总交易次数: {total_trades}")
    print(f"最终持仓: {final_position:.6f}")
    print(f"最终余额: {final_balance:.2f} USDT")
    
    # 绘制图表
    print("\n正在生成图表...")
    stats.plot()