"""Command-line backtest runner for the order flow imbalance strategy.

Example
-------
python backtests/run_order_flow_imbalance.py ^
    --data OKX-HFT-BTC-USDT-2025-03-02.npz ^
    --volume-threshold 100.0 ^
    --take-profit-pct 0.002 ^
    --stop-loss-pct 0.001
"""
import argparse
import math
import sys
from pathlib import Path
from typing import List

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest
from strategies.order_flow_imbalance import (
    order_flow_imbalance_strategy,
    is_success,
)


def _resolve_mid(best_bid: float, best_ask: float) -> float:
    if math.isfinite(best_bid) and math.isfinite(best_ask):
        return 0.5 * (best_bid + best_ask)
    if math.isfinite(best_bid):
        return best_bid
    if math.isfinite(best_ask):
        return best_ask
    return 0.0


def load_hft_arrays(files: List[Path]) -> List[np.ndarray]:
    arrays: List[np.ndarray] = []
    for path in files:
        with np.load(path) as data:
            if "data" not in data:
                raise KeyError(f"{path} 缺少 'data' 键")
            arrays.append(data["data"])
    if not arrays:
        raise ValueError("未提供任何可用的数据文件")
    return arrays


def build_asset(data_arrays: List[np.ndarray], tick_size: float, lot_size: float) -> BacktestAsset:
    return (
        BacktestAsset()
        .data(data_arrays)
        .linear_asset(1.0)
        .constant_order_latency(10_000_000, 10_000_000)
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
        .trading_value_fee_model(0.0008, 0.0010)
        .tick_size(tick_size)
        .lot_size(lot_size)
        .last_trades_capacity(1_000_000)
    )


def summarize_result(hbt: HashMapMarketDepthBacktest) -> dict:
    asset_no = 0
    depth = hbt.depth(asset_no)
    state = hbt.state_values(asset_no)

    mid_price = _resolve_mid(depth.best_bid, depth.best_ask)
    balance = state.balance
    position = state.position
    fee = state.fee
    equity = balance + position * mid_price - fee

    return {
        "current_timestamp": hbt.current_timestamp,
        "mid_price": mid_price,
        "balance": balance,
        "position": position,
        "fee": fee,
        "equity": equity,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="运行订单流不平衡策略回测")
    parser.add_argument(
        "--data",
        nargs="+",
        type=Path,
        required=True,
        help="HFT 事件 npz 文件路径列表（必须包含 'data' 数组）",
    )
    parser.add_argument("--tick-size", type=float, default=0.1, help="合约最小价格跳动")
    parser.add_argument("--lot-size", type=float, default=0.001, help="合约最小下单数量")
    parser.add_argument("--step-ns", type=int, default=50_000_000, help="策略循环的时间步长（纳秒）")
    parser.add_argument("--volume-threshold", type=float, default=100.0, 
                       help="触发交易的净成交量阈值")
    parser.add_argument("--take-profit-pct", type=float, default=0.002, 
                       help="止盈百分比（例如 0.002 表示 0.2%）")
    parser.add_argument("--stop-loss-pct", type=float, default=0.001, 
                       help="止损百分比（例如 0.001 表示 0.1%）")
    parser.add_argument("--fee-rate", type=float, default=0.0008, 
                       help="交易手续费率（例如 0.0008 表示 0.08%）")
    parser.add_argument("--max-position-lots", type=float, default=10.0, 
                       help="最大仓位（以 lot_size 倍数计）")
    parser.add_argument("--order-qty-lots", type=float, default=1.0, 
                       help="单笔委托数量（以 lot_size 倍数计）")
    return parser.parse_args()


def main():
    args = parse_args()

    data_files = [path.resolve(strict=True) for path in args.data]
    data_arrays = load_hft_arrays(data_files)

    asset = build_asset(
        data_arrays=data_arrays,
        tick_size=args.tick_size,
        lot_size=args.lot_size,
    )
    hbt = HashMapMarketDepthBacktest([asset])

    exit_code = order_flow_imbalance_strategy(
        hbt,
        step_ns=args.step_ns,
        volume_threshold=args.volume_threshold,
        take_profit_pct=args.take_profit_pct,
        stop_loss_pct=args.stop_loss_pct,
        fee_rate=args.fee_rate,
        max_position_lots=args.max_position_lots,
        order_qty_lots=args.order_qty_lots,
    )

    summary = summarize_result(hbt)
    status = "success" if is_success(exit_code) else f"exit_code={exit_code}"

    print("=== Order Flow Imbalance Strategy Backtest Summary ===")
    print(f"status              : {status}")
    print(f"current_timestamp   : {summary['current_timestamp']}")
    print(f"mid_price           : {summary['mid_price']:.4f}")
    print(f"balance             : {summary['balance']:.6f}")
    print(f"position            : {summary['position']:.6f}")
    print(f"fee                 : {summary['fee']:.6f}")
    print(f"equity              : {summary['equity']:.6f}")
    print("=====================================================")

    # 显示策略参数
    print("\n策略参数:")
    print("-" * 30)
    print(f"成交量阈值         : {args.volume_threshold}")
    print(f"止盈百分比         : {args.take_profit_pct:.4f} ({args.take_profit_pct*100:.2f}%)")
    print(f"止损百分比         : {args.stop_loss_pct:.4f} ({args.stop_loss_pct*100:.2f}%)")
    print(f"手续费率           : {args.fee_rate:.4f} ({args.fee_rate*100:.2f}%)")
    print(f"最大仓位           : {args.max_position_lots} lots")
    print(f"单笔委托数量       : {args.order_qty_lots} lots")


if __name__ == "__main__":
    main()