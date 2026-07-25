r"""Grid search 子进程：对单 (npz, 参数组合) 运行一次回测，写出 result JSON。

由 backtests\grid_search.py 通过子进程方式调用（规避 SHM 序列化复杂度，每个 worker
独立加载 npz；长 seg 内存由单进程承担，多参数靠进程并行覆盖）。

输出 JSON：
  {"strategy":..,"seg_index":..,"params":{..},"status":"ok"|"fail",
   "metrics":{..},         # 全指标（recorder 类策略来自 LinearAssetRecord.summary）
   "equity":..,"return":..,"fee":..,"num_trades":..,"duration_h":..,
   "buyhold_return":..,      # buy&hold 基准（首事件 mid 持有至末事件 mid）
   "error":..}
"""
import argparse
import json
import os
import sys
import traceback
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest, Recorder
from hftbacktest.stats import LinearAssetRecord
from backtests.strategy_registry import get_strategy


def _resolve_mid(best_bid, best_ask):
    if np.isfinite(best_bid) and np.isfinite(best_ask):
        return 0.5 * (best_bid + best_ask)
    if np.isfinite(best_bid):
        return best_bid
    if np.isfinite(best_ask):
        return best_ask
    return 0.0


def build_asset(data_array, tick_size, lot_size, maker_fee, taker_fee,
                last_trades_capacity=1_000_000):
    return (
        BacktestAsset()
        .data([data_array])
        .linear_asset(1.0)
        .constant_order_latency(10_000_000, 10_000_000)
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
        .trading_value_fee_model(maker_fee, taker_fee)
        .tick_size(tick_size)
        .lot_size(lot_size)
        .last_trades_capacity(last_trades_capacity)
    )


def run_one(npz_path, strategy_name, params, contract):
    spec = get_strategy(strategy_name)
    with np.load(npz_path) as d:
        data = d["data"]
    asset = build_asset(data, contract["tick_size"], contract["lot_size"],
                        contract["maker_fee"], contract["taker_fee"])
    hbt = HashMapMarketDepthBacktest([asset])
    # hftbacktest 的账户余额从 0 起算；equity 直接代表策略净盈亏（含持仓按市价折算）。
    # 故 per-seg Return 用 equity 作为分子，单位净值，受 lot/规模影响——
    # 为满足规模无关性，由 aggregate.py 用各 seg 的 equity 区间转换为 return。
    initial_balance = 1.0

    # buy&hold 基准：用 npz 首末 px 近似 mid（snapshot 事件 px 即最新价）
    prices = data["px"]
    nz = prices[prices > 0]
    first_px = float(nz[0]) if len(nz) else 0.0
    last_px = float(nz[-1]) if len(nz) else 0.0

    summary_metrics = {}
    final_state = None
    recorder = Recorder(1, 5_000_000) if spec.uses_recorder else None

    try:
        if spec.uses_recorder:
            exit_code = spec.func(hbt, recorder.recorder, **params)
            _ = hbt.close()
            stats = LinearAssetRecord(recorder.get(0)).stats(book_size=10_000)
            df = stats.summary()
            if not df.is_empty():
                row = df.to_pandas().iloc[0].to_dict()
                for k, v in row.items():
                    if isinstance(v, (pd.Timestamp, np.datetime64)):
                        v = pd.to_datetime(v).isoformat()
                    summary_metrics[str(k)] = float(v) if isinstance(v, (int, float, np.integer, np.floating)) else v
            ok = bool(exit_code) if isinstance(exit_code, bool) else True
        else:
            exit_code = spec.func(hbt, **params)
            # 在关闭前读取最终账户状态
            depth = hbt.depth(0)
            state = hbt.state_values(0)
            mid = _resolve_mid(depth.best_bid, depth.best_ask)
            final_state = {
                "balance": float(state.balance),
                "position": float(state.position),
                "fee": float(state.fee),
                "mid_price": float(mid),
                "equity": float(state.balance + state.position * mid - state.fee),
            }
            ok = spec.is_success(exit_code) if spec.is_success else (exit_code == 0)
            try:
                _ = hbt.close()
            except Exception:
                pass
    except Exception as e:
        return None, {"error": str(e), "traceback": traceback.format_exc()[-1500:]}

    # 统一提取 per-seg 指标
    if spec.uses_recorder:
        metrics_out = {
            "equity": float(summary_metrics.get("Equity", 0.0)),
            "return": float(summary_metrics.get("Return", 0.0)),
            "sharpe": float(summary_metrics.get("SR", summary_metrics.get("Sharpe", 0.0))),
            "sortino": float(summary_metrics.get("Sortino", 0.0)),
            "max_drawdown": float(summary_metrics.get("MaxDrawdown", 0.0)),
            "num_trades": float(summary_metrics.get("DailyNumberOfTrades", summary_metrics.get("NumberOfTrades", 0))),
            "fee": float(summary_metrics.get("Fee", 0.0)),
            "_raw": summary_metrics,
        }
    else:
        equity = final_state["equity"] if final_state else 0.0
        metrics_out = {
            "equity": equity,
            "return": equity,  # 净盈亏金额（单位 USDT）；aggregate 归一化为 per-lot 或 per-balance 比率
            "sharpe": 0.0,
            "sortino": 0.0,
            "max_drawdown": 0.0,
            "num_trades": 0,
            "fee": final_state["fee"] if final_state else 0.0,
            "final_position": float(final_state["position"]) if final_state else 0.0,
            "_raw": (final_state or {}),
        }
    metrics_out["buyhold_return"] = (last_px / first_px - 1.0) if first_px > 0 else 0.0
    return metrics_out, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--npz", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--params", required=True)  # JSON dict
    parser.add_argument("--contract", required=True, help="合约 JSON 字符串 或 文件路径")
    parser.add_argument("--seg-index", required=True)
    parser.add_argument("--seg-duration-h", type=float, default=0.0)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    params = json.loads(args.params)
    contract_arg = args.contract
    if os.path.exists(contract_arg):
        with open(contract_arg) as f:
            contract = json.load(f)
    else:
        contract = json.loads(contract_arg)
    metrics, err = run_one(args.npz, args.strategy, params, contract)
    out = {
        "strategy": args.strategy,
        "seg_index": int(args.seg_index),
        "seg_duration_h": args.seg_duration_h,
        "params": params,
        "status": "ok" if err is None else "fail",
        "metrics": metrics or {},
        "error": err or {},
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(out, f, default=str, indent=2)
    print(json.dumps({"out": args.out, "status": out["status"]}))


if __name__ == "__main__":
    main()