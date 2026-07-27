r"""Grid search worker: run one backtest of (npz, strategy, params) and write result JSON.

Launched as subprocess by backtests/grid_search.py to avoid pickling numba-compiled
strategies across processes. Memory: each worker loads its npz independently.
"""
import argparse
import json
import os
import sys
import traceback
from collections import namedtuple as _nt
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


def run_one(npz_path, strategy_name, params, contract, max_seg_hours=0.0):
    spec = get_strategy(strategy_name)
    with np.load(npz_path) as d:
        data = d["data"]
    if max_seg_hours and max_seg_hours > 0:
        # Truncate to first max_seg_hours of the segment; snapshot is at the
        # head of the array so book reconstruction is unaffected.
        first_ts = int(data["exch_ts"][0])
        cutoff = first_ts + int(max_seg_hours * 3600 * 1_000_000_000)
        idx = int(np.searchsorted(data["exch_ts"], cutoff, side="right"))
        if 0 < idx < len(data):
            data = data[:idx]
    asset = build_asset(data, contract["tick_size"], contract["lot_size"],
                        contract["maker_fee"], contract["taker_fee"])
    hbt = HashMapMarketDepthBacktest([asset])

    # buy&hold baseline: approx mid via first/last positive px in the event stream
    prices = data["px"]
    nz = prices[prices > 0]
    first_px = float(nz[0]) if len(nz) else 0.0
    last_px = float(nz[-1]) if len(nz) else 0.0

    summary_metrics = {}
    final_state = None
    recorder = Recorder(1, 5_000_000) if spec.uses_recorder else None

    try:
        if spec.uses_recorder:
            if spec.params_as_object:
                nt = _nt("Params", spec.param_keys)
                exit_code = spec.func(hbt, recorder.recorder, nt(**params))
            else:
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
            if spec.params_as_object:
                nt = _nt("Params", spec.param_keys)
                exit_code = spec.func(hbt, nt(**params))
            else:
                exit_code = spec.func(hbt, **params)
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
    except Exception:
        return None, {"error": "exception", "traceback": traceback.format_exc()[-1500:]}

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
            "return": equity,
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
    parser.add_argument("--params", required=True)
    parser.add_argument("--contract", required=True, help="contract JSON string or file path")
    parser.add_argument("--seg-index", required=True)
    parser.add_argument("--seg-duration-h", type=float, default=0.0)
    parser.add_argument("--max-seg-hours", type=float, default=0.0)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    params = json.loads(args.params)
    contract_arg = args.contract
    if os.path.exists(contract_arg):
        with open(contract_arg) as f:
            contract = json.load(f)
    else:
        contract = json.loads(contract_arg)
    metrics, err = run_one(args.npz, args.strategy, params, contract,
                          max_seg_hours=args.max_seg_hours)
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