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
from hftbacktest.types import BUY_EVENT, DEPTH_SNAPSHOT_EVENT, SELL_EVENT
from hftbacktest.stats import LinearAssetRecord
from hftbacktest.stats.metrics import (
    DailyNumberOfTrades,
    DailyTradingValue,
    MaxDrawdown,
    MaxPositionValue,
    Ret,
    ReturnOverMDD,
    ReturnOverTrade,
    SR,
    Sortino,
)
from backtests.strategy_registry import get_strategy
from rl.policy_core import RL_DIAGNOSTIC_FIELDS, terminal_liquidation_cost


def _json_safe(value):
    """Convert non-finite/numpy values to strict JSON-compatible values."""
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if np.isfinite(number) else None
    return value


def _resolve_mid(best_bid, best_ask):
    if np.isfinite(best_bid) and np.isfinite(best_ask):
        return 0.5 * (best_bid + best_ask)
    if np.isfinite(best_bid):
        return best_bid
    if np.isfinite(best_ask):
        return best_ask
    return 0.0


def _initial_snapshot_mid(data):
    event_mask = (data["ev"] & DEPTH_SNAPSHOT_EVENT) != 0
    if not event_mask.any():
        return 0.0
    snapshot_ts = int(data["exch_ts"][event_mask].min())
    at_snapshot = event_mask & (data["exch_ts"] == snapshot_ts)
    buys = at_snapshot & ((data["ev"] & BUY_EVENT) != 0) & (data["px"] > 0)
    sells = at_snapshot & ((data["ev"] & SELL_EVENT) != 0) & (data["px"] > 0)
    if not buys.any() or not sells.any():
        return 0.0
    return float(0.5 * (data["px"][buys].max() + data["px"][sells].min()))


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
        first_ts = int(data["exch_ts"].min())
        cutoff = first_ts + int(max_seg_hours * 3600 * 1_000_000_000)
        data = data[data["exch_ts"] <= cutoff]
    if len(data) == 0:
        return None, {"error": "empty_data"}
    backtest_duration_h = float(
        (int(data["exch_ts"].max()) - int(data["exch_ts"].min()))
        / (3600 * 1_000_000_000)
    )
    asset = build_asset(data, contract["tick_size"], contract["lot_size"],
                        contract["maker_fee"], contract["taker_fee"])
    hbt = HashMapMarketDepthBacktest([asset])

    first_mid = _initial_snapshot_mid(data)

    report_notional = float(
        contract.get("report_notional_usdt", contract["initial_balance"])
    )
    summary_metrics = {}
    final_state = None
    liquidation_cost = 0.0
    strategy_diagnostics = {}
    recorder = Recorder(1, 5_000_000) if spec.uses_recorder else None

    try:
        if spec.uses_recorder:
            if spec.params_as_object:
                nt = _nt("Params", spec.param_keys)
                strategy_result = spec.func(
                    hbt, recorder.recorder, nt(**params)
                )
            else:
                strategy_result = spec.func(
                    hbt, recorder.recorder, **params
                )
            if (
                isinstance(strategy_result, dict)
                and "exit_code" in strategy_result
            ):
                exit_code = strategy_result["exit_code"]
                strategy_diagnostics = dict(
                    strategy_result.get("diagnostics", {})
                )
            else:
                exit_code = strategy_result
            depth = hbt.depth(0)
            state = hbt.state_values(0)
            mid = _resolve_mid(depth.best_bid, depth.best_ask)
            liquidation_cost = terminal_liquidation_cost(
                state, depth, contract["taker_fee"]
            )
            final_state = {
                "balance": float(state.balance),
                "position": float(state.position),
                "fee": float(state.fee),
                "mid_price": float(mid),
                "liquidation_cost": float(liquidation_cost),
                "equity": float(
                    state.balance + state.position * mid - state.fee - liquidation_cost
                ),
                "num_trades": float(state.num_trades),
                "trading_volume": float(state.trading_volume),
                "trading_value": float(state.trading_value),
            }
            record_data = recorder.get(0).copy()
            if len(record_data) == 0:
                _ = hbt.close()
                return None, {"error": "empty_recorder"}
            # Make the final record executable: flatten at bid/ask and charge
            # taker fees.  Treat both spread and fee as terminal execution cost.
            record_data[-1]["fee"] += liquidation_cost
            stats = LinearAssetRecord(record_data).stats(
                metrics=[
                    SR(trading_days_per_year=365),
                    Sortino(trading_days_per_year=365),
                    Ret(book_size=report_notional),
                    MaxDrawdown(book_size=report_notional),
                    DailyNumberOfTrades(),
                    DailyTradingValue(),
                    ReturnOverMDD(),
                    ReturnOverTrade(),
                    MaxPositionValue(),
                ]
            )
            df = stats.summary()
            if not df.is_empty():
                row = df.to_pandas().iloc[0].to_dict()
                for k, v in row.items():
                    if isinstance(v, (pd.Timestamp, np.datetime64)):
                        v = pd.to_datetime(v).isoformat()
                    summary_metrics[str(k)] = float(v) if isinstance(v, (int, float, np.integer, np.floating)) else v
            if spec.is_success is not None:
                ok = bool(spec.is_success(strategy_result))
            elif isinstance(exit_code, (bool, np.bool_)):
                ok = bool(exit_code)
            else:
                ok = exit_code in (1, 2, 15)
            _ = hbt.close()
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
                "liquidation_cost": float(
                    terminal_liquidation_cost(state, depth, contract["taker_fee"])
                ),
                "num_trades": float(state.num_trades),
                "trading_volume": float(state.trading_volume),
                "trading_value": float(state.trading_value),
            }
            final_state["equity"] = float(
                state.balance
                + state.position * mid
                - state.fee
                - final_state["liquidation_cost"]
            )
            ok = spec.is_success(exit_code) if spec.is_success else (exit_code == 0)
            try:
                _ = hbt.close()
            except Exception:
                pass
    except Exception:
        try:
            _ = hbt.close()
        except Exception:
            pass
        return None, {"error": "exception", "traceback": traceback.format_exc()[-1500:]}

    if not ok:
        return None, {
            "error": "bad_exit_code",
            "exit_code": bool(exit_code) if isinstance(exit_code, np.bool_) else exit_code,
        }

    if spec.uses_recorder:
        equity = float(final_state["equity"])
        metrics_out = {
            "equity": equity,
            "return": equity / report_notional,
            "sharpe": float(summary_metrics.get("SR", summary_metrics.get("Sharpe", 0.0))),
            "sortino": float(summary_metrics.get("Sortino", 0.0)),
            "max_drawdown": float(summary_metrics.get("MaxDrawdown", 0.0)),
            "num_trades": float(final_state["num_trades"]),
            "daily_num_trades": float(summary_metrics.get("DailyNumberOfTrades", 0.0)),
            "fee": float(final_state["fee"]),
            "liquidation_cost": float(final_state["liquidation_cost"]),
            "final_position": float(final_state["position"]),
            "trading_volume": float(final_state["trading_volume"]),
            "trading_value": float(final_state["trading_value"]),
            "_raw": summary_metrics,
        }
    else:
        equity = final_state["equity"] if final_state else 0.0
        metrics_out = {
            "equity": equity,
            "return": equity / report_notional,
            "sharpe": 0.0,
            "sortino": 0.0,
            "max_drawdown": 0.0,
            "num_trades": final_state["num_trades"] if final_state else 0.0,
            "fee": final_state["fee"] if final_state else 0.0,
            "liquidation_cost": final_state["liquidation_cost"] if final_state else 0.0,
            "final_position": float(final_state["position"]) if final_state else 0.0,
            "trading_volume": final_state["trading_volume"] if final_state else 0.0,
            "trading_value": final_state["trading_value"] if final_state else 0.0,
            "_raw": (final_state or {}),
        }
    last_mid = float(final_state["mid_price"]) if final_state else 0.0
    metrics_out["buyhold_return"] = (
        last_mid / first_mid - 1.0
        if first_mid > 0 and last_mid > 0
        else 0.0
    )
    metrics_out["backtest_duration_h"] = backtest_duration_h
    metrics_out["report_notional"] = report_notional
    for field in RL_DIAGNOSTIC_FIELDS:
        if field in strategy_diagnostics:
            metrics_out[field] = float(strategy_diagnostics[field])
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
        "result_schema_version": "backtest-result-v2",
        "strategy": args.strategy,
        "seg_index": int(args.seg_index),
        "seg_duration_h": args.seg_duration_h,
        "params": params,
        "status": "ok" if err is None else "fail",
        "metrics": metrics or {},
        "error": err or {},
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out = _json_safe(out)
    with open(args.out, "w") as f:
        json.dump(out, f, default=str, indent=2, allow_nan=False)
    print(json.dumps({"out": args.out, "status": out["status"]}))
    if err is not None:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
