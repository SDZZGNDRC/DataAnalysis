r"""Select best params from train aggregate, write val grid for top-K, print report.

For each parameter combination (grouped by the strategy's param_keys), aggregate
per-segment metrics:
All strategies are ranked on the same per-segment return series.  The primary
score is a cross-segment return t-statistic, not an average of annualised
within-segment Sharpe ratios.

The first invocation writes explicit top-K validation tuples.  After validation
has been run, pass ``--val-agg`` and ``--test-grid-out`` to select exactly one
test tuple from validation performance.
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtests.strategy_registry import get_strategy


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", required=True)
    p.add_argument("--train-agg", required=True)
    p.add_argument("--notional", type=float, default=10000.0,
                   help="deprecated; v2 aggregate return is already capital-normalised")
    p.add_argument("--top-k", type=int, default=5)
    p.add_argument("--val-grid-out", required=True)
    p.add_argument("--val-agg", default=None,
                   help="optional validation aggregate; required to select a test tuple")
    p.add_argument("--test-grid-out", default=None)
    p.add_argument("--report-out", required=True)
    args = p.parse_args()

    spec = get_strategy(args.strategy)
    has_recorder = spec.uses_recorder
    non_param = {"result_schema_version", "strategy", "seg_index", "seg_duration_h",
                 "status", "equity", "return",
                 "sharpe", "sortino", "max_drawdown", "num_trades", "fee", "buyhold_return",
                 "daily_num_trades", "liquidation_cost", "final_position",
                 "trading_volume", "trading_value", "backtest_duration_h",
                 "report_notional", "error"}
    df = pd.read_csv(args.train_agg)
    if (
        "result_schema_version" not in df
        or not df["result_schema_version"].eq("backtest-result-v2").all()
    ):
        raise ValueError("parameter selection requires backtest-result-v2 aggregates")
    df = df[df["status"] == "ok"].copy()
    param_cols = [c for c in df.columns if c not in non_param]
    df["pk"] = df[param_cols].astype(str).agg("|".join, axis=1)

    rows = []
    for pk, g in df.groupby("pk"):
        g = g.sort_values("seg_index")
        n = len(g)
        eq = g["equity"].astype(float).values
        r = g["return"].astype(float).values
        r_std = float(np.std(r, ddof=1)) if n > 1 else 0.0
        return_t_stat = float(np.mean(r) / r_std * np.sqrt(n)) if r_std > 0 else np.nan
        seg_sharpe_mean = (
            float(g["sharpe"].astype(float).mean())
            if g["sharpe"].notna().any()
            else np.nan
        )
        seg_return = float(r.mean()) if n else 0.0
        sum_eq = float(eq.sum())
        win = float((eq > 0).mean()) if n else 0.0
        cum = np.cumsum(eq)
        mdd = abs(float((cum - np.maximum.accumulate(cum)).min())) if n else 0.0
        bh = float(g["buyhold_return"].astype(float).mean()) if n else 0.0
        rec = {"pk": pk, "n_segs": n, "return_t_stat": return_t_stat,
               "seg_sharpe_mean": seg_sharpe_mean,
               "seg_return": seg_return, "sum_equity": sum_eq,
               "win_rate": win, "cross_mdd": mdd, "mean_buyhold": bh}
        for c in param_cols:
            rec[c] = g[c].iloc[0]
        rows.append(rec)
    agg = pd.DataFrame(rows)
    expected_segments = int(df["seg_index"].nunique())
    incomplete = agg["n_segs"] != expected_segments
    if incomplete.any():
        print(
            f"忽略 {int(incomplete.sum())} 个未覆盖全部 "
            f"{expected_segments} 个训练段的参数组合"
        )
        agg = agg[~incomplete].copy()
    if agg.empty:
        raise RuntimeError("没有覆盖完整训练集的参数组合")

    rank_key = "return_t_stat"
    agg = agg.sort_values([rank_key, "sum_equity"], ascending=[False, False])
    top = agg.head(args.top_k).copy()

    def native(value):
        return value.item() if isinstance(value, np.generic) else value

    val_grid = {
        "_combinations": [
            {c: native(row[c]) for c in param_cols}
            for _, row in top.iterrows()
        ]
    }
    Path(args.val_grid_out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.val_grid_out, "w") as f:
        json.dump(val_grid, f, indent=2)

    if bool(args.val_agg) != bool(args.test_grid_out):
        p.error("--val-agg and --test-grid-out must be provided together")
    if args.val_agg:
        val_df = pd.read_csv(args.val_agg)
        if (
            "result_schema_version" not in val_df
            or not val_df["result_schema_version"].eq("backtest-result-v2").all()
        ):
            raise ValueError("validation selection requires backtest-result-v2 aggregates")
        val_df = val_df[val_df["status"] == "ok"].copy()
        val_df["pk"] = val_df[param_cols].astype(str).agg("|".join, axis=1)
        val_rows = []
        for pk, group in val_df.groupby("pk"):
            returns = group["return"].astype(float).values
            std = float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0
            score = (
                float(np.mean(returns) / std * np.sqrt(len(returns)))
                if std > 0
                else np.nan
            )
            row = {
                "pk": pk,
                "return_t_stat": score,
                "sum_equity": float(group["equity"].sum()),
            }
            for c in param_cols:
                row[c] = group[c].iloc[0]
            val_rows.append(row)
        val_ranked = pd.DataFrame(val_rows).sort_values(
            ["return_t_stat", "sum_equity"], ascending=[False, False]
        )
        expected_val_segments = int(val_df["seg_index"].nunique())
        counts = val_df.groupby("pk")["seg_index"].nunique()
        valid_keys = set(counts[counts == expected_val_segments].index)
        val_ranked = val_ranked[val_ranked["pk"].isin(valid_keys)]
        if val_ranked.empty:
            raise RuntimeError("没有覆盖完整验证集的参数组合")
        best = val_ranked.iloc[0]
        test_grid = {
            "_combinations": [{c: native(best[c]) for c in param_cols}]
        }
        Path(args.test_grid_out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.test_grid_out, "w") as f:
            json.dump(test_grid, f, indent=2)

    # report
    rep = []
    rep.append(f"# {args.strategy} 参数选择\n")
    rep.append(f"- recorder: {has_recorder} | 排序键: {rank_key} | 训练 seg: {df['seg_index'].nunique()} | 参数组: {len(agg)}\n")
    rep.append("## top-K 训练期聚合\n")
    cols = ["n_segs", "return_t_stat", "seg_sharpe_mean", "seg_return",
            "sum_equity", "win_rate", "cross_mdd", "mean_buyhold"] + param_cols
    rep.append(top[cols].to_markdown(index=False))
    rep.append("\n## 训练期全参数分布（前10 & 后10）\n")
    show = agg.head(10)[["return_t_stat", "seg_return", "sum_equity"] + param_cols]
    rep.append(show.to_markdown(index=False))
    Path(args.report_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report_out).write_text("\n".join(rep) + "\n", encoding="utf-8")
    print(f"top-K grid -> {args.val_grid_out}")
    if args.test_grid_out:
        print(f"validation-selected top-1 grid -> {args.test_grid_out}")
    print(f"report -> {args.report_out}")
    print("\nTOP5:")
    print(top[cols].to_string(index=False))


if __name__ == "__main__":
    main()
