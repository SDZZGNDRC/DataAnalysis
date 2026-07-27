r"""Select best params from train aggregate, write val grid for top-K, print report.

For each parameter combination (grouped by the strategy's param_keys), aggregate
per-segment metrics:
  - recorder strategies: seg_sharpe = mean(per-seg SR), seg_return = mean(per-seg Return)
  - non-recorder strategies: pseudo cross-sharpe = mean(per-seg equity)/std(per-seg equity);
    per-seg "return" = equity / notional (size-normalised)

Rank by available sharpe, output top-K as a val grid json (so Phase 3 val run reuses
grid_search with that grid) and top-1 as a single test grid json.
"""
import argparse
import json
import sys
from itertools import product
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
                   help="per-seg notional used to size-normalise equity for non-recorder strategies")
    p.add_argument("--top-k", type=int, default=5)
    p.add_argument("--val-grid-out", required=True)
    p.add_argument("--test-grid-out", required=True)
    p.add_argument("--report-out", required=True)
    args = p.parse_args()

    spec = get_strategy(args.strategy)
    has_recorder = spec.uses_recorder
    non_param = {"strategy", "seg_index", "seg_duration_h", "status", "equity", "return",
                 "sharpe", "sortino", "max_drawdown", "num_trades", "fee", "buyhold_return",
                 "final_position", "error"}
    df = pd.read_csv(args.train_agg)
    df = df[df["status"] == "ok"].copy()
    param_cols = [c for c in df.columns if c not in non_param]
    df["pk"] = df[param_cols].astype(str).agg("|".join, axis=1)

    rows = []
    for pk, g in df.groupby("pk"):
        g = g.sort_values("seg_index")
        n = len(g)
        eq = g["equity"].astype(float).values
        if has_recorder and g["sharpe"].notna().any():
            seg_sharpe = float(g["sharpe"].astype(float).mean())
            seg_return = float(g["return"].astype(float).mean())
        else:
            r = eq / args.notional
            seg_sharpe = float(r.mean() / r.std()) if (n > 1 and r.std() > 0) else np.nan
            seg_return = float(r.mean()) if n else 0.0
        sum_eq = float(eq.sum())
        win = float((eq > 0).mean()) if n else 0.0
        cum = np.cumsum(eq)
        mdd = float((cum - np.maximum.accumulate(cum)).min()) if n else 0.0
        bh = float(g["buyhold_return"].astype(float).mean()) if n else 0.0
        rec = {"pk": pk, "n_segs": n, "seg_sharpe": seg_sharpe,
               "seg_return": seg_return, "sum_equity": sum_eq,
               "win_rate": win, "cross_mdd": mdd, "mean_buyhold": bh}
        for c in param_cols:
            rec[c] = g[c].iloc[0]
        rows.append(rec)
    agg = pd.DataFrame(rows)

    # ranking
    if has_recorder and agg["seg_sharpe"].notna().any():
        rank_key = "seg_sharpe"
    else:
        rank_key = "seg_sharpe"  # for non-recorder we put pseudo-sharpe in same column
    agg = agg.sort_values([rank_key, "sum_equity"], ascending=[False, False])
    top = agg.head(args.top_k).copy()

    # write val/test grids
    def to_grid(rows_df):
        g = {}
        for c in param_cols:
            vals = []
            for _, r in rows_df.iterrows():
                v = r[c]
                try:
                    iv = int(v)
                    if str(iv) == str(v) or float(iv) == float(v):
                        vals.append(iv); continue
                except Exception:
                    pass
                try:
                    vals.append(float(v))
                except Exception:
                    vals.append(v)
            g[c] = vals
        return g
    val_grid = to_grid(top)
    test_grid = {c: [val_grid[c][0]] for c in param_cols}
    Path(args.val_grid_out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.val_grid_out, "w") as f:
        json.dump(val_grid, f, indent=2)
    with open(args.test_grid_out, "w") as f:
        json.dump(test_grid, f, indent=2)

    # report
    rep = []
    rep.append(f"# {args.strategy} 参数选择\n")
    rep.append(f"- recorder: {has_recorder} | 排序键: {rank_key} | 训练 seg: {df['seg_index'].nunique()} | 参数组: {len(agg)}\n")
    rep.append("## top-K 训练期聚合\n")
    cols = ["n_segs", "seg_sharpe", "seg_return", "sum_equity", "win_rate", "cross_mdd", "mean_buyhold"] + param_cols
    rep.append(top[cols].to_markdown(index=False))
    rep.append("\n## 训练期全参数分布（前10 & 后10）\n")
    show = agg.head(10)[["seg_sharpe", "seg_return", "sum_equity"] + param_cols]
    rep.append(show.to_markdown(index=False))
    Path(args.report_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report_out).write_text("\n".join(rep) + "\n", encoding="utf-8")
    print(f"top-K grid -> {args.val_grid_out}")
    print(f"top-1 grid -> {args.test_grid_out}")
    print(f"report -> {args.report_out}")
    print("\nTOP5:")
    print(top[cols].to_string(index=False))


if __name__ == "__main__":
    main()