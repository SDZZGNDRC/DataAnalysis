r"""聚合 grid_search 的 result JSON 为分析用宽表。

用法：
    python evaluation/aggregate.py --results-dir E:\tmp\results\mr_train ^
        --out E:\tmp\results\mr_train\aggregate.csv

扫描 --results-dir 下的所有 *.json，解析为 pandas DataFrame，列：
  strategy, seg_index, seg_duration_h, <params...>, status,
  equity, return, sharpe, sortino, max_drawdown, num_trades, fee, buyhold_return
"""
import argparse
import glob
import json
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    rows = []
    for f in glob.glob(str(Path(args.results_dir) / "**" / "*.json"), recursive=True):
        with open(f) as fh:
            d = json.load(fh)
        row = {"strategy": d.get("strategy"), "seg_index": d.get("seg_index"),
               "seg_duration_h": d.get("seg_duration_h"), "status": d.get("status")}
        row.update(d.get("params", {}))
        m = d.get("metrics", {}) or {}
        for k in ("equity", "return", "sharpe", "sortino", "max_drawdown",
                  "num_trades", "fee", "buyhold_return", "final_position"):
            row[k] = m.get(k)
        row["error"] = (d.get("error") or {}).get("error", "")
        rows.append(row)

    if not rows:
        print("无结果文件"); return
    df = pd.DataFrame(rows)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    ok = df[df["status"] == "ok"]
    print(f"汇总: {len(df)} 行 (ok={len(ok)})")
    print(f"seg 数: {ok['seg_index'].nunique() if len(ok) else 0}")
    print(f"参数组数: {ok.drop(columns=['seg_index','seg_duration_h','status','equity','return','sharpe','sortino','max_drawdown','num_trades','fee','buyhold_return','final_position','error','strategy']).drop_duplicates().shape[0] if len(ok) else 0}")
    print(f"输出: {args.out}")


if __name__ == "__main__":
    main()