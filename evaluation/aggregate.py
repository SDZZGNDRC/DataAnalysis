r"""Aggregate grid_search results into a wide CSV.

Scans --results-dir/**/*.json -> DataFrame.
"""
import argparse
import glob
import json
from pathlib import Path

import pandas as pd


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--results-dir", required=True)
    p.add_argument("--out", required=True)
    args = p.parse_args()

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
        print("no files"); return
    df = pd.DataFrame(rows)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    ok = df[df["status"] == "ok"]
    fail = df[df["status"] != "ok"]
    if len(fail) and fail["error"].str.len().any():
        print("first error:", fail["error"].iloc[0])
    print(f"rows={len(df)} ok={len(ok)} segs={ok['seg_index'].nunique() if len(ok) else 0} out={args.out}")


if __name__ == "__main__":
    main()