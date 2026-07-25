r"""Phase 2 网格搜索主程序。

用法：
    python backtests/grid_search.py --strategy mean_reversion ^
        --manifest E:\tmp\npz\manifest.csv --split train ^
        --grid grids\mean_reversion.json --contract contracts\btc_usdt_swap.json ^
        --out-dir E:\tmp\results\mr_train --processes 8

grid json 格式（参数值列表的笛卡尔积，键须与策略 param_keys 一致）：
    {"ema_alpha":[0.1,0.2], "threshold_ticks":[1.5,2.0], "step_ns":[50000000]}

contract json：{"tick_size":0.1,"lot_size":0.01,"maker_fee":0.0002,"taker_fee":0.0007,
               "initial_balance":100000}
"""
import argparse
import json
import os
import subprocess
import sys
from itertools import product
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_PY = sys.executable
_WORKER = Path(__file__).parent / "worker_grid.py"


def _run_task(t):
    """模块级 worker：通过子进程执行单次回测，规避 numba 与本地对象在不同进程间不可 pickle。"""
    cmd = [_PY, str(_WORKER), "--npz", t["npz"], "--strategy", t["strategy"],
           "--params", t["params"], "--contract", t["contract"],
           "--seg-index", t["seg_index"], "--seg-duration-h", t["seg_duration_h"],
           "--out", t["out"]]
    Path(t["out"]).parent.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return 0
    except Exception:
        return -1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--split", default="train", choices=["train", "val", "test", "all"])
    parser.add_argument("--grid", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--processes", type=int, default=8)
    parser.add_argument("--max-per-seg", type=int, default=0,
                        help="每个 seg 最多跑的参数组合数（0=全部），便于 smoke test")
    args = parser.parse_args()

    with open(args.grid) as f:
        grid = json.load(f)
    with open(args.contract) as f:
        contract = json.load(f)
    _ = contract  # contract 文件路径直接传给 worker 读取（避免超长 JSON 串）
    m = pd.read_csv(args.manifest)
    if args.split != "all":
        m = m[m["split"] == args.split]
    segs = m.to_dict("records")

    keys = list(grid.keys())
    combos = list(product(*[grid[k] for k in keys]))
    if args.max_per_seg > 0 and len(combos) > args.max_per_seg:
        combos = combos[:args.max_per_seg]
    print(f"[grid_search] strategy={args.strategy} split={args.split} "
          f"segs={len(segs)} combos={len(combos)} tasks={len(segs)*len(combos)}")

    tasks = []
    for seg in segs:
        for ci, combo in enumerate(combos):
            params = dict(zip(keys, combo))
            out_file = Path(args.out_dir) / f"seg{int(seg['seg_index'])}_{Path(seg['npz_path']).stem}" / f"param_{ci}.json"
            if out_file.exists():
                continue
            tasks.append({
                "npz": seg["npz_path"],
                "strategy": args.strategy,
                "params": json.dumps(params),
                "contract": args.contract,
                "seg_index": str(int(seg["seg_index"])),
                "seg_duration_h": str(float(seg["duration_hours"])),
                "out": str(out_file),
                "ci": ci,
            })

    print(f"[grid_search] 待跑任务: {len(tasks)}（已跳过已存在的）")
    if not tasks:
        print("无新任务。"); return

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    import multiprocessing as mp
    done = 0
    with mp.Pool(processes=args.processes) as pool:
        for _ in pool.imap_unordered(_run_task, tasks, chunksize=1):
            done += 1
            if done % 10 == 0 or done == len(tasks):
                print(f"  进度 {done}/{len(tasks)}", flush=True)
    print(f"[grid_search] 完成: {done} 任务")


if __name__ == "__main__":
    main()