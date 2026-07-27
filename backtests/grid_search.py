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


def _make_cmd(t):
    return ([_PY, str(_WORKER), "--npz", t["npz"], "--strategy", t["strategy"],
             "--params", t["params"], "--contract", t["contract"],
             "--seg-index", t["seg_index"], "--seg-duration-h", t["seg_duration_h"],
             "--max-seg-hours", t["max_seg_hours"],
             "--out", t["out"]])


def run_tasks_popen(tasks, processes, out_dir, fail_log="failures.log"):
    """Popen 限并发：每任务一个独立子进程，单个崩溃不影响其他。

    比 multiprocessing.Pool 更鲁棒：mp.Pool 的 worker 崩溃会触发
    BrokenProcessPool 致整个 pool 退出；Popen 列表中单进程异常只影响自身。
    """
    import subprocess as sp
    import time
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    fail_path = Path(out_dir) / fail_log
    fail_fh = open(fail_path, "a", encoding="utf-8")
    running = []  # list of (proc, task)
    todo = list(tasks)
    done = 0
    total = len(todo)
    CREATIONFLAGS = 0
    try:
        CREATIONFLAGS = sp.CREATE_NO_WINDOW
    except AttributeError:
        CREATIONFLAGS = 0
    while todo or running:
        while todo and len(running) < processes:
            t = todo.pop(0)
            Path(t["out"]).parent.mkdir(parents=True, exist_ok=True)
            try:
                p = sp.Popen(_make_cmd(t), stdout=sp.DEVNULL, stderr=sp.DEVNULL,
                             creationflags=CREATIONFLAGS)
                running.append((p, t))
            except Exception as e:
                fail_fh.write(f"[spawn] {t['out']}: {e}\n"); fail_fh.flush()
        if not running:
            continue
        time.sleep(0.2)
        still = []
        for p, t in running:
            rc = p.poll()
            if rc is None:
                still.append((p, t))
            else:
                done += 1
                if rc != 0 or not Path(t["out"]).exists():
                    fail_fh.write(f"[rc={rc}] seg{t['seg_index']} {t['out']}\n"); fail_fh.flush()
                if done % 10 == 0 or done == total:
                    print(f"  进度 {done}/{total} (running={len(still)})", flush=True)
        running = still
    fail_fh.close()
    return done


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--split", default="train", choices=["train", "val", "test", "all"])
    parser.add_argument("--grid", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--processes", type=int, default=8)
    parser.add_argument("--skip-existing", action="store_true",
                        help="已存在的结果文件跳过（断点续跑）")
    parser.add_argument("--max-seg-hours", type=float, default=0.0,
                        help="网格搜索时截断每段回测到前 N 小时（0=完整段）；建议 6-12 加速长段")
    parser.add_argument("--max-per-seg", type=int, default=0,
                        help="每个 seg 最多跑的参数组合数（0=全部），便于 smoke test")
    args = parser.parse_args()

    with open(args.grid) as f:
        grid_raw = json.load(f)
    # 去重每个参数的取值列表（top-K 网格常含重复值，避免笛卡尔爆炸）
    grid = {k: list(dict.fromkeys(v)) for k, v in grid_raw.items()}
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
                "max_seg_hours": str(args.max_seg_hours),
            })

    print(f"[grid_search] 待跑任务: {len(tasks)}（已跳过已存在的）")
    if not tasks:
        print("无新任务。"); return

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    done = run_tasks_popen(tasks, args.processes, args.out_dir)
    print(f"[grid_search] 完成: {done}/{len(tasks)} 任务")


if __name__ == "__main__":
    main()