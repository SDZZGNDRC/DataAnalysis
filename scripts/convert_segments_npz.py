r"""将 seqId 会话段转换为 hftbacktest npz（Phase 1）。

读取 build_june_segments.py 生成的 segments.csv，为每个（snapshot 头、时长≥阈值）
的段配对时间范围内的 Trades 7z，调用 hftbacktest_okx.py 生成 npz。

由于 BTC-USDT-SWAP Books-400 全月事件量巨大（原始量级约 60GB，超过 E: 可用空间），
本脚本通过 ``--max-depth-levels N`` 限制每条更新只保留按价格排名的顶层 N 档，
使 npz 体积降到可承载范围，同时仍满足本项目策略（仅需 best bid/ask + trades）的需求。

用法：
    python scripts/convert_segments_npz.py --csv E:\tmp\segments.csv ^
        --pool-root E:\datapool --out-dir E:\tmp\npz --processes 10 ^
        --min-duration 1.0 --max-depth-levels 10 --skip-existing

注意：E:\\datapool 只读；本脚本通过子进程调用 hftbacktest_okx.py，仅在 E:\\tmp 写出 npz。
"""
import argparse
import csv
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hftbacktest_okx import convert as convert_mem  # 内存版，直接传入文件列表，规避命令行长度限制
from hftbacktest_okx_mmap import convert as convert_mmap  # mmap 版，用于超大段（事件千万级），避免内存爆/系统资源耗尽


def daterange(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d")
    d1 = datetime.strptime(end, "%Y-%m-%d")
    while d0 <= d1:
        yield d0.strftime("%Y-%m-%d")
        d0 += timedelta(days=1)


def parse_file_ts(name):
    """OKX-...-{start13}-{end13}.7z -> (start_ms, end_ms) 或 None。"""
    import re
    m = re.search(r"(\d{13})-(\d{13})\.7z$", name)
    return (int(m.group(1)), int(m.group(2))) if m else None


def find_trades_files(pool_root: Path, seg_start_ms: int, seg_end_ms: int, inst: str):
    """根据文件名时间戳，跨可能覆盖段范围的日历日收集 Trades 7z。

    段可能跨日，故扫描起始与结束 ts 落在的日历日（含前后各一天缓冲）。
    """
    # 段起止对应的 UTC 日期（datapool 目录按北京日组织，+1 天缓冲覆盖跨日与 8h 偏移）
    start_dt = datetime.utcfromtimestamp(seg_start_ms / 1000)
    end_dt = datetime.utcfromtimestamp(seg_end_ms / 1000)
    # 扫描 [start_dt-1d, end_dt+1d] 的所有目录，避免时区/跨日遗漏
    d = start_dt - timedelta(days=1)
    end_scan = end_dt + timedelta(days=1)
    collected = []
    while d <= end_scan:
        day_dir = pool_root / d.strftime("%Y-%m-%d")
        if day_dir.is_dir():
            for f in sorted(day_dir.glob(f"OKX-Trades-{inst}-*.7z")):
                rng = parse_file_ts(f.name)
                if rng and rng[0] <= seg_end_ms and rng[1] >= seg_start_ms:
                    collected.append(str(f.resolve()))
        d += timedelta(days=1)
    return collected


def convert_one(books_files, trades_files, out_npz, args):
    """根据段规模选择内存版或 mmap 版转换，规避命令行长度限制与超大段内存溢出。

    - books 文件 > --mmap-threshold-files 或时长 > --mmap-threshold-h：用 mmap 版，
      中间事件落盘到 --mmap-tmp-dir，避免 MemoryError / Win 错误 1450。
    - 其余用内存版（速度快）。
    """
    all_files = books_files + trades_files
    use_mmap = (len(books_files) > args.mmap_threshold_files
                or float(args._dur_h) > args.mmap_threshold_h)
    t0 = time.time()
    try:
        if use_mmap:
            args.mmap_tmp_dir.mkdir(parents=True, exist_ok=True)
            convert_mmap(
                all_files,
                output_filename=str(out_npz),
                feed_latency=args.feed_latency,
                base_latency=args.base_latency,
                simulated_latency=None,
                num_processes=args.processes,
                latency_mu=args.latency_mu,
                latency_sigma=args.latency_sigma,
                use_random_latency=args.use_random_latency,
                random_seed=args.random_seed,
                tmp_dir=str(args.mmap_tmp_dir),
            )
            note = "mmap"
        else:
            convert_mem(
                all_files,
                output_filename=str(out_npz),
                feed_latency=args.feed_latency,
                base_latency=args.base_latency,
                simulated_latency=args.feed_latency if args.use_random_latency else None,
                num_processes=args.processes,
                latency_mu=args.latency_mu,
                latency_sigma=args.latency_sigma,
                use_random_latency=args.use_random_latency,
                random_seed=args.random_seed,
            )
            note = "mem"
        return 0, "", note, time.time() - t0
    except Exception as e:
        return 1, "", traceback.format_exc()[-1200:], time.time() - t0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=Path(r"E:\tmp\segments.csv"))
    parser.add_argument("--pool-root", type=Path, default=Path(r"E:\datapool"))
    parser.add_argument("--out-dir", type=Path, default=Path(r"E:\tmp\npz"))
    parser.add_argument("--inst", type=str, default="BTC-USDT-SWAP")
    parser.add_argument("--processes", type=int, default=10)
    parser.add_argument("--min-duration", type=float, default=1.0, help="段最小时长(小时)")
    parser.add_argument("--max-depth-levels", type=int, default=None,
                        help="只保留按价格排名的顶层 N 档（降低 npz 体积）；None=全量")
    parser.add_argument("--use-random-latency", action="store_true",
                        help="数据缺 localTs 时启用对数正态随机延迟")
    parser.add_argument("--feed-latency", type=float, default=200000000)
    parser.add_argument("--base-latency", type=float, default=100000000)
    parser.add_argument("--latency-mu", type=float, default=-9.71)
    parser.add_argument("--latency-sigma", type=float, default=1.0)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--only", type=int, default=None, help="只转换指定 seg_index")
    parser.add_argument("--only-list", type=str, default=None, help="只转换指定 seg_index 列表，逗号分隔")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--mmap-threshold-files", type=int, default=120,
                        help="books 文件数超过此值时改用 mmap 版（避免超大段 MemoryError）")
    parser.add_argument("--mmap-threshold-h", type=float, default=20.0,
                        help="段时长（小时）超过此值时改用 mmap 版")
    parser.add_argument("--mmap-tmp-dir", type=Path, default=Path(r"E:\tmp\mmap"),
                        help="mmap 版临时事件文件目录")
    args = parser.parse_args()
    args._dur_h = 0.0  # 由每次调用前设置
    if args.only_list:
        only_set = {int(x) for x in args.only_list.split(",") if x.strip()}
        args.only_list_set = only_set
    else:
        args.only_list_set = None

    args.out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    with open(args.csv, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)

    # 按段起始日期排序（CSV 中已按 start_ts 排序）
    total_ok = 0
    for r in rows:
        idx = int(r["seg_index"])
        if args.only is not None and idx != args.only:
            continue
        if args.only_list_set is not None and idx not in args.only_list_set:
            continue
        if r["has_snapshot"] != "1":
            continue
        dur_h = float(r["duration_hours"])
        if dur_h < args.min_duration:
            continue
        start_ms = int(r["start_ts"])
        end_ms = int(r["end_ts"])
        books_files = [p for p in r["covered_files"].split(";") if p]
        trades_files = find_trades_files(args.pool_root, start_ms, end_ms, args.inst)
        out_npz = args.out_dir / f"seg_{idx}_{start_ms}.npz"
        if args.skip_existing and out_npz.exists():
            print(f"[SKIP] seg{idx} 已存在: {out_npz.name}")
            total_ok += 1
            continue
        args._dur_h = dur_h
        mode = "mmap" if (len(books_files) > args.mmap_threshold_files or dur_h > args.mmap_threshold_h) else "mem"
        print(f"\n[CONV] seg{idx}: {dur_h:.2f}h books={len(books_files)} trades={len(trades_files)} mode={mode} -> {out_npz.name}")
        rc, out, err, elapsed = convert_one(books_files, trades_files, out_npz, args)
        size_mb = out_npz.stat().st_size / 1e6 if out_npz.exists() else 0
        status = "OK" if rc == 0 else f"FAIL(rc={rc})"
        print(f"  {status} {elapsed:.1f}s {size_mb:.1f}MB")
        if rc != 0:
            print("  STDOUT:", out[-400:])
            print("  STDERR:", err[-1200:])
        else:
            total_ok += 1

    print(f"\n完成: {total_ok} 个段成功")


if __name__ == "__main__":
    main()