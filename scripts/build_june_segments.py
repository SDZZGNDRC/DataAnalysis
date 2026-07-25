r"""构建 2026-06 BTC-USDT-SWAP 的 seqId 会话段（Phase 1）。

跨 30 天收集 Books-400 7z，复用 ``reconstruct_segments.process_file`` +
``merge_pairs`` + ``decode_bitmap``，输出每个 session 的覆盖文件与时间范围。

每个 session 从一个 snapshot（prevSeqId=-1 链头）开始，到下一次断链结束，
天然满足 ``hftbacktest_okx.py`` 对事件流首部 snapshot 的要求。

用法：
    python scripts/build_june_segments.py --pool-root E:\datapool ^
        --start 2026-06-01 --end 2026-06-30 --inst BTC-USDT-SWAP ^
        --processes 10 --output E:\tmp\segments.csv

注意：E:\\datapool 只读，本脚本只读取 7z，不写入任何原始数据。
"""
import argparse
import csv
import multiprocessing as mp
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from reconstruct_segments import process_file, merge_pairs, decode_bitmap  # noqa: E402


def daterange(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d")
    d1 = datetime.strptime(end, "%Y-%m-%d")
    while d0 <= d1:
        yield d0.strftime("%Y-%m-%d")
        d0 += timedelta(days=1)


def ts_to_dt(ts_ms):
    if ts_ms is None:
        return ""
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    parser = argparse.ArgumentParser(description="构建 BTC-USDT-SWAP seqId 会话段")
    parser.add_argument("--pool-root", type=Path, default=Path(r"E:\datapool"))
    parser.add_argument("--start", type=str, default="2026-06-01")
    parser.add_argument("--end", type=str, default="2026-06-30")
    parser.add_argument("--inst", type=str, default="BTC-USDT-SWAP")
    parser.add_argument("--processes", type=int, default=10)
    parser.add_argument("--output", type=Path, default=Path(r"E:\tmp\segments.csv"))
    args = parser.parse_args()

    # 收集所有 June 的 BTC-USDT-SWAP Books-400 7z（按日历日再按文件名排序）
    books_files = []
    for date_str in daterange(args.start, args.end):
        day_dir = args.pool_root / date_str
        if not day_dir.is_dir():
            continue
        books_files.extend(sorted(day_dir.glob(f"OKX-Books-{args.inst}-400-*.7z")))
    books_files = [str(p.resolve()) for p in books_files]
    print(f"收集到 {len(books_files)} 个 Books 7z 文件")

    if not books_files:
        sys.exit("未找到任何 Books 文件")

    # 多进程解析每个文件的 seqId 链
    worker_args = [(Path(f), i) for i, f in enumerate(books_files)]
    all_segments = []
    with mp.Pool(processes=args.processes) as pool:
        for res in pool.imap_unordered(process_file, worker_args):
            if res["status"] != "ok":
                print(f"[WARN] 处理 {res['file']} 失败: {res.get('msg')}")
                continue
            all_segments.extend(res.get("segments", []))

    print(f"收集到 {len(all_segments)} 个初始段，开始全局合并...")
    all_segments.sort(key=lambda x: x[0])
    final_segments = merge_pairs(all_segments)
    final_segments.sort(key=lambda x: x[3] if x[3] is not None else 0)
    print(f"合并后得到 {len(final_segments)} 个会话段")

    # 写 CSV
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "seg_index", "prevSeqId", "seqId", "startSeqId",
            "start_ts", "start_datetime", "end_ts", "end_datetime",
            "duration_hours", "num_files", "has_snapshot", "covered_files",
        ])
        for i, seg in enumerate(final_segments):
            p, s, bitmap, start_ts, end_ts, start_seq = seg
            covered = decode_bitmap(bitmap, books_files)
            n_files = covered.count(";") + 1 if covered else 0
            duration_h = ((end_ts - start_ts) / 3_600_000) if (start_ts and end_ts) else 0.0
            writer.writerow([
                i, p, s, start_seq,
                start_ts, ts_to_dt(start_ts),
                end_ts, ts_to_dt(end_ts),
                f"{duration_h:.4f}", n_files, int(p == -1), covered,
            ])

    # 汇总
    snap_segs = [seg for seg in final_segments if seg[0] == -1]
    total_h = sum(((s[4] - s[3]) / 3_600_000) for s in final_segments if s[3] and s[4])
    print(f"\n汇总: {len(final_segments)} 段, 其中 snapshot 头段 {len(snap_segs)} 个")
    print(f"      总覆盖时长 {total_h:.1f} 小时 (约 {total_h/24:.1f} 天)")
    print(f"输出: {args.output}")


if __name__ == "__main__":
    main()