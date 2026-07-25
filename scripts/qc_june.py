r"""2026-06 BTC-USDT-SWAP 原始数据质检脚本（Phase 0）。

对 E:\datapool 中每一天的原始 7z 数据做两级质检（只读，不修改任何原始数据）：

1. 文件级（快速）：解析文件名中的 start/end 时间戳，检查 Books/Trades 序列的
   时间覆盖连续性（相邻文件空洞）。
2. 消息级（深度）：复用 ``check_seqId.check_seqId_worker_raw`` 解压 Books 7z，
   校验 seqId/prevSeqId 链连续性（merge_pairs 合并后，起始 prevSeqId != -1 的
   段视为断链缺口）。

产出（写入 --out-dir，默认 E:\tmp\qc）：
- ``2026-06-DD.json``：每日质检报告
- ``summary.csv``：全月汇总
- ``valid_days.txt``：通过质检的日期清单

用法：
    python scripts/qc_june.py --pool-root E:\datapool --out-dir E:\tmp\qc ^
        --start 2026-06-01 --end 2026-06-30 --processes 10
"""
import argparse
import csv
import json
import multiprocessing as mp
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from check_seqId import check_seqId_worker_raw, merge_pairs  # noqa: E402

# 相邻文件 start 与前一文件 end 允许的最大间隙（毫秒），超过视为空洞
FILE_GAP_TOLERANCE_MS = 5_000
# 一天期望的最小覆盖时长（小时），低于视为覆盖不足
MIN_COVERAGE_HOURS = 23.0

TS_PATTERN = re.compile(r"(\d{13})-(\d{13})\.7z$")


def file_ts_range(path: Path):
    m = TS_PATTERN.search(path.name)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def check_filename_continuity(files, tolerance_ms=FILE_GAP_TOLERANCE_MS):
    """检查按文件名 start_ts 排序后的时间空洞。返回 (gaps, first_start, last_end)。"""
    ranges = []
    for f in files:
        s, e = file_ts_range(f)
        if s is not None:
            ranges.append((s, e))
    if not ranges:
        return [], None, None
    ranges.sort()
    gaps = []
    prev_end = ranges[0][1]
    for s, e in ranges[1:]:
        if s > prev_end + tolerance_ms:
            gaps.append({"gap_start_ts": prev_end, "gap_end_ts": s, "gap_ms": s - prev_end})
        prev_end = max(prev_end, e)
    return gaps, ranges[0][0], max(r[1] for r in ranges)


def ts_to_iso(ts_ms):
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%dT%H:%M:%SZ") if ts_ms else None


def qc_one_day(day_dir: Path, date_str: str, inst: str, processes: int) -> dict:
    books_files = sorted(day_dir.glob(f"OKX-Books-{inst}-400-*.7z"))
    trades_files = sorted(day_dir.glob(f"OKX-Trades-{inst}-*.7z"))

    report = {
        "date": date_str,
        "dir": str(day_dir),
        "inst": inst,
        "books_files": len(books_files),
        "trades_files": len(trades_files),
    }

    # ---- 文件级：Books ----
    b_gaps, b_first, b_last = check_filename_continuity(books_files)
    report["books_filename_gaps"] = b_gaps
    report["books_coverage_start"] = ts_to_iso(b_first)
    report["books_coverage_end"] = ts_to_iso(b_last)
    report["books_coverage_hours"] = round((b_last - b_first) / 3_600_000, 2) if b_first else 0.0

    # ---- 文件级：Trades ----
    t_gaps, t_first, t_last = check_filename_continuity(trades_files)
    report["trades_filename_gaps"] = t_gaps
    report["trades_coverage_start"] = ts_to_iso(t_first)
    report["trades_coverage_end"] = ts_to_iso(t_last)
    report["trades_coverage_hours"] = round((t_last - t_first) / 3_600_000, 2) if t_first else 0.0

    # ---- 消息级：Books seqId 链 ----
    seqid_gap_segments = 0
    seqid_chains = 0
    seqid_errors = 0
    if books_files:
        all_segments = []
        with mp.Pool(processes=processes) as pool:
            for res in pool.imap_unordered(check_seqId_worker_raw, books_files):
                if res["status"] != "ok":
                    seqid_errors += 1
                    continue
                all_segments.extend(res.get("segments", []))
                seqid_errors += len(res.get("errors", []))
        all_segments.sort(key=lambda x: x[0])
        merged = merge_pairs(all_segments)
        seqid_chains = len(merged)
        gap_segs = [seg for seg in merged if seg[0] != -1]
        seqid_gap_segments = len(gap_segs)
        report["seqid_gap_details"] = [
            {
                "start_prevSeqId": seg[0],
                "end_seqId": seg[1],
                "start_file": seg[2],
                "start_ts": seg[4] if len(seg) >= 6 else None,
                "end_ts": seg[5] if len(seg) >= 6 else None,
            }
            for seg in gap_segs[:20]  # 最多记录 20 条，防止报告膨胀
        ]
    report["seqid_chains"] = seqid_chains
    report["seqid_gap_segments"] = seqid_gap_segments
    report["seqid_errors"] = seqid_errors

    # ---- 判定 ----
    valid = (
        len(books_files) > 0
        and len(trades_files) > 0
        and not b_gaps
        and not t_gaps
        and seqid_gap_segments == 0
        and seqid_errors == 0
        and report["books_coverage_hours"] >= MIN_COVERAGE_HOURS
        and report["trades_coverage_hours"] >= MIN_COVERAGE_HOURS
    )
    report["valid"] = valid
    return report


def daterange(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d")
    d1 = datetime.strptime(end, "%Y-%m-%d")
    while d0 <= d1:
        yield d0.strftime("%Y-%m-%d")
        d0 += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="2026-06 BTC-USDT-SWAP 原始数据质检")
    parser.add_argument("--pool-root", type=Path, default=Path(r"E:\datapool"))
    parser.add_argument("--out-dir", type=Path, default=Path(r"E:\tmp\qc"))
    parser.add_argument("--start", type=str, default="2026-06-01")
    parser.add_argument("--end", type=str, default="2026-06-30")
    parser.add_argument("--inst", type=str, default="BTC-USDT-SWAP")
    parser.add_argument("--processes", type=int, default=10)
    parser.add_argument(
        "--skip-seqid",
        action="store_true",
        help="只做文件级检查，跳过耗时的 seqId 深度检查",
    )
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    reports = []
    valid_days = []

    for date_str in daterange(args.start, args.end):
        day_dir = args.pool_root / date_str
        if not day_dir.is_dir():
            print(f"[MISS] {date_str}: 目录不存在")
            reports.append({"date": date_str, "valid": False, "error": "dir missing"})
            continue

        print(f"[QC] {date_str} ...", flush=True)
        if args.skip_seqid:
            # 文件级快速检查
            books_files = sorted(day_dir.glob(f"OKX-Books-{args.inst}-400-*.7z"))
            trades_files = sorted(day_dir.glob(f"OKX-Trades-{args.inst}-*.7z"))
            b_gaps, b_first, b_last = check_filename_continuity(books_files)
            t_gaps, t_first, t_last = check_filename_continuity(trades_files)
            report = {
                "date": date_str,
                "inst": args.inst,
                "books_files": len(books_files),
                "trades_files": len(trades_files),
                "books_filename_gaps": b_gaps,
                "trades_filename_gaps": t_gaps,
                "books_coverage_hours": round((b_last - b_first) / 3_600_000, 2) if b_first else 0.0,
                "trades_coverage_hours": round((t_last - t_first) / 3_600_000, 2) if t_first else 0.0,
                "valid": bool(books_files) and bool(trades_files) and not b_gaps and not t_gaps
                and (b_last - b_first) / 3_600_000 >= MIN_COVERAGE_HOURS
                and (t_last - t_first) / 3_600_000 >= MIN_COVERAGE_HOURS,
            }
        else:
            report = qc_one_day(day_dir, date_str, args.inst, args.processes)

        reports.append(report)
        status = "OK" if report.get("valid") else "FAIL"
        print(
            f"  -> {status}: books={report.get('books_files')}, trades={report.get('trades_files')}, "
            f"b_gaps={len(report.get('books_filename_gaps', []))}, "
            f"t_gaps={len(report.get('trades_filename_gaps', []))}, "
            f"seqid_gaps={report.get('seqid_gap_segments', '-')}, "
            f"chains={report.get('seqid_chains', '-')}, "
            f"cover={report.get('books_coverage_hours')}h"
        )
        out_file = args.out_dir / f"{date_str}.json"
        out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        if report.get("valid"):
            valid_days.append(date_str)

    # 汇总
    summary_csv = args.out_dir / "summary.csv"
    keys = [
        "date", "books_files", "trades_files", "books_filename_gaps",
        "trades_filename_gaps", "books_coverage_hours",
        "trades_coverage_hours", "seqid_chains", "seqid_gap_segments",
        "seqid_errors", "valid",
    ]
    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in reports:
            row = {k: r.get(k, "") for k in keys}
            row["books_filename_gaps"] = len(r["books_filename_gaps"]) if isinstance(r.get("books_filename_gaps"), list) else ""
            row["trades_filename_gaps"] = len(r["trades_filename_gaps"]) if isinstance(r.get("trades_filename_gaps"), list) else ""
            writer.writerow(row)

    (args.out_dir / "valid_days.txt").write_text("\n".join(valid_days) + "\n", encoding="utf-8")
    print(f"\n完成: {len(valid_days)}/{len(reports)} 天通过质检")
    print(f"汇总: {summary_csv}")
    print(f"可用日清单: {args.out_dir / 'valid_days.txt'}")


if __name__ == "__main__":
    main()
