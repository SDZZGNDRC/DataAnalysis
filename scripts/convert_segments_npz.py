r"""将 seqId 会话段转换为 hftbacktest npz（Phase 1）。

读取 build_june_segments.py 生成的 segments.csv，为每个（snapshot 头、时长≥阈值）
的段配对时间范围内的 Trades 7z，调用 hftbacktest_okx.py 生成 npz。

转换器会在解析阶段严格过滤到 segment 的闭区间，并在输出后校验事件边界和
snapshot。当前不支持在保持增量盘口正确性的前提下裁剪深度档位。

用法：
    python scripts/convert_segments_npz.py --csv E:\tmp\segments.csv ^
        --pool-root E:\datapool --out-dir E:\tmp\npz_v2 --processes 10 ^
        --min-duration 1.0 --skip-existing

注意：E:\\datapool 只读；本脚本通过子进程调用 hftbacktest_okx.py，仅在 E:\\tmp 写出 npz。
"""
import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hftbacktest_okx import convert as convert_mem  # 内存版，直接传入文件列表，规避命令行长度限制
from hftbacktest_okx_mmap import convert as convert_mmap  # mmap 版，用于超大段（事件千万级），避免内存爆/系统资源耗尽
from hftbacktest.types import (
    DEPTH_CLEAR_EVENT,
    DEPTH_EVENT,
    DEPTH_SNAPSHOT_EVENT,
)

SEGMENT_SCHEMA_VERSION = "exact-segment-v2"


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
    start_dt = datetime.fromtimestamp(seg_start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(seg_end_ms / 1000, tz=timezone.utc)
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


def _validate_segment_data(data, start_ms: int, end_ms: int):
    if len(data) == 0:
        raise ValueError("转换结果为空")
    start_ns = int(start_ms) * 1_000_000
    end_ns = int(end_ms) * 1_000_000
    min_ts = int(data["exch_ts"].min())
    max_ts = int(data["exch_ts"].max())
    if min_ts < start_ns or max_ts > end_ns:
        raise ValueError(
            f"转换结果越界: [{min_ts}, {max_ts}] not in [{start_ns}, {end_ns}]"
        )
    snapshot_mask = (data["ev"] & DEPTH_SNAPSHOT_EVENT) != 0
    if not snapshot_mask.any():
        raise ValueError("精确切段后没有 snapshot，不能作为独立回测段")
    first_snapshot_ns = int(data["exch_ts"][snapshot_mask].min())
    if first_snapshot_ns != start_ns:
        raise ValueError(
            f"首个 snapshot={first_snapshot_ns}，不等于 segment start={start_ns}"
        )
    depth_mask = (
        ((data["ev"] & DEPTH_EVENT) != 0)
        | ((data["ev"] & DEPTH_CLEAR_EVENT) != 0)
        | snapshot_mask
    )
    if int(data["exch_ts"][depth_mask].min()) < first_snapshot_ns:
        raise ValueError("首个 snapshot 前仍有深度 update，订单簿无法独立重建")
    return {
        "schema_version": SEGMENT_SCHEMA_VERSION,
        "requested_start_ms": int(start_ms),
        "requested_end_ms": int(end_ms),
        "actual_start_ns": min_ts,
        "actual_end_ns": max_ts,
        "first_snapshot_ns": first_snapshot_ns,
        "event_count": int(len(data)),
    }


def _metadata_path(npz_path: Path) -> Path:
    return npz_path.with_suffix(".meta.json")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_metadata(npz_path: Path, metadata: dict):
    metadata = dict(metadata)
    metadata["npz_size"] = int(npz_path.stat().st_size)
    metadata["npz_sha256"] = _file_sha256(npz_path)
    _metadata_path(npz_path).write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )


def _existing_segment_is_valid(npz_path: Path, start_ms: int, end_ms: int) -> bool:
    meta_path = _metadata_path(npz_path)
    if meta_path.exists():
        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
            basic_metadata_matches = (
                metadata.get("schema_version") == SEGMENT_SCHEMA_VERSION
                and metadata.get("requested_start_ms") == int(start_ms)
                and metadata.get("requested_end_ms") == int(end_ms)
                and metadata.get("npz_size") == int(npz_path.stat().st_size)
            )
            if basic_metadata_matches and metadata.get("npz_sha256"):
                if metadata["npz_sha256"] == _file_sha256(npz_path):
                    return True
                print(f"[STALE] {npz_path.name}: SHA-256 mismatch")
                return False
        except Exception:
            pass
    try:
        with np.load(npz_path) as source:
            metadata = _validate_segment_data(source["data"], start_ms, end_ms)
        _write_metadata(npz_path, metadata)
        return True
    except Exception as exc:
        print(f"[STALE] {npz_path.name}: {exc}")
        return False


def convert_one(books_files, trades_files, out_npz, args):
    """根据段规模选择内存版或 mmap 版转换，规避命令行长度限制与超大段内存溢出。

    - books 文件 > --mmap-threshold-files 或时长 > --mmap-threshold-h：用 mmap 版，
      中间事件落盘到 --mmap-tmp-dir，避免 MemoryError / Win 错误 1450。
    - 其余用内存版（速度快）。
    """
    out_npz = Path(out_npz)
    partial_npz = out_npz.with_name(f"{out_npz.stem}.partial.npz")
    all_files = books_files + trades_files
    use_mmap = (len(books_files) > args.mmap_threshold_files
                or float(args._dur_h) > args.mmap_threshold_h)
    t0 = time.time()
    try:
        partial_npz.unlink(missing_ok=True)
        if use_mmap:
            args.mmap_tmp_dir.mkdir(parents=True, exist_ok=True)
            data = convert_mmap(
                all_files,
                output_filename=str(partial_npz),
                feed_latency=args.feed_latency,
                base_latency=args.base_latency,
                simulated_latency=None,
                num_processes=args.processes,
                latency_mu=args.latency_mu,
                latency_sigma=args.latency_sigma,
                use_random_latency=args.use_random_latency,
                random_seed=args.random_seed,
                tmp_dir=str(args.mmap_tmp_dir),
                start_ts=args._start_ms,
                end_ts=args._end_ms,
            )
            note = "mmap"
        else:
            data = convert_mem(
                all_files,
                output_filename=str(partial_npz),
                feed_latency=args.feed_latency,
                base_latency=args.base_latency,
                simulated_latency=args.feed_latency if args.use_random_latency else None,
                num_processes=args.processes,
                latency_mu=args.latency_mu,
                latency_sigma=args.latency_sigma,
                use_random_latency=args.use_random_latency,
                random_seed=args.random_seed,
                start_ts=args._start_ms,
                end_ts=args._end_ms,
            )
            note = "mem"
        # Validate the persisted archive, not only its in-memory source. This
        # forces ZIP decompression and CRC verification before publication.
        del data
        with np.load(partial_npz) as source:
            metadata = _validate_segment_data(
                source["data"], args._start_ms, args._end_ms
            )
        os.replace(partial_npz, out_npz)
        _write_metadata(out_npz, metadata)
        return 0, "", note, time.time() - t0
    except Exception as e:
        return 1, "", traceback.format_exc()[-1200:], time.time() - t0
    finally:
        partial_npz.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=Path(r"E:\tmp\segments.csv"))
    parser.add_argument("--pool-root", type=Path, default=Path(r"E:\datapool"))
    parser.add_argument("--out-dir", type=Path, default=Path(r"E:\tmp\npz_v2"))
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
    args._start_ms = 0
    args._end_ms = 0
    if args.max_depth_levels is not None:
        parser.error(
            "--max-depth-levels 当前转换器未实现，不能静默忽略；"
            "请移除此参数，或先实现保持增量盘口正确性的深度裁剪。"
        )
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
        if (
            args.skip_existing
            and out_npz.exists()
            and _existing_segment_is_valid(out_npz, start_ms, end_ms)
        ):
            print(f"[SKIP] seg{idx} 已存在: {out_npz.name}")
            total_ok += 1
            continue
        args._dur_h = dur_h
        args._start_ms = start_ms
        args._end_ms = end_ms
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
