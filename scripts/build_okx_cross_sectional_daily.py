"""Build fixed-time daily MarkPrice samples for a frozen swap universe."""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_rl_external_features import (
    file_sha256,
    read_archive,
)
from scripts.screen_okx_swap_liquidity import (
    discover_aligned_daily_archives,
    write_csv,
)


def read_daily_mark(item: tuple[tuple[str, str], dict]) -> dict:
    (instrument, directory_date), row = item
    path = Path(row["path"])
    timestamps, values = read_archive(
        path,
        inst_id=instrument,
        fields=("markPx",),
    )
    cutoff_ms = int(row["cutoff_ms"])
    usable = np.flatnonzero(timestamps <= cutoff_ms)
    if not len(usable):
        raise ValueError(f"no mark samples at cutoff in {path}")
    index = int(usable[np.argmax(timestamps[usable])])
    mark_px = float(values["markPx"][index])
    if not np.isfinite(mark_px) or mark_px <= 0:
        raise ValueError(f"invalid mark price in {path}")
    timestamp_ms = int(timestamps[index])
    return {
        "instrument": instrument,
        "directory_date": directory_date,
        "sample_cutoff_ms": cutoff_ms,
        "timestamp_ms": timestamp_ms,
        "staleness_ms": cutoff_ms - timestamp_ms,
        "mark_px": mark_px,
        "source_path": str(path.resolve()),
        "source_size": path.stat().st_size,
        "source_sha256": file_sha256(path),
    }


def summarize_marks(
    rows: list[dict],
    *,
    expected_days: int,
    max_p90_staleness_ms: float,
) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["instrument"]].append(row)
    result = []
    for instrument, samples in grouped.items():
        staleness = np.asarray(
            [row["staleness_ms"] for row in samples],
            dtype=np.float64,
        )
        p90 = float(np.quantile(staleness, 0.9))
        result.append({
            "instrument": instrument,
            "sample_days": len(samples),
            "coverage_fraction": len(samples) / expected_days,
            "median_staleness_ms": float(np.median(staleness)),
            "p90_staleness_ms": p90,
            "max_staleness_ms": float(np.max(staleness)),
            "qualifies": int(
                len(samples) == expected_days
                and p90 <= max_p90_staleness_ms
            ),
        })
    return sorted(
        result,
        key=lambda row: (
            -row["qualifies"],
            row["p90_staleness_ms"],
        ),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-dir", type=Path, required=True)
    parser.add_argument("--liquidity-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument(
        "--cutoff-hour-utc",
        type=int,
        default=12,
        help="fixed time inside the common MarkPrice archive coverage",
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--max-p90-staleness-ms", type=float, default=60_000
    )
    args = parser.parse_args()

    inventory_metadata = json.loads(
        (args.inventory_dir / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    liquidity_metadata = json.loads(
        (args.liquidity_dir / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    instruments = set(liquidity_metadata["qualified_instruments"])
    if not instruments:
        raise ValueError("liquidity universe is empty")
    daily_paths = discover_aligned_daily_archives(
        args.inventory_dir / "archive_inventory.csv",
        instruments,
        channel="MarkPrice",
        cutoff_hour_utc=args.cutoff_hour_utc,
    )
    print(
        f"instruments={len(instruments)} "
        f"daily_archives={len(daily_paths)}",
        flush=True,
    )
    rows = []
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(read_daily_mark, item): item[0]
            for item in daily_paths.items()
        }
        total = len(futures)
        for completed, future in enumerate(
            as_completed(futures), start=1
        ):
            key = futures[future]
            try:
                rows.append(future.result())
            except Exception as exc:
                failures.append({
                    "instrument": key[0],
                    "directory_date": key[1],
                    "error": repr(exc),
                })
            if completed % 100 == 0 or completed == total:
                print(
                    f"[{completed}/{total}] "
                    f"ok={len(rows)} failed={len(failures)}",
                    flush=True,
                )
    expected_days = int(inventory_metadata["expected_days"])
    summary = summarize_marks(
        rows,
        expected_days=expected_days,
        max_p90_staleness_ms=args.max_p90_staleness_ms,
    )
    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.out_dir / "daily_mark_samples.csv", rows)
    write_csv(args.out_dir / "mark_quality_summary.csv", summary)
    if failures:
        write_csv(args.out_dir / "failures.csv", failures)
    metadata = {
        "daily_panel_schema_version": "okx-daily-mark-panel-v1",
        "inventory_metadata": inventory_metadata,
        "liquidity_metadata_path": str(
            (args.liquidity_dir / "metadata.json").resolve()
        ),
        "cutoff_hour_utc": args.cutoff_hour_utc,
        "instruments": sorted(instruments),
        "daily_archives": len(daily_paths),
        "successful_samples": len(rows),
        "failures": len(failures),
        "max_p90_staleness_ms": args.max_p90_staleness_ms,
        "qualified_instruments": [
            row["instrument"]
            for row in summary
            if row["qualifies"]
        ],
    }
    (args.out_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        f"qualified={len(metadata['qualified_instruments'])}",
        flush=True,
    )
    for item in summary:
        print(
            f"{item['instrument']:<24} "
            f"days={item['sample_days']} "
            f"p90_stale={item['p90_staleness_ms']/1000:.2f}s "
            f"max_stale={item['max_staleness_ms']/1000:.2f}s "
            f"qualified={item['qualifies']}"
        )


if __name__ == "__main__":
    main()
