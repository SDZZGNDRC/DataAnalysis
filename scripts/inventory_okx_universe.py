"""Inventory OKX archive coverage without reading market outcomes.

The inventory operates only on filenames, sizes, and directory dates. It is
intended to define a development universe before any return or strategy
statistics are inspected.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

ARCHIVE_NAME = re.compile(
    r"^OKX-(?P<channel>[^-]+)-(?P<body>.+)-"
    r"(?P<start_ms>\d{13})-(?P<end_ms>\d{13})\.7z$"
)


@dataclass(frozen=True)
class ArchiveName:
    channel: str
    instrument: str
    start_ms: int
    end_ms: int
    depth: int | None = None


def parse_archive_name(name: str) -> ArchiveName | None:
    match = ARCHIVE_NAME.fullmatch(name)
    if match is None:
        return None
    channel = match.group("channel")
    body = match.group("body")
    depth = None
    if channel.lower().startswith("books"):
        instrument, separator, maybe_depth = body.rpartition("-")
        if separator and maybe_depth.isdigit():
            body = instrument
            depth = int(maybe_depth)
    if not body:
        return None
    return ArchiveName(
        channel=channel,
        instrument=body,
        start_ms=int(match.group("start_ms")),
        end_ms=int(match.group("end_ms")),
        depth=depth,
    )


def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def scan_archives(
    pool_root: Path,
    start: date,
    end: date,
) -> list[dict]:
    rows = []
    for day in date_range(start, end):
        directory = pool_root / day.isoformat()
        if not directory.is_dir():
            continue
        with os.scandir(directory) as entries:
            for entry in entries:
                if not entry.is_file():
                    continue
                parsed = parse_archive_name(entry.name)
                if parsed is None:
                    continue
                stat = entry.stat()
                rows.append({
                    "directory_date": day.isoformat(),
                    "channel": parsed.channel,
                    "instrument": parsed.instrument,
                    "depth": parsed.depth or "",
                    "start_ms": parsed.start_ms,
                    "end_ms": parsed.end_ms,
                    "size_bytes": stat.st_size,
                    "path": str(Path(entry.path).resolve()),
                })
    return rows


def summarize(rows: list[dict]) -> list[dict]:
    grouped = defaultdict(lambda: {
        "dates": set(),
        "archives": 0,
        "size_bytes": 0,
        "first_ms": None,
        "last_ms": None,
    })
    for row in rows:
        key = (row["channel"], row["instrument"])
        item = grouped[key]
        item["dates"].add(row["directory_date"])
        item["archives"] += 1
        item["size_bytes"] += row["size_bytes"]
        item["first_ms"] = (
            row["start_ms"]
            if item["first_ms"] is None
            else min(item["first_ms"], row["start_ms"])
        )
        item["last_ms"] = (
            row["end_ms"]
            if item["last_ms"] is None
            else max(item["last_ms"], row["end_ms"])
        )
    result = []
    for (channel, instrument), item in grouped.items():
        days = len(item["dates"])
        result.append({
            "channel": channel,
            "instrument": instrument,
            "directory_days": days,
            "archives": item["archives"],
            "size_bytes": item["size_bytes"],
            "mean_bytes_per_day": (
                item["size_bytes"] / days if days else 0.0
            ),
            "first_ms": item["first_ms"],
            "last_ms": item["last_ms"],
        })
    return sorted(
        result,
        key=lambda item: (
            item["channel"],
            item["instrument"],
        ),
    )


def build_swap_universe(
    summary_rows: list[dict],
    *,
    expected_days: int,
) -> list[dict]:
    lookup = {
        (row["channel"], row["instrument"]): row
        for row in summary_rows
    }
    swap_instruments = sorted({
        row["instrument"]
        for row in summary_rows
        if row["instrument"].endswith("-USDT-SWAP")
    })
    channels = (
        "Books",
        "Trades",
        "Tickers",
        "MarkPrice",
        "OpenInterest",
        "FundingRate",
    )
    result = []
    for instrument in swap_instruments:
        spot = instrument.removesuffix("-SWAP")
        item = {
            "instrument": instrument,
            "spot_instrument": spot,
            "base": spot.removesuffix("-USDT"),
            "expected_days": expected_days,
        }
        core_days = []
        activity_bytes = 0
        for channel in channels:
            row = lookup.get((channel, instrument), {})
            key = channel.lower()
            days = int(row.get("directory_days", 0))
            size = int(row.get("size_bytes", 0))
            item[f"{key}_days"] = days
            item[f"{key}_archives"] = int(row.get("archives", 0))
            item[f"{key}_bytes"] = size
            if channel in {"Books", "Trades"}:
                core_days.append(days)
                activity_bytes += size
        for channel in ("Books", "Trades", "Tickers"):
            row = lookup.get((channel, spot), {})
            key = f"spot_{channel.lower()}"
            item[f"{key}_days"] = int(
                row.get("directory_days", 0)
            )
            item[f"{key}_bytes"] = int(row.get("size_bytes", 0))
        item["core_coverage_fraction"] = (
            min(core_days) / expected_days
            if core_days and expected_days
            else 0.0
        )
        item["activity_bytes_per_expected_day"] = (
            activity_bytes / expected_days
            if expected_days
            else 0.0
        )
        item["has_matching_spot"] = int(
            item["spot_books_days"] > 0
            and item["spot_trades_days"] > 0
        )
        result.append(item)
    return sorted(
        result,
        key=lambda item: item["activity_bytes_per_expected_day"],
        reverse=True,
    )


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        raise ValueError(f"no rows for {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pool-root", type=Path, default=Path(r"E:\datapool")
    )
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.end_date < args.start_date:
        raise ValueError("end date precedes start date")

    rows = scan_archives(
        args.pool_root, args.start_date, args.end_date
    )
    summary_rows = summarize(rows)
    expected_days = (
        args.end_date - args.start_date
    ).days + 1
    universe_rows = build_swap_universe(
        summary_rows, expected_days=expected_days
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.out_dir / "archive_inventory.csv", rows)
    write_csv(args.out_dir / "channel_instrument_summary.csv", summary_rows)
    write_csv(args.out_dir / "usdt_swap_universe.csv", universe_rows)
    metadata = {
        "inventory_schema_version": "okx-universe-inventory-v1",
        "pool_root": str(args.pool_root.resolve()),
        "start_date": args.start_date.isoformat(),
        "end_date": args.end_date.isoformat(),
        "expected_days": expected_days,
        "archives": len(rows),
        "channel_instruments": len(summary_rows),
        "usdt_swap_instruments": len(universe_rows),
        "warning": (
            "Filename/size inventory only; compressed byte activity is "
            "a prefilter, not executable liquidity."
        ),
    }
    (args.out_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(metadata, ensure_ascii=False, indent=2))
    print("top USDT swaps by Books+Trades compressed bytes/day:")
    for rank, item in enumerate(universe_rows[:30], start=1):
        print(
            f"{rank:2d} {item['instrument']:<24} "
            f"coverage={item['core_coverage_fraction']:.1%} "
            f"activity_mb/day="
            f"{item['activity_bytes_per_expected_day'] / 1_000_000:.1f} "
            f"spot={item['has_matching_spot']}"
        )


if __name__ == "__main__":
    main()
