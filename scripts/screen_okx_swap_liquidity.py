"""Screen a development-only OKX swap universe using daily ticker samples."""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_rl_external_features import (
    file_sha256,
    read_archive,
)

TICKER_FIELDS = (
    "last",
    "bidPx",
    "askPx",
    "bidSz",
    "askSz",
    "volCcy24h",
    "vol24h",
)


def select_prefilter_universe(
    universe_rows: list[dict],
    *,
    min_core_coverage: float,
    min_ticker_days: int,
    min_mark_days: int,
    top_n: int,
    excluded: set[str],
    controls: set[str],
) -> list[str]:
    eligible = [
        row
        for row in universe_rows
        if float(row["core_coverage_fraction"])
        >= min_core_coverage
        and int(row["tickers_days"]) >= min_ticker_days
        and int(row["markprice_days"]) >= min_mark_days
        and row["instrument"] not in excluded
    ]
    eligible.sort(
        key=lambda row: float(
            row["activity_bytes_per_expected_day"]
        ),
        reverse=True,
    )
    selected = [row["instrument"] for row in eligible[:top_n]]
    for instrument in sorted(controls):
        if instrument not in selected:
            selected.append(instrument)
    return selected


def discover_aligned_daily_archives(
    inventory_csv: Path,
    instruments: set[str],
    *,
    channel: str,
    cutoff_hour_utc: int = 15,
) -> dict[tuple[str, str], dict]:
    if not 0 <= cutoff_hour_utc <= 23:
        raise ValueError("cutoff hour must be between 0 and 23")
    candidates = defaultdict(lambda: defaultdict(list))
    with inventory_csv.open(
        newline="", encoding="utf-8"
    ) as handle:
        for row in csv.DictReader(handle):
            if (
                row["channel"] != channel
                or row["instrument"] not in instruments
            ):
                continue
            candidates[row["directory_date"]][
                row["instrument"]
            ].append(row)

    selected = {}
    for directory_date, by_instrument in candidates.items():
        if set(by_instrument) != instruments:
            continue
        cutoff_ms = int(
            datetime.fromisoformat(directory_date)
            .replace(
                hour=cutoff_hour_utc,
                tzinfo=timezone.utc,
            )
            .timestamp()
            * 1000
        )
        for instrument, rows in by_instrument.items():
            eligible = [
                row
                for row in rows
                if int(row["start_ms"]) <= cutoff_ms
            ]
            if eligible:
                row = max(
                    eligible,
                    key=lambda item: int(item["start_ms"]),
                )
                key = (instrument, directory_date)
                selected[key] = {
                    **row,
                    "cutoff_ms": cutoff_ms,
                }
    return selected


def discover_aligned_daily_ticker_samples(
    inventory_csv: Path,
    instruments: set[str],
    *,
    cutoff_hour_utc: int = 15,
) -> dict[tuple[str, str], dict]:
    return discover_aligned_daily_archives(
        inventory_csv,
        instruments,
        channel="Tickers",
        cutoff_hour_utc=cutoff_hour_utc,
    )


def read_daily_ticker(
    item: tuple[tuple[str, str], dict],
    *,
    hash_source: bool = True,
) -> dict:
    (instrument, directory_date), row = item
    path = Path(row["path"])
    timestamps, values = read_archive(
        path,
        inst_id=instrument,
        fields=TICKER_FIELDS,
    )
    cutoff_ms = int(row["cutoff_ms"])
    usable = np.flatnonzero(timestamps <= cutoff_ms)
    if not len(usable):
        raise ValueError(f"no ticker samples in {path}")
    index = int(usable[np.argmax(timestamps[usable])])
    ticker = {
        field: float(field_values[index])
        for field, field_values in values.items()
    }
    bid = ticker["bidPx"]
    ask = ticker["askPx"]
    last = ticker["last"]
    if not (
        math.isfinite(bid)
        and math.isfinite(ask)
        and math.isfinite(last)
        and bid > 0
        and ask >= bid
        and last > 0
    ):
        raise ValueError(f"invalid ticker prices in {path}")
    mid = 0.5 * (bid + ask)
    quote_volume_24h = ticker["volCcy24h"] * last
    return {
        "instrument": instrument,
        "directory_date": directory_date,
        "sample_cutoff_ms": cutoff_ms,
        "timestamp_ms": int(timestamps[index]),
        "staleness_ms": cutoff_ms - int(timestamps[index]),
        "last": last,
        "bid": bid,
        "ask": ask,
        "spread_bps": (ask / bid - 1.0) * 10_000,
        "bid_size_raw": ticker["bidSz"],
        "ask_size_raw": ticker["askSz"],
        "base_volume_24h": ticker["volCcy24h"],
        "contract_volume_24h": ticker["vol24h"],
        "quote_volume_24h": quote_volume_24h,
        "source_path": str(path.resolve()),
        "source_size": path.stat().st_size,
        "source_sha256": (
            file_sha256(path) if hash_source else ""
        ),
    }


def summarize_daily_samples(
    rows: list[dict],
    *,
    expected_days: int,
    min_sample_days: int,
    min_median_quote_volume: float,
    max_p90_spread_bps: float,
    max_p90_staleness_ms: float,
) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["instrument"]].append(row)
    result = []
    for instrument, samples in grouped.items():
        spreads = np.asarray(
            [row["spread_bps"] for row in samples],
            dtype=np.float64,
        )
        volumes = np.asarray(
            [row["quote_volume_24h"] for row in samples],
            dtype=np.float64,
        )
        staleness = np.asarray(
            [row["staleness_ms"] for row in samples],
            dtype=np.float64,
        )
        median_spread = float(np.median(spreads))
        p90_spread = float(np.quantile(spreads, 0.9))
        median_volume = float(np.median(volumes))
        p90_staleness = float(np.quantile(staleness, 0.9))
        qualifies = (
            len(samples) >= min_sample_days
            and median_volume >= min_median_quote_volume
            and p90_spread <= max_p90_spread_bps
            and p90_staleness <= max_p90_staleness_ms
        )
        result.append({
            "instrument": instrument,
            "sample_days": len(samples),
            "coverage_fraction": (
                len(samples) / expected_days if expected_days else 0.0
            ),
            "median_quote_volume_24h": median_volume,
            "p10_quote_volume_24h": float(
                np.quantile(volumes, 0.1)
            ),
            "median_spread_bps": median_spread,
            "p90_spread_bps": p90_spread,
            "p99_spread_bps": float(np.quantile(spreads, 0.99)),
            "median_staleness_ms": float(np.median(staleness)),
            "p90_staleness_ms": p90_staleness,
            "liquidity_score": (
                median_volume / max(p90_spread, 0.01)
            ),
            "qualifies": int(qualifies),
        })
    return sorted(
        result,
        key=lambda row: row["liquidity_score"],
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


def _read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _parse_set(raw: str) -> set[str]:
    return {
        item.strip()
        for item in raw.split(",")
        if item.strip()
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-dir", type=Path, required=True)
    parser.add_argument(
        "--inventory-csv",
        type=Path,
        help="optional per-channel inventory index",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--min-core-coverage", type=float, default=0.90)
    parser.add_argument("--min-ticker-days", type=int, default=55)
    parser.add_argument("--min-mark-days", type=int, default=55)
    parser.add_argument("--min-sample-days", type=int, default=50)
    parser.add_argument(
        "--min-median-quote-volume",
        type=float,
        default=10_000_000,
    )
    parser.add_argument(
        "--max-p90-spread-bps", type=float, default=10.0
    )
    parser.add_argument(
        "--max-p90-staleness-ms", type=float, default=60_000
    )
    parser.add_argument(
        "--exclude",
        default="BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP",
    )
    parser.add_argument(
        "--controls",
        default="BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP",
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--skip-source-hash",
        action="store_true",
        help="defer SHA-256 until a candidate passes quality screening",
    )
    parser.add_argument(
        "--cutoff-hour-utc",
        type=int,
        default=15,
        help="15:00 UTC is 23:00 Asia/Shanghai for date-named folders",
    )
    args = parser.parse_args()
    if args.top_n <= 0 or args.workers <= 0:
        raise ValueError("top-n and workers must be positive")

    metadata = json.loads(
        (args.inventory_dir / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    universe_rows = _read_csv(
        args.inventory_dir / "usdt_swap_universe.csv"
    )
    selected = select_prefilter_universe(
        universe_rows,
        min_core_coverage=args.min_core_coverage,
        min_ticker_days=args.min_ticker_days,
        min_mark_days=args.min_mark_days,
        top_n=args.top_n,
        excluded=_parse_set(args.exclude),
        controls=_parse_set(args.controls),
    )
    daily_paths = discover_aligned_daily_ticker_samples(
        (
            args.inventory_csv
            if args.inventory_csv is not None
            else args.inventory_dir / "archive_inventory.csv"
        ),
        set(selected),
        cutoff_hour_utc=args.cutoff_hour_utc,
    )
    print(
        f"selected={len(selected)} daily_archives={len(daily_paths)}",
        flush=True,
    )

    rows = []
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                read_daily_ticker,
                item,
                hash_source=not args.skip_source_hash,
            ): item[0]
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

    summary = summarize_daily_samples(
        rows,
        expected_days=int(metadata["expected_days"]),
        min_sample_days=args.min_sample_days,
        min_median_quote_volume=args.min_median_quote_volume,
        max_p90_spread_bps=args.max_p90_spread_bps,
        max_p90_staleness_ms=args.max_p90_staleness_ms,
    )
    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.out_dir / "daily_ticker_samples.csv", rows)
    write_csv(args.out_dir / "liquidity_summary.csv", summary)
    if failures:
        write_csv(args.out_dir / "failures.csv", failures)
    output = {
        "liquidity_screen_schema_version": "okx-swap-liquidity-v1",
        "inventory_metadata": metadata,
        "selected_prefilter_instruments": selected,
        "daily_archives": len(daily_paths),
        "successful_samples": len(rows),
        "failures": len(failures),
        "criteria": {
            "min_core_coverage": args.min_core_coverage,
            "min_ticker_days": args.min_ticker_days,
            "min_mark_days": args.min_mark_days,
            "top_n": args.top_n,
            "cutoff_hour_utc": args.cutoff_hour_utc,
            "min_sample_days": args.min_sample_days,
            "min_median_quote_volume": (
                args.min_median_quote_volume
            ),
            "max_p90_spread_bps": args.max_p90_spread_bps,
            "max_p90_staleness_ms": args.max_p90_staleness_ms,
            "excluded": sorted(_parse_set(args.exclude)),
            "controls": sorted(_parse_set(args.controls)),
            "source_hashes_complete": not args.skip_source_hash,
        },
        "qualified_instruments": [
            row["instrument"]
            for row in summary
            if row["qualifies"]
        ],
    }
    (args.out_dir / "metadata.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        f"qualified={len(output['qualified_instruments'])}",
        flush=True,
    )
    for rank, item in enumerate(summary, start=1):
        print(
            f"{rank:2d} {item['instrument']:<24} "
            f"days={item['sample_days']:2d} "
            f"median_vol=${item['median_quote_volume_24h']/1e6:,.1f}m "
            f"p90_spread={item['p90_spread_bps']:.2f}bp "
            f"p90_stale={item['p90_staleness_ms']/1000:.1f}s "
            f"qualified={item['qualifies']}"
        )


if __name__ == "__main__":
    main()
