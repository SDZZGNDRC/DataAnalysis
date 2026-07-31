"""Build fixed-time daily OKX IndexTicker samples for swap underlyings."""
from __future__ import annotations

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_rl_external_features import read_archive
from scripts.screen_okx_swap_liquidity import write_csv


def select_daily_index_archives(
    inventory_csv: Path,
    index_instruments: set[str],
    *,
    cutoff_hour_utc: int,
) -> dict[tuple[str, str], dict]:
    selected = {}
    with inventory_csv.open(
        newline="", encoding="utf-8"
    ) as handle:
        for row in csv.DictReader(handle):
            instrument = row["instrument"]
            if instrument not in index_instruments:
                continue
            cutoff_ms = int(
                datetime.fromisoformat(row["directory_date"])
                .replace(
                    hour=cutoff_hour_utc,
                    tzinfo=timezone.utc,
                )
                .timestamp()
                * 1000
            )
            start_ms = int(row["start_ms"])
            if start_ms > cutoff_ms:
                continue
            key = (instrument, row["directory_date"])
            if (
                key not in selected
                or start_ms > int(selected[key]["start_ms"])
            ):
                selected[key] = {
                    **row,
                    "cutoff_ms": cutoff_ms,
                }
    return selected


def read_daily_index(
    item: tuple[tuple[str, str], dict],
) -> dict:
    (index_instrument, directory_date), row = item
    path = Path(row["path"])
    timestamps, values = read_archive(
        path,
        inst_id=index_instrument,
        fields=("idxPx",),
    )
    cutoff_ms = int(row["cutoff_ms"])
    usable = np.flatnonzero(timestamps <= cutoff_ms)
    if not len(usable):
        raise ValueError(f"no index sample at cutoff in {path}")
    index = int(usable[np.argmax(timestamps[usable])])
    index_px = float(values["idxPx"][index])
    if not np.isfinite(index_px) or index_px <= 0:
        raise ValueError(f"invalid index price in {path}")
    timestamp_ms = int(timestamps[index])
    return {
        "instrument": f"{index_instrument}-SWAP",
        "index_instrument": index_instrument,
        "directory_date": directory_date,
        "sample_cutoff_ms": cutoff_ms,
        "timestamp_ms": timestamp_ms,
        "staleness_ms": cutoff_ms - timestamp_ms,
        "index_px": index_px,
        "source_path": str(path.resolve()),
        "source_size": path.stat().st_size,
        "source_sha256": "",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-dir", type=Path, required=True)
    parser.add_argument("--index-inventory", type=Path, required=True)
    parser.add_argument(
        "--universe-metadata", type=Path, required=True
    )
    parser.add_argument(
        "--universe-key", default="qualified_instruments"
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--cutoff-hour-utc", type=int, default=12)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    inventory_metadata = json.loads(
        (args.inventory_dir / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    universe_metadata = json.loads(
        args.universe_metadata.read_text(encoding="utf-8")
    )
    swap_instruments = sorted(
        set(universe_metadata[args.universe_key])
    )
    index_instruments = {
        instrument.removesuffix("-SWAP")
        for instrument in swap_instruments
    }
    selected = select_daily_index_archives(
        args.index_inventory,
        index_instruments,
        cutoff_hour_utc=args.cutoff_hour_utc,
    )
    print(
        f"instruments={len(index_instruments)} "
        f"daily_archives={len(selected)}",
        flush=True,
    )
    rows = []
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(read_daily_index, item): item[0]
            for item in selected.items()
        }
        for completed, future in enumerate(
            as_completed(futures), start=1
        ):
            key = futures[future]
            try:
                rows.append(future.result())
            except Exception as exc:
                failures.append({
                    "index_instrument": key[0],
                    "directory_date": key[1],
                    "error": repr(exc),
                })
            if completed % 100 == 0 or completed == len(futures):
                print(
                    f"[{completed}/{len(futures)}] "
                    f"rows={len(rows)} failures={len(failures)}",
                    flush=True,
                )
    quality = []
    for instrument in swap_instruments:
        samples = [
            row for row in rows
            if row["instrument"] == instrument
        ]
        staleness = np.asarray(
            [row["staleness_ms"] for row in samples],
            dtype=np.float64,
        )
        quality.append({
            "instrument": instrument,
            "sample_days": len(samples),
            "coverage_fraction": (
                len(samples)
                / int(inventory_metadata["expected_days"])
            ),
            "p90_staleness_ms": (
                float(np.quantile(staleness, 0.9))
                if len(staleness)
                else ""
            ),
            "max_staleness_ms": (
                float(staleness.max()) if len(staleness) else ""
            ),
        })
    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.out_dir / "daily_index_samples.csv", rows)
    write_csv(args.out_dir / "index_quality.csv", quality)
    if failures:
        write_csv(args.out_dir / "failures.csv", failures)
    metadata = {
        "index_panel_schema_version": "okx-daily-index-panel-v1",
        "inventory_metadata": inventory_metadata,
        "universe_metadata": str(
            args.universe_metadata.resolve()
        ),
        "instruments": swap_instruments,
        "cutoff_hour_utc": args.cutoff_hour_utc,
        "successful_samples": len(rows),
        "failures": failures,
        "source_hashes_complete": False,
    }
    (args.out_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
