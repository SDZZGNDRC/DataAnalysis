"""Build synchronized daily OI and funding samples for OKX swaps."""
from __future__ import annotations

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_rl_external_features import read_archive
from scripts.screen_okx_swap_liquidity import write_csv


def daily_cutoffs(
    start: date,
    end: date,
    *,
    cutoff_hour_utc: int,
) -> list[tuple[str, int]]:
    result = []
    current = start
    while current <= end:
        cutoff = datetime.combine(
            current,
            datetime.min.time(),
            tzinfo=timezone.utc,
        ).replace(hour=cutoff_hour_utc)
        result.append((
            current.isoformat(),
            int(cutoff.timestamp() * 1000),
        ))
        current += timedelta(days=1)
    return result


def last_before_indices(
    timestamps_ms: np.ndarray,
    cutoffs_ms: np.ndarray,
) -> np.ndarray:
    if timestamps_ms.ndim != 1 or cutoffs_ms.ndim != 1:
        raise ValueError("timestamps and cutoffs must be one-dimensional")
    return np.searchsorted(
        timestamps_ms, cutoffs_ms, side="right"
    ) - 1


def read_inventory(
    path: Path,
    instruments: set[str],
) -> dict[str, list[dict]]:
    grouped = {instrument: [] for instrument in instruments}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            instrument = row["instrument"]
            if instrument in grouped:
                grouped[instrument].append(row)
    return grouped


def load_series(
    rows: list[dict],
    *,
    instrument: str,
    fields: tuple[str, ...],
) -> tuple[np.ndarray, dict[str, np.ndarray], list[dict]]:
    timestamps = []
    values = {field: [] for field in fields}
    sources = []
    seen = set()
    for row in sorted(rows, key=lambda item: int(item["start_ms"])):
        path = Path(row["path"])
        if path in seen:
            continue
        seen.add(path)
        archive_timestamps, archive_values = read_archive(
            path,
            inst_id=instrument,
            fields=fields,
        )
        if len(archive_timestamps):
            timestamps.append(archive_timestamps)
            for field in fields:
                values[field].append(archive_values[field])
        sources.append({
            "instrument": instrument,
            "channel": row["channel"],
            "path": str(path.resolve()),
            "size_bytes": int(row["size_bytes"]),
            "start_ms": int(row["start_ms"]),
            "end_ms": int(row["end_ms"]),
        })
    if not timestamps:
        return (
            np.empty(0, dtype=np.int64),
            {
                field: np.empty(0, dtype=np.float64)
                for field in fields
            },
            sources,
        )
    combined_timestamps = np.concatenate(timestamps)
    combined_values = {
        field: np.concatenate(values[field])
        for field in fields
    }
    order = np.argsort(combined_timestamps, kind="stable")
    combined_timestamps = combined_timestamps[order]
    combined_values = {
        field: field_values[order]
        for field, field_values in combined_values.items()
    }
    keep = np.r_[
        combined_timestamps[1:] != combined_timestamps[:-1],
        True,
    ]
    return (
        combined_timestamps[keep],
        {
            field: field_values[keep]
            for field, field_values in combined_values.items()
        },
        sources,
    )


def build_instrument_samples(
    instrument: str,
    oi_rows: list[dict],
    funding_rows: list[dict],
    cutoffs: list[tuple[str, int]],
) -> tuple[list[dict], list[dict]]:
    oi_ts, oi_values, oi_sources = load_series(
        oi_rows,
        instrument=instrument,
        fields=("oi", "oiCcy", "oiUsd"),
    )
    funding_ts, funding_values, funding_sources = load_series(
        funding_rows,
        instrument=instrument,
        fields=("fundingRate",),
    )
    cutoff_values = np.asarray(
        [cutoff for _, cutoff in cutoffs], dtype=np.int64
    )
    oi_indices = last_before_indices(oi_ts, cutoff_values)
    funding_indices = last_before_indices(funding_ts, cutoff_values)
    rows = []
    for position, (directory_date, cutoff_ms) in enumerate(cutoffs):
        oi_index = int(oi_indices[position])
        funding_index = int(funding_indices[position])
        if oi_index < 0 or funding_index < 0:
            continue
        oi = float(oi_values["oi"][oi_index])
        oi_ccy = float(oi_values["oiCcy"][oi_index])
        oi_usd = float(oi_values["oiUsd"][oi_index])
        funding_rate = float(
            funding_values["fundingRate"][funding_index]
        )
        if not (
            np.isfinite(oi)
            and np.isfinite(oi_ccy)
            and np.isfinite(oi_usd)
            and np.isfinite(funding_rate)
            and oi > 0
            and oi_ccy > 0
            and oi_usd > 0
        ):
            continue
        rows.append({
            "instrument": instrument,
            "directory_date": directory_date,
            "sample_cutoff_ms": cutoff_ms,
            "oi_timestamp_ms": int(oi_ts[oi_index]),
            "oi_staleness_ms": (
                cutoff_ms - int(oi_ts[oi_index])
            ),
            "oi": oi,
            "oi_ccy": oi_ccy,
            "oi_usd": oi_usd,
            "funding_timestamp_ms": int(
                funding_ts[funding_index]
            ),
            "funding_staleness_ms": (
                cutoff_ms - int(funding_ts[funding_index])
            ),
            "funding_rate": funding_rate,
        })
    return rows, oi_sources + funding_sources


def summarize_quality(
    rows: list[dict],
    instruments: list[str],
    *,
    expected_days: int,
) -> list[dict]:
    result = []
    for instrument in instruments:
        samples = [
            row for row in rows
            if row["instrument"] == instrument
        ]
        if not samples:
            result.append({
                "instrument": instrument,
                "sample_days": 0,
                "coverage_fraction": 0.0,
                "p90_oi_staleness_ms": "",
                "max_oi_staleness_ms": "",
                "p90_funding_staleness_ms": "",
                "max_funding_staleness_ms": "",
            })
            continue
        oi_staleness = np.asarray(
            [row["oi_staleness_ms"] for row in samples],
            dtype=np.float64,
        )
        funding_staleness = np.asarray(
            [row["funding_staleness_ms"] for row in samples],
            dtype=np.float64,
        )
        result.append({
            "instrument": instrument,
            "sample_days": len(samples),
            "coverage_fraction": len(samples) / expected_days,
            "p90_oi_staleness_ms": float(
                np.quantile(oi_staleness, 0.9)
            ),
            "max_oi_staleness_ms": float(oi_staleness.max()),
            "p90_funding_staleness_ms": float(
                np.quantile(funding_staleness, 0.9)
            ),
            "max_funding_staleness_ms": float(
                funding_staleness.max()
            ),
        })
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-dir", type=Path, required=True)
    parser.add_argument(
        "--open-interest-inventory", type=Path, required=True
    )
    parser.add_argument(
        "--funding-inventory", type=Path, required=True
    )
    parser.add_argument(
        "--universe-metadata", type=Path, required=True
    )
    parser.add_argument(
        "--universe-key", default="qualified_instruments"
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--cutoff-hour-utc", type=int, default=12)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    inventory_metadata = json.loads(
        (args.inventory_dir / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    universe_metadata = json.loads(
        args.universe_metadata.read_text(encoding="utf-8")
    )
    instruments = sorted(
        set(universe_metadata[args.universe_key])
    )
    cutoffs = daily_cutoffs(
        date.fromisoformat(inventory_metadata["start_date"]),
        date.fromisoformat(inventory_metadata["end_date"]),
        cutoff_hour_utc=args.cutoff_hour_utc,
    )
    oi_inventory = read_inventory(
        args.open_interest_inventory, set(instruments)
    )
    funding_inventory = read_inventory(
        args.funding_inventory, set(instruments)
    )
    print(
        f"instruments={len(instruments)} days={len(cutoffs)}",
        flush=True,
    )
    rows = []
    sources = []
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                build_instrument_samples,
                instrument,
                oi_inventory[instrument],
                funding_inventory[instrument],
                cutoffs,
            ): instrument
            for instrument in instruments
        }
        for completed, future in enumerate(
            as_completed(futures), start=1
        ):
            instrument = futures[future]
            try:
                instrument_rows, instrument_sources = future.result()
                rows.extend(instrument_rows)
                sources.extend(instrument_sources)
            except Exception as exc:
                failures.append({
                    "instrument": instrument,
                    "error": repr(exc),
                })
            print(
                f"[{completed}/{len(futures)}] "
                f"rows={len(rows)} failures={len(failures)}",
                flush=True,
            )
    quality = summarize_quality(
        rows,
        instruments,
        expected_days=len(cutoffs),
    )
    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.out_dir / "daily_derivatives_samples.csv", rows)
    write_csv(args.out_dir / "derivatives_quality.csv", quality)
    write_csv(args.out_dir / "source_manifest.csv", sources)
    if failures:
        write_csv(args.out_dir / "failures.csv", failures)
    metadata = {
        "derivatives_panel_schema_version": (
            "okx-daily-derivatives-panel-v1"
        ),
        "inventory_metadata": inventory_metadata,
        "universe_metadata": str(
            args.universe_metadata.resolve()
        ),
        "instruments": instruments,
        "cutoff_hour_utc": args.cutoff_hour_utc,
        "successful_samples": len(rows),
        "failures": failures,
        "source_hashes_complete": False,
        "warning": (
            "Source paths/sizes/ranges are recorded; SHA-256 is "
            "deferred until a candidate passes development screening."
        ),
    }
    (args.out_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
