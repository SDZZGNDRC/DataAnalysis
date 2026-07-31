"""Materialize per-channel indexes from a large OKX archive inventory.

The inventory CSV can contain millions of rows.  Repeated Python ``csv``
scans are needlessly expensive, while ripgrep can stream fixed channel
matches quickly.  The generated files retain the original CSV header and
rows unchanged.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def split_channel(
    inventory_csv: Path,
    out_csv: Path,
    channel: str,
    *,
    rg_executable: str = "rg",
    instruments: set[str] | None = None,
) -> int:
    with inventory_csv.open(
        "r", encoding="utf-8", newline=""
    ) as source:
        header = source.readline()
    if not header:
        raise ValueError("inventory CSV is empty")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as handle:
        handle.write(header)
        handle.flush()
        command = [rg_executable, "-N", "-F"]
        patterns = (
            [f",{channel},{instrument}," for instrument in instruments]
            if instruments
            else [f",{channel},"]
        )
        for pattern in patterns:
            command.extend(["-e", pattern])
        command.append(str(inventory_csv))
        result = subprocess.run(
            command,
            stdout=handle,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    if result.returncode not in {0, 1}:
        raise RuntimeError(
            f"rg failed for {channel}: {result.stderr.strip()}"
        )
    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-csv", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument(
        "--channels",
        default="Tickers,MarkPrice,OpenInterest,FundingRate",
    )
    parser.add_argument(
        "--instruments-metadata",
        type=Path,
        help="optional JSON containing an instrument list",
    )
    parser.add_argument(
        "--instruments-key",
        default="selected_prefilter_instruments",
    )
    parser.add_argument(
        "--suffix",
        default="",
        help="filename suffix for a targeted index",
    )
    parser.add_argument(
        "--strip-instrument-suffix",
        default="",
        help="map universe instruments before matching a spot/index channel",
    )
    args = parser.parse_args()
    channels = [
        item.strip()
        for item in args.channels.split(",")
        if item.strip()
    ]
    if not channels:
        raise ValueError("at least one channel is required")
    instruments = None
    if args.instruments_metadata is not None:
        instrument_metadata = json.loads(
            args.instruments_metadata.read_text(encoding="utf-8")
        )
        instruments = set(
            instrument_metadata[args.instruments_key]
        )
        if args.strip_instrument_suffix:
            suffix = args.strip_instrument_suffix
            instruments = {
                (
                    instrument[: -len(suffix)]
                    if instrument.endswith(suffix)
                    else instrument
                )
                for instrument in instruments
            }
        if not instruments:
            raise ValueError("instrument list is empty")

    source_stat = args.inventory_csv.stat()
    rows = {}
    for channel in channels:
        out_csv = args.out_dir / f"{channel}{args.suffix}.csv"
        rows[channel] = split_channel(
            args.inventory_csv,
            out_csv,
            channel,
            instruments=instruments,
        )
        print(
            f"{channel}: {rows[channel]:,} rows -> {out_csv}",
            flush=True,
        )
    metadata = {
        "split_inventory_schema_version": (
            "okx-channel-inventory-v1"
        ),
        "source_inventory": str(args.inventory_csv.resolve()),
        "source_size": source_stat.st_size,
        "source_mtime_ns": source_stat.st_mtime_ns,
        "channels": rows,
        "instruments": (
            sorted(instruments) if instruments is not None else None
        ),
    }
    (args.out_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
