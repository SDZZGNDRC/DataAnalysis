"""Build compact second-level external features from OKX JSON archives."""
from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import py7zr


SERIES = {
    "mark": {
        "pattern": "OKX-MarkPrice-BTC-USDT-SWAP-*.7z",
        "inst_id": "BTC-USDT-SWAP",
        "fields": ("markPx",),
    },
    "index": {
        "pattern": "OKX-IndexTickers-BTC-USDT-*.7z",
        "inst_id": "BTC-USDT",
        "fields": ("idxPx",),
    },
    "open_interest": {
        "pattern": "OKX-OpenInterest-BTC-USDT-SWAP-*.7z",
        "inst_id": "BTC-USDT-SWAP",
        "fields": ("oi", "oiCcy", "oiUsd"),
    },
    "funding": {
        "pattern": "OKX-FundingRate-BTC-USDT-SWAP-*.7z",
        "inst_id": "BTC-USDT-SWAP",
        "fields": ("fundingRate", "premium"),
    },
}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_root_entries(
    root: dict,
    *,
    inst_id: str,
    fields: tuple[str, ...],
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    timestamps = []
    values = {field: [] for field in fields}
    for item in root.get("data", []):
        argument = item.get("arg", {})
        if argument.get("instId") != inst_id:
            continue
        for entry in item.get("data", []):
            try:
                timestamp = int(entry["ts"])
                parsed = [float(entry[field]) for field in fields]
            except (KeyError, TypeError, ValueError):
                continue
            if not all(np.isfinite(parsed)):
                continue
            timestamps.append(timestamp)
            for field, value in zip(fields, parsed):
                values[field].append(value)
    return (
        np.asarray(timestamps, dtype=np.int64),
        {
            field: np.asarray(field_values, dtype=np.float64)
            for field, field_values in values.items()
        },
    )


def deduplicate_last_per_second(
    timestamps_ms: np.ndarray,
    values: dict[str, np.ndarray],
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    if len(timestamps_ms) == 0:
        return timestamps_ms, values
    order = np.argsort(timestamps_ms, kind="stable")
    timestamps = timestamps_ms[order]
    seconds = timestamps // 1000
    keep = np.r_[seconds[1:] != seconds[:-1], True]
    selected_order = order[keep]
    selected_timestamps = timestamps_ms[selected_order]
    final_order = np.argsort(selected_timestamps, kind="stable")
    selected_timestamps = selected_timestamps[final_order]
    selected_values = {
        field: field_values[selected_order][final_order]
        for field, field_values in values.items()
    }
    return selected_timestamps, selected_values


def read_archive(
    path: Path,
    *,
    inst_id: str,
    fields: tuple[str, ...],
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    with tempfile.TemporaryDirectory() as temporary_dir:
        with py7zr.SevenZipFile(path, mode="r") as archive:
            archive.extractall(temporary_dir)
        json_files = list(Path(temporary_dir).glob("*.json"))
        if len(json_files) != 1:
            raise ValueError(
                f"{path} contains {len(json_files)} JSON files"
            )
        root = json.loads(
            json_files[0].read_text(encoding="utf-8")
        )
    return extract_root_entries(
        root, inst_id=inst_id, fields=fields
    )


def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def discover_archives(
    pool_root: Path,
    start: date,
    end: date,
    pattern: str,
) -> list[Path]:
    paths = []
    for day in date_range(start, end):
        directory = pool_root / day.isoformat()
        if directory.exists():
            paths.extend(sorted(directory.glob(pattern)))
    return paths


def build_series(
    paths: list[Path],
    *,
    inst_id: str,
    fields: tuple[str, ...],
) -> tuple[np.ndarray, dict[str, np.ndarray], list[dict]]:
    day_timestamps = []
    day_values = {field: [] for field in fields}
    sources = []
    for index, path in enumerate(paths, start=1):
        print(
            f"[{index}/{len(paths)}] {path.name}",
            flush=True,
        )
        timestamps, values = read_archive(
            path, inst_id=inst_id, fields=fields
        )
        if len(timestamps):
            day_timestamps.append(timestamps)
            for field in fields:
                day_values[field].append(values[field])
        sources.append({
            "path": str(path.resolve()),
            "size": path.stat().st_size,
            "sha256": file_sha256(path),
        })
    if not day_timestamps:
        return (
            np.empty(0, dtype=np.int64),
            {
                field: np.empty(0, dtype=np.float64)
                for field in fields
            },
            sources,
        )
    timestamps = np.concatenate(day_timestamps)
    values = {
        field: np.concatenate(day_values[field])
        for field in fields
    }
    timestamps, values = deduplicate_last_per_second(
        timestamps, values
    )
    return timestamps, values, sources


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pool-root", type=Path, default=Path(r"E:\datapool")
    )
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.end_date < args.start_date:
        raise ValueError("end date precedes start date")

    arrays = {}
    metadata = {
        "external_feature_schema_version": "rl-external-features-v1",
        "start_date": args.start_date.isoformat(),
        "end_date": args.end_date.isoformat(),
        "series": {},
    }
    for name, specification in SERIES.items():
        paths = discover_archives(
            args.pool_root,
            args.start_date,
            args.end_date,
            specification["pattern"],
        )
        print(f"{name}: {len(paths)} archives", flush=True)
        timestamps, values, sources = build_series(
            paths,
            inst_id=specification["inst_id"],
            fields=specification["fields"],
        )
        arrays[f"{name}_ts_ms"] = timestamps
        for field, field_values in values.items():
            arrays[f"{name}_{field}"] = field_values
        metadata["series"][name] = {
            "inst_id": specification["inst_id"],
            "fields": list(specification["fields"]),
            "samples": int(len(timestamps)),
            "first_ts_ms": (
                int(timestamps[0]) if len(timestamps) else None
            ),
            "last_ts_ms": (
                int(timestamps[-1]) if len(timestamps) else None
            ),
            "sources": sources,
        }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.out.with_suffix(".tmp.npz")
    np.savez_compressed(
        temporary,
        **arrays,
        metadata=np.asarray(
            json.dumps(metadata, ensure_ascii=False, sort_keys=True)
        ),
    )
    temporary.replace(args.out)
    metadata_path = args.out.with_suffix(".meta.json")
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"external features -> {args.out}")
    for name, item in metadata["series"].items():
        print(
            f"  {name}: {item['samples']:,} samples "
            f"{item['first_ts_ms']}..{item['last_ts_ms']}"
        )


if __name__ == "__main__":
    main()
