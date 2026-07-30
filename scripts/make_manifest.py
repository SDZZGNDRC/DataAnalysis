"""Build a strict manifest from exact-segment-v2 NPZ metadata."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

EXPECTED_SCHEMA = "exact-segment-v2"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def split_of(start_ts_ms: int) -> str:
    date = datetime.fromtimestamp(
        start_ts_ms / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")
    if date <= "2026-06-20":
        return "train"
    if date <= "2026-06-25":
        return "val"
    return "test"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--segments", type=Path, default=Path(r"E:\tmp\segments.csv"))
    parser.add_argument("--npz-dir", type=Path, default=Path(r"E:\tmp\npz_v2"))
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    output = args.out or (args.npz_dir / "manifest.csv")

    rows = list(csv.DictReader(args.segments.open(encoding="utf-8")))
    npz_files = {path.stem: path for path in args.npz_dir.glob("*.npz")}
    output_rows = []
    invalid = []
    for row in rows:
        if row["has_snapshot"] != "1" or float(row["duration_hours"]) < 1.0:
            continue
        index = row["seg_index"]
        start_ms = int(row["start_ts"])
        end_ms = int(row["end_ts"])
        stem = f"seg_{index}_{start_ms}"
        npz_path = npz_files.get(stem)
        if npz_path is None:
            continue
        metadata_path = npz_path.with_suffix(".meta.json")
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("schema_version") != EXPECTED_SCHEMA:
                raise ValueError(f"schema={metadata.get('schema_version')!r}")
            if metadata.get("requested_start_ms") != start_ms:
                raise ValueError("start bound mismatch")
            if metadata.get("requested_end_ms") != end_ms:
                raise ValueError("end bound mismatch")
            if metadata.get("first_snapshot_ns") != start_ms * 1_000_000:
                raise ValueError("first snapshot does not match segment start")
            if metadata.get("npz_size") != npz_path.stat().st_size:
                raise ValueError("npz size changed after validation")
            expected_hash = metadata.get("npz_sha256")
            if not expected_hash:
                raise ValueError("missing persisted NPZ SHA-256")
            if expected_hash != file_sha256(npz_path):
                raise ValueError("NPZ SHA-256 mismatch")
        except Exception as exc:
            invalid.append(f"{npz_path.name}: {exc}")
            continue

        actual_start_ns = int(metadata["actual_start_ns"])
        actual_end_ns = int(metadata["actual_end_ns"])
        output_rows.append({
            "seg_index": index,
            "start_ts": start_ms,
            "end_ts": end_ms,
            "duration_hours": f"{(actual_end_ns - actual_start_ns) / 3.6e12:.4f}",
            "start_utc": datetime.fromtimestamp(
                actual_start_ns / 1e9, tz=timezone.utc
            ).isoformat(),
            "actual_start_ns": actual_start_ns,
            "actual_end_ns": actual_end_ns,
            "event_count": int(metadata["event_count"]),
            "npz_size": int(metadata["npz_size"]),
            "npz_sha256": metadata["npz_sha256"],
            "schema_version": EXPECTED_SCHEMA,
            "npz_path": str(npz_path.resolve()),
            "split": split_of(start_ms),
        })

    if invalid:
        sample = "\n".join(invalid[:10])
        raise RuntimeError(
            f"{len(invalid)} NPZ files failed strict metadata validation:\n{sample}"
        )
    if not output_rows:
        raise RuntimeError("no validated exact-segment-v2 files found")

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(output_rows[0].keys()))
        writer.writeheader()
        writer.writerows(output_rows)

    counts = Counter(row["split"] for row in output_rows)
    print(f"manifest: {output} | {len(output_rows)} exact segments")
    for split in ("train", "val", "test"):
        hours = sum(
            float(row["duration_hours"])
            for row in output_rows
            if row["split"] == split
        )
        print(f"  {split}: {counts[split]} segments, {hours:.1f}h")


if __name__ == "__main__":
    main()
