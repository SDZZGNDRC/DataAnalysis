"""Build a compact second-level BTC-USDT spot ticker cache."""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_rl_external_features import (
    build_series,
    date_range,
)

SPOT_FILENAME = re.compile(
    r"^OKX-Tickers-BTC-USDT-\d{13}-\d{13}\.7z$"
)
SPOT_FIELDS = ("last", "bidPx", "askPx", "bidSz", "askSz")


def discover_spot_archives(
    pool_root: Path,
    start: date,
    end: date,
) -> list[Path]:
    paths = []
    for day in date_range(start, end):
        directory = pool_root / day.isoformat()
        if not directory.exists():
            continue
        paths.extend(
            path
            for path in sorted(
                directory.glob("OKX-Tickers-BTC-USDT-*.7z")
            )
            if SPOT_FILENAME.fullmatch(path.name)
        )
    return paths


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

    paths = discover_spot_archives(
        args.pool_root, args.start_date, args.end_date
    )
    print(f"spot_ticker: {len(paths)} archives", flush=True)
    timestamps, values, sources = build_series(
        paths,
        inst_id="BTC-USDT",
        fields=SPOT_FIELDS,
    )
    metadata = {
        "spot_feature_schema_version": "rl-spot-features-v1",
        "start_date": args.start_date.isoformat(),
        "end_date": args.end_date.isoformat(),
        "inst_id": "BTC-USDT",
        "fields": list(SPOT_FIELDS),
        "samples": int(len(timestamps)),
        "first_ts_ms": int(timestamps[0]) if len(timestamps) else None,
        "last_ts_ms": int(timestamps[-1]) if len(timestamps) else None,
        "sources": sources,
    }
    arrays = {"spot_ts_ms": timestamps}
    arrays.update({
        f"spot_{field}": field_values
        for field, field_values in values.items()
    })

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
    args.out.with_suffix(".meta.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        f"spot features -> {args.out}: {len(timestamps):,} samples "
        f"{metadata['first_ts_ms']}..{metadata['last_ts_ms']}"
    )


if __name__ == "__main__":
    main()
