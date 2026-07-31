"""Intraday signal gate using synchronized OKX Ticker archives.

This probe is deliberately simpler than an executable backtest.  It tests
whether top-of-book imbalance and short-horizon price continuation/reversal
have enough gross edge to justify a later Books/Trades hftbacktest.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_rl_external_features import (
    deduplicate_last_per_second,
    read_archive,
)

FIELDS = ("bidPx", "askPx", "bidSz", "askSz")


def extract_decisions(
    timestamps_ms: np.ndarray,
    values: dict[str, np.ndarray],
    *,
    cutoff_ms: int,
    window_minutes: int,
    sample_seconds: int,
    horizons_seconds: tuple[int, ...],
    max_quote_staleness_ms: int,
) -> list[dict]:
    timestamps_ms, values = deduplicate_last_per_second(
        timestamps_ms, values
    )
    if not len(timestamps_ms):
        return []
    start_ms = cutoff_ms - window_minutes * 60_000
    max_horizon_ms = max(horizons_seconds) * 1000
    decisions = np.arange(
        start_ms + 10_000,
        cutoff_ms - max_horizon_ms + 1,
        sample_seconds * 1000,
        dtype=np.int64,
    )
    rows = []
    for decision_ms in decisions:
        current_index = (
            np.searchsorted(
                timestamps_ms, decision_ms, side="right"
            )
            - 1
        )
        past_index = (
            np.searchsorted(
                timestamps_ms, decision_ms - 10_000, side="right"
            )
            - 1
        )
        if current_index < 0 or past_index < 0:
            continue
        if (
            decision_ms - int(timestamps_ms[current_index])
            > max_quote_staleness_ms
            or decision_ms
            - 10_000
            - int(timestamps_ms[past_index])
            > max_quote_staleness_ms
        ):
            continue
        bid = float(values["bidPx"][current_index])
        ask = float(values["askPx"][current_index])
        bid_size = float(values["bidSz"][current_index])
        ask_size = float(values["askSz"][current_index])
        past_mid = 0.5 * (
            float(values["bidPx"][past_index])
            + float(values["askPx"][past_index])
        )
        if not (
            bid > 0
            and ask >= bid
            and bid_size >= 0
            and ask_size >= 0
            and past_mid > 0
            and bid_size + ask_size > 0
        ):
            continue
        mid = 0.5 * (bid + ask)
        base = {
            "decision_ms": int(decision_ms),
            "quote_timestamp_ms": int(
                timestamps_ms[current_index]
            ),
            "spread_bps": (ask / bid - 1.0) * 10_000,
            "book_imbalance": (
                (bid_size - ask_size) / (bid_size + ask_size)
            ),
            "return_10s_bps": math.log(mid / past_mid) * 10_000,
        }
        for horizon_seconds in horizons_seconds:
            target_ms = decision_ms + horizon_seconds * 1000
            future_index = np.searchsorted(
                timestamps_ms, target_ms, side="left"
            )
            if future_index >= len(timestamps_ms):
                continue
            if (
                int(timestamps_ms[future_index]) - target_ms
                > max_quote_staleness_ms
            ):
                continue
            future_mid = 0.5 * (
                float(values["bidPx"][future_index])
                + float(values["askPx"][future_index])
            )
            if future_mid <= 0:
                continue
            rows.append({
                **base,
                "horizon_seconds": horizon_seconds,
                "future_return_bps": (
                    math.log(future_mid / mid) * 10_000
                ),
            })
    return rows


def read_asset_day(
    row: dict,
    *,
    window_minutes: int,
    sample_seconds: int,
    horizons_seconds: tuple[int, ...],
    max_quote_staleness_ms: int,
) -> list[dict]:
    path = Path(row["source_path"])
    timestamps, values = read_archive(
        path,
        inst_id=row["instrument"],
        fields=FIELDS,
    )
    decisions = extract_decisions(
        timestamps,
        values,
        cutoff_ms=int(row["sample_cutoff_ms"]),
        window_minutes=window_minutes,
        sample_seconds=sample_seconds,
        horizons_seconds=horizons_seconds,
        max_quote_staleness_ms=max_quote_staleness_ms,
    )
    for decision in decisions:
        decision.update({
            "instrument": row["instrument"],
            "directory_date": row["directory_date"],
            "source_path": row["source_path"],
            "source_size": int(row["source_size"]),
        })
    return decisions


def evaluate_signals(samples: pd.DataFrame) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    signal_values = {
        "book_imbalance": samples["book_imbalance"],
        "momentum_10s": samples["return_10s_bps"],
        "reversal_10s": -samples["return_10s_bps"],
    }
    rows = []
    for signal_name, signal in signal_values.items():
        frame = samples[[
            "directory_date",
            "horizon_seconds",
            "future_return_bps",
            "spread_bps",
        ]].copy()
        frame["signal"] = signal
        frame["signed_gross_bps"] = (
            np.sign(frame["signal"]) * frame["future_return_bps"]
        )
        frame["maker_net_bps"] = frame["signed_gross_bps"] - 4.0
        frame["taker_net_bps"] = (
            frame["signed_gross_bps"]
            - 14.0
            - frame["spread_bps"]
        )
        for (date_value, horizon), group in frame.groupby(
            ["directory_date", "horizon_seconds"]
        ):
            correlation = group["signal"].corr(
                group["future_return_bps"]
            )
            rows.append({
                "signal": signal_name,
                "directory_date": date_value,
                "horizon_seconds": int(horizon),
                "samples": len(group),
                "pearson_ic": float(correlation),
                "gross_edge_bps": float(
                    group["signed_gross_bps"].mean()
                ),
                "maker_net_edge_bps": float(
                    group["maker_net_bps"].mean()
                ),
                "taker_net_edge_bps": float(
                    group["taker_net_bps"].mean()
                ),
            })
    daily = pd.DataFrame(rows)
    summary_rows = []
    for keys, group in daily.groupby(
        ["signal", "horizon_seconds"]
    ):
        signal_name, horizon = keys
        dates = sorted(group["directory_date"].unique())
        fold_ids = {
            date_value: fold
            for fold, partition in enumerate(
                np.array_split(dates, 4)
            )
            for date_value in partition
        }
        with_folds = group.copy()
        with_folds["fold"] = with_folds[
            "directory_date"
        ].map(fold_ids)
        for cost_column in (
            "gross_edge_bps",
            "maker_net_edge_bps",
            "taker_net_edge_bps",
        ):
            fold_edge = with_folds.groupby("fold")[
                cost_column
            ].mean()
            summary_rows.append({
                "signal": signal_name,
                "horizon_seconds": int(horizon),
                "cost_mode": cost_column.removesuffix(
                    "_edge_bps"
                ),
                "days": len(group),
                "median_daily_ic": float(
                    group["pearson_ic"].median()
                ),
                "mean_edge_bps_per_trade": float(
                    group[cost_column].mean()
                ),
                "positive_fold_fraction": float(
                    (fold_edge > 0).mean()
                ),
                "worst_fold_edge_bps": float(fold_edge.min()),
            })
    summary = pd.DataFrame(summary_rows).sort_values(
        [
            "cost_mode",
            "positive_fold_fraction",
            "mean_edge_bps_per_trade",
        ],
        ascending=[True, False, False],
    )
    return daily, summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker-panel-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument(
        "--controls",
        default="BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP",
    )
    parser.add_argument("--top-n", type=int, default=8)
    parser.add_argument("--start-date", default="2026-06-08")
    parser.add_argument("--end-date", default="2026-06-30")
    parser.add_argument("--window-minutes", type=int, default=60)
    parser.add_argument("--sample-seconds", type=int, default=60)
    parser.add_argument("--horizons-seconds", default="10,60")
    parser.add_argument(
        "--max-quote-staleness-ms", type=int, default=2_000
    )
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    controls = {
        item.strip()
        for item in args.controls.split(",")
        if item.strip()
    }
    liquidity = pd.read_csv(
        args.ticker_panel_dir / "liquidity_summary.csv"
    )
    selected = (
        liquidity[
            (liquidity["qualifies"] == 1)
            & ~liquidity["instrument"].isin(controls)
        ]
        .sort_values("liquidity_score", ascending=False)
        .head(args.top_n)["instrument"]
        .tolist()
    )
    daily_samples = pd.read_csv(
        args.ticker_panel_dir / "daily_ticker_samples.csv"
    )
    tasks = daily_samples[
        daily_samples["instrument"].isin(selected)
        & daily_samples["directory_date"].between(
            args.start_date, args.end_date
        )
    ].to_dict(orient="records")
    horizons = tuple(
        int(item)
        for item in args.horizons_seconds.split(",")
        if item.strip()
    )
    rows = []
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                read_asset_day,
                row,
                window_minutes=args.window_minutes,
                sample_seconds=args.sample_seconds,
                horizons_seconds=horizons,
                max_quote_staleness_ms=args.max_quote_staleness_ms,
            ): (row["instrument"], row["directory_date"])
            for row in tasks
        }
        for completed, future in enumerate(
            as_completed(futures), start=1
        ):
            key = futures[future]
            try:
                rows.extend(future.result())
            except Exception as exc:
                failures.append({
                    "instrument": key[0],
                    "directory_date": key[1],
                    "error": repr(exc),
                })
            if completed % 25 == 0 or completed == len(futures):
                print(
                    f"[{completed}/{len(futures)}] "
                    f"samples={len(rows)} failures={len(failures)}",
                    flush=True,
                )
    samples = pd.DataFrame(rows)
    if samples.empty:
        raise ValueError("no intraday samples")
    daily, summary = evaluate_signals(samples)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    samples.to_csv(
        args.out_dir / "microstructure_samples.csv", index=False
    )
    daily.to_csv(
        args.out_dir / "microstructure_daily.csv", index=False
    )
    summary.to_csv(
        args.out_dir / "microstructure_summary.csv", index=False
    )
    if failures:
        pd.DataFrame(failures).to_csv(
            args.out_dir / "failures.csv", index=False
        )
    metadata = {
        "probe_schema_version": "rl-microstructure-probe-v1",
        "warning": (
            "Signal gate only; maker edge assumes fills and is not an "
            "executable hftbacktest."
        ),
        "selected_instruments": selected,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "window_minutes": args.window_minutes,
        "sample_seconds": args.sample_seconds,
        "horizons_seconds": list(horizons),
        "max_quote_staleness_ms": args.max_quote_staleness_ms,
        "asset_days": len(tasks),
        "samples": len(samples),
        "failures": failures,
    }
    (args.out_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(summary.to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
