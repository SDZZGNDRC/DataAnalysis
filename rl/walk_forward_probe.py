"""Expanding-window, cost-sensitive alpha stability evaluation.

All folds are formed chronologically from development splits. The final test
split remains blocked by ``alpha_probe._load_manifest`` unless explicitly
overridden after an experiment has been frozen.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rl.alpha_probe import (
    _eligible_rows,
    _finite_or_none,
    _load_manifest,
    collect_rows,
    evaluate_probe,
    fit_random_feature_probe,
    fit_ridge_probe,
    make_horizon_dataset,
    predict_probe,
)
from rl.external_features import (
    EXTERNAL_FEATURE_NAMES,
    ExternalFeatureStore,
    augment_segments,
)
from rl.spot_features import (
    SPOT_FEATURE_NAMES,
    SpotFeatureStore,
    augment_segments_with_spot,
)


def expanding_folds(
    n_segments: int,
    min_train_segments: int,
    n_folds: int,
) -> list[tuple[slice, slice]]:
    if min_train_segments < 2:
        raise ValueError("min_train_segments must be at least 2")
    if n_folds <= 0:
        raise ValueError("n_folds must be positive")
    remaining = n_segments - min_train_segments
    if remaining < n_folds:
        raise ValueError(
            "not enough evaluation segments for requested folds"
        )
    base_size, extra = divmod(remaining, n_folds)
    folds = []
    eval_start = min_train_segments
    for fold_index in range(n_folds):
        eval_size = base_size + (1 if fold_index < extra else 0)
        eval_end = eval_start + eval_size
        folds.append((
            slice(0, eval_start),
            slice(eval_start, eval_end),
        ))
        eval_start = eval_end
    return folds


def _parse_numbers(raw: str, converter):
    values = [
        converter(item.strip())
        for item in raw.split(",")
        if item.strip()
    ]
    if not values:
        raise ValueError("numeric list cannot be empty")
    return values


def _summary(fold_results: pd.DataFrame) -> pd.DataFrame:
    rows = []
    group_columns = [
        "model_type",
        "horizon_seconds",
        "tail_fraction",
        "cost_bps",
    ]
    for keys, group in fold_results.groupby(group_columns):
        model_type, horizon, tail_fraction, cost_bps = keys
        rows.append({
            "model_type": str(model_type),
            "horizon_seconds": int(horizon),
            "tail_fraction": float(tail_fraction),
            "cost_bps": float(cost_bps),
            "folds": int(len(group)),
            "median_information_coefficient": float(
                group["information_coefficient"].median()
            ),
            "median_gross_edge_bps": float(
                group["gross_edge_bps"].median()
            ),
            "median_net_edge_bps": float(
                group["net_edge_bps"].median()
            ),
            "worst_fold_net_edge_bps": float(
                group["net_edge_bps"].min()
            ),
            "positive_fold_fraction": float(
                (group["net_edge_bps"] > 0).mean()
            ),
            "median_positive_segment_fraction": float(
                group["positive_segment_fraction"].median()
            ),
            "median_signal_coverage": float(
                group["signal_coverage"].median()
            ),
        })
    return pd.DataFrame(rows).sort_values(
        [
            "cost_bps",
            "median_net_edge_bps",
            "worst_fold_net_edge_bps",
        ],
        ascending=[False, False, False],
    )


def evaluate_persistent_signal(
    segments: list[tuple[int, np.ndarray, np.ndarray]],
    model: dict,
    *,
    horizon_steps: int,
    one_way_cost_bps: float,
    exit_mode: str = "neutral",
    stop_loss_bps: float = 0.0,
    take_profit_bps: float = 0.0,
) -> dict:
    """Optimistic mid-price upper bound with persistent, non-overlapping positions."""
    allowed_modes = {
        "neutral",
        "fixed",
        "opposite",
        "vote",
        "fixed_barrier",
        "vote_barrier",
    }
    if exit_mode not in allowed_modes:
        raise ValueError(
            f"exit_mode must be one of {sorted(allowed_modes)}"
        )
    uses_barrier = exit_mode.endswith("_barrier")
    base_exit_mode = exit_mode.removesuffix("_barrier")
    if uses_barrier and (
        stop_loss_bps <= 0 or take_profit_bps <= 0
    ):
        raise ValueError(
            "barrier exits require positive stop loss and take profit"
        )
    segment_net_pnl = []
    segment_metrics = []
    gross_pnl = 0.0
    costs = 0.0
    turnover = 0.0
    exposed_steps = 0
    total_steps = 0
    lower = float(model["lower_threshold_bps"])
    upper = float(model["upper_threshold_bps"])
    neutral = float(model["neutral_threshold_bps"])

    for segment in segments:
        segment_id, features, mids = segment[:3]
        if len(mids) < 2:
            continue
        predictions = predict_probe(features[:-1], model)
        returns_bps = (
            mids[1:] / mids[:-1] - 1.0
        ) * 10_000
        position = 0
        entry_mid = 0.0
        blocked_direction = 0
        held_steps = 0
        vote_buffer = np.zeros(horizon_steps, dtype=np.int8)
        vote_index = 0
        vote_sum = 0
        segment_gross = 0.0
        segment_cost = 0.0
        segment_turnover = 0.0
        segment_exposed_steps = 0
        segment_total_steps = 0
        for step_index, (prediction, return_bps) in enumerate(zip(
            predictions, returns_bps
        )):
            current_mid = float(mids[step_index])
            if base_exit_mode == "vote":
                signal = (
                    1 if prediction >= upper
                    else -1 if prediction <= lower
                    else 0
                )
                expired = int(vote_buffer[vote_index])
                vote_buffer[vote_index] = signal
                vote_index = (vote_index + 1) % horizon_steps
                vote_sum += signal - expired
                next_position = int(np.sign(vote_sum))
            else:
                next_position = position
                if position == 0:
                    if prediction >= upper:
                        next_position = 1
                    elif prediction <= lower:
                        next_position = -1
                elif position > 0:
                    should_exit = held_steps >= horizon_steps
                    if base_exit_mode == "neutral":
                        should_exit = (
                            should_exit or prediction <= neutral
                        )
                    elif base_exit_mode == "opposite":
                        should_exit = (
                            should_exit or prediction <= lower
                        )
                    if should_exit:
                        next_position = 0
                else:
                    should_exit = held_steps >= horizon_steps
                    if base_exit_mode == "neutral":
                        should_exit = (
                            should_exit or prediction >= neutral
                        )
                    elif base_exit_mode == "opposite":
                        should_exit = (
                            should_exit or prediction >= upper
                        )
                    if should_exit:
                        next_position = 0

            if uses_barrier:
                desired_direction = int(np.sign(next_position))
                if (
                    blocked_direction
                    and desired_direction != blocked_direction
                ):
                    blocked_direction = 0
                if position and entry_mid > 0 and current_mid > 0:
                    open_pnl_bps = (
                        position
                        * (current_mid / entry_mid - 1.0)
                        * 10_000
                    )
                    if (
                        open_pnl_bps <= -stop_loss_bps
                        or open_pnl_bps >= take_profit_bps
                    ):
                        next_position = 0
                        blocked_direction = position
                elif (
                    blocked_direction
                    and desired_direction == blocked_direction
                ):
                    next_position = 0

            position_change = abs(next_position - position)
            if position_change:
                charge = position_change * one_way_cost_bps
                segment_cost += charge
                turnover += position_change
                segment_turnover += position_change
                held_steps = 0
                if position == 0 and next_position != 0:
                    entry_mid = current_mid
                elif next_position == 0:
                    entry_mid = 0.0
                elif next_position != position:
                    entry_mid = current_mid
            elif next_position != 0:
                held_steps += 1
            position = next_position
            segment_gross += position * float(return_bps)
            exposed_steps += int(position != 0)
            segment_exposed_steps += int(position != 0)
            total_steps += 1
            segment_total_steps += 1

        if position != 0:
            segment_cost += one_way_cost_bps
            turnover += 1.0
            segment_turnover += 1.0
        segment_net = segment_gross - segment_cost
        segment_net_pnl.append(segment_net)
        segment_metrics.append({
            "segment_id": int(segment_id),
            "gross_pnl_bps": float(segment_gross),
            "cost_bps": float(segment_cost),
            "net_pnl_bps": float(segment_net),
            "round_trip_equivalents": float(
                segment_turnover / 2.0
            ),
            "exposure_fraction": (
                float(segment_exposed_steps / segment_total_steps)
                if segment_total_steps
                else 0.0
            ),
        })
        gross_pnl += segment_gross
        costs += segment_cost

    net_pnl = gross_pnl - costs
    segment_array = np.asarray(segment_net_pnl, dtype=np.float64)
    return {
        "gross_pnl_bps": float(gross_pnl),
        "cost_bps": float(costs),
        "net_pnl_bps": float(net_pnl),
        "turnover_units": float(turnover),
        "round_trip_equivalents": float(turnover / 2.0),
        "exposure_fraction": (
            float(exposed_steps / total_steps) if total_steps else 0.0
        ),
        "positive_segment_fraction": (
            float(np.mean(segment_array > 0))
            if len(segment_array)
            else float("nan")
        ),
        "mean_segment_net_pnl_bps": (
            float(np.mean(segment_array))
            if len(segment_array)
            else float("nan")
        ),
        "worst_segment_net_pnl_bps": (
            float(np.min(segment_array))
            if len(segment_array)
            else float("nan")
        ),
        "stop_loss_bps": float(stop_loss_bps),
        "take_profit_bps": float(take_profit_bps),
        "segment_metrics": segment_metrics,
    }


def _persistent_summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    keys = [
        "model_type",
        "horizon_seconds",
        "tail_fraction",
        "exit_mode",
        "stop_loss_bps",
        "take_profit_bps",
        "round_trip_cost_bps",
    ]
    for values, group in frame.groupby(keys):
        (
            model_type,
            horizon,
            tail_fraction,
            exit_mode,
            stop_loss_bps,
            take_profit_bps,
            round_trip_cost,
        ) = values
        rows.append({
            "model_type": str(model_type),
            "horizon_seconds": int(horizon),
            "tail_fraction": float(tail_fraction),
            "exit_mode": str(exit_mode),
            "stop_loss_bps": float(stop_loss_bps),
            "take_profit_bps": float(take_profit_bps),
            "round_trip_cost_bps": float(round_trip_cost),
            "folds": int(len(group)),
            "median_net_pnl_bps": float(
                group["net_pnl_bps"].median()
            ),
            "worst_fold_net_pnl_bps": float(
                group["net_pnl_bps"].min()
            ),
            "positive_fold_fraction": float(
                (group["net_pnl_bps"] > 0).mean()
            ),
            "median_positive_segment_fraction": float(
                group["positive_segment_fraction"].median()
            ),
            "median_round_trip_equivalents": float(
                group["round_trip_equivalents"].median()
            ),
            "median_exposure_fraction": float(
                group["exposure_fraction"].median()
            ),
        })
    return pd.DataFrame(rows).sort_values(
        [
            "round_trip_cost_bps",
            "median_net_pnl_bps",
            "worst_fold_net_pnl_bps",
        ],
        ascending=[False, False, False],
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument(
        "--contract",
        type=Path,
        default=PROJECT_ROOT / "contracts" / "btc_usdt_swap.json",
    )
    parser.add_argument(
        "--development-splits",
        default="train,val",
        help="chronological manifest splits; test remains blocked by default",
    )
    parser.add_argument("--allow-test", action="store_true")
    parser.add_argument("--step-ns", type=int, default=1_000_000_000)
    parser.add_argument("--warmup-minutes", type=float, default=15.0)
    parser.add_argument("--max-segment-hours", type=float, default=2.0)
    parser.add_argument(
        "--min-segment-hours",
        type=float,
        default=0.0,
        help="exclude segments too short for slow-horizon labels",
    )
    parser.add_argument("--max-events", type=int, default=50_000_000)
    parser.add_argument("--max-segments", type=int, default=0)
    parser.add_argument("--min-train-segments", type=int, default=12)
    parser.add_argument("--folds", type=int, default=3)
    parser.add_argument(
        "--horizons-seconds", default="10,30,120,300,900"
    )
    parser.add_argument(
        "--tail-fractions", default="0.01,0.05,0.10"
    )
    parser.add_argument(
        "--costs-bps",
        default="0,1,2,4",
        help="round-trip fee scenarios",
    )
    parser.add_argument(
        "--exit-modes", default="neutral,fixed,opposite,vote"
    )
    parser.add_argument(
        "--risk-barriers-bps",
        default="",
        help="comma-separated stop:take pairs for *_barrier exits",
    )
    parser.add_argument("--ridge", type=float, default=1e-3)
    parser.add_argument(
        "--model",
        choices=("ridge", "random-features"),
        default="ridge",
    )
    parser.add_argument("--random-features", type=int, default=128)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument(
        "--external-features",
        type=Path,
        default=None,
        help="optional rl-external-features-v1 NPZ",
    )
    parser.add_argument(
        "--spot-features",
        type=Path,
        default=None,
        help="optional rl-spot-features-v1 NPZ",
    )
    parser.add_argument(
        "--spot-feed-delay-ms",
        type=int,
        default=100,
        help="minimum delay before an exchange-timestamped spot update is usable",
    )
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()

    split_names = [
        item.strip()
        for item in args.development_splits.split(",")
        if item.strip()
    ]
    frames = [
        _load_manifest(
            args.manifest, split_name, args.allow_test
        )
        for split_name in split_names
    ]
    rows = pd.concat(frames, ignore_index=True).sort_values(
        ["actual_start_ns", "seg_index"]
    )
    if args.min_segment_hours > 0:
        rows = rows[
            rows["duration_hours"].astype(float)
            >= args.min_segment_hours
        ]
        if rows.empty:
            raise ValueError(
                "no segments remain after --min-segment-hours"
            )
    rows, skipped = _eligible_rows(
        rows, args.max_events, args.max_segments
    )
    contract = json.loads(args.contract.read_text(encoding="utf-8"))
    segments = collect_rows(
        rows,
        contract,
        step_ns=args.step_ns,
        warmup_minutes=args.warmup_minutes,
        max_segment_hours=args.max_segment_hours,
        cache_dir=args.cache_dir,
        label="development",
    )
    if args.external_features is not None:
        external_store = ExternalFeatureStore.load(
            args.external_features
        )
        segments = augment_segments(segments, external_store)
    if args.spot_features is not None:
        spot_store = SpotFeatureStore.load(args.spot_features)
        segments = augment_segments_with_spot(
            segments,
            spot_store,
            feed_delay_ms=args.spot_feed_delay_ms,
        )
    folds = expanding_folds(
        len(segments), args.min_train_segments, args.folds
    )
    horizons = _parse_numbers(args.horizons_seconds, int)
    tail_fractions = _parse_numbers(
        args.tail_fractions, float
    )
    costs = _parse_numbers(args.costs_bps, float)
    exit_modes = [
        item.strip()
        for item in args.exit_modes.split(",")
        if item.strip()
    ]
    barrier_pairs = []
    for item in args.risk_barriers_bps.split(","):
        item = item.strip()
        if not item:
            continue
        stop, take = item.split(":", maxsplit=1)
        pair = (float(stop), float(take))
        if pair[0] <= 0 or pair[1] <= 0:
            raise ValueError("risk barriers must be positive")
        barrier_pairs.append(pair)
    if any(mode.endswith("_barrier") for mode in exit_modes):
        if not barrier_pairs:
            raise ValueError(
                "*_barrier exit mode requires --risk-barriers-bps"
            )

    fold_results = []
    persistent_results = []
    persistent_segment_results = []
    fold_definitions = []
    for fold_index, (fit_slice, eval_slice) in enumerate(folds):
        fit_segments = segments[fit_slice]
        eval_segments = segments[eval_slice]
        fold_definitions.append({
            "fold": fold_index,
            "fit_segments": [item[0] for item in fit_segments],
            "eval_segments": [item[0] for item in eval_segments],
        })
        print(
            f"[fold {fold_index}] fit={len(fit_segments)} "
            f"eval={len(eval_segments)}",
            flush=True,
        )
        for horizon in horizons:
            horizon_steps = int(math.ceil(
                horizon * 1_000_000_000 / args.step_ns
            ))
            x_fit, y_fit, _ = make_horizon_dataset(
                fit_segments, horizon_steps
            )
            x_eval, y_eval, eval_segment_ids = make_horizon_dataset(
                eval_segments, horizon_steps
            )
            for tail_fraction in tail_fractions:
                if args.model == "random-features":
                    model = fit_random_feature_probe(
                        x_fit,
                        y_fit,
                        args.ridge,
                        n_random_features=args.random_features,
                        seed=(
                            args.seed
                            + fold_index * 100_000
                            + horizon
                        ),
                        tail_fraction=tail_fraction,
                    )
                else:
                    model = fit_ridge_probe(
                        x_fit,
                        y_fit,
                        args.ridge,
                        tail_fraction=tail_fraction,
                    )
                predictions = predict_probe(x_eval, model)
                for cost_bps in costs:
                    metrics = evaluate_probe(
                        y_eval,
                        predictions,
                        eval_segment_ids,
                        lower_threshold_bps=(
                            model["lower_threshold_bps"]
                        ),
                        upper_threshold_bps=(
                            model["upper_threshold_bps"]
                        ),
                        round_trip_cost_bps=cost_bps,
                    )
                    metrics.pop("segment_net_edge_bps", None)
                    metrics.update({
                        "fold": fold_index,
                        "model_type": args.model,
                        "fit_segment_count": len(fit_segments),
                        "eval_segment_count": len(eval_segments),
                        "horizon_seconds": horizon,
                        "tail_fraction": tail_fraction,
                        "cost_bps": cost_bps,
                    })
                    fold_results.append(metrics)
                    for exit_mode in exit_modes:
                        risk_pairs = (
                            barrier_pairs
                            if exit_mode.endswith("_barrier")
                            else [(0.0, 0.0)]
                        )
                        for stop_loss_bps, take_profit_bps in risk_pairs:
                            persistent = evaluate_persistent_signal(
                                eval_segments,
                                model,
                                horizon_steps=horizon_steps,
                                one_way_cost_bps=cost_bps / 2.0,
                                exit_mode=exit_mode,
                                stop_loss_bps=stop_loss_bps,
                                take_profit_bps=take_profit_bps,
                            )
                            segment_metrics = persistent.pop(
                                "segment_metrics"
                            )
                            persistent.update({
                                "fold": fold_index,
                                "model_type": args.model,
                                "fit_segment_count": len(fit_segments),
                                "eval_segment_count": len(eval_segments),
                                "horizon_seconds": horizon,
                                "tail_fraction": tail_fraction,
                                "exit_mode": exit_mode,
                                "round_trip_cost_bps": cost_bps,
                            })
                            persistent_results.append(persistent)
                            for detail in segment_metrics:
                                detail.update({
                                    "fold": fold_index,
                                    "model_type": args.model,
                                    "horizon_seconds": horizon,
                                    "tail_fraction": tail_fraction,
                                    "exit_mode": exit_mode,
                                    "stop_loss_bps": stop_loss_bps,
                                    "take_profit_bps": take_profit_bps,
                                    "round_trip_cost_bps": cost_bps,
                                })
                                persistent_segment_results.append(detail)

    fold_frame = pd.DataFrame(fold_results)
    summary_frame = _summary(fold_frame)
    persistent_frame = pd.DataFrame(persistent_results)
    persistent_summary = _persistent_summary(persistent_frame)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    fold_frame.to_csv(args.out_dir / "walk_forward_folds.csv", index=False)
    summary_frame.to_csv(
        args.out_dir / "walk_forward_summary.csv", index=False
    )
    persistent_frame.to_csv(
        args.out_dir / "persistent_signal_folds.csv", index=False
    )
    pd.DataFrame(persistent_segment_results).to_csv(
        args.out_dir / "persistent_signal_segments.csv",
        index=False,
    )
    persistent_summary.to_csv(
        args.out_dir / "persistent_signal_summary.csv", index=False
    )
    output = {
        "probe_schema_version": "rl-walk-forward-probe-v1",
        "warning": (
            "Development-only overlapping-return signal diagnostic; "
            "not executable PnL or Sharpe."
        ),
        "manifest": str(args.manifest.resolve()),
        "development_splits": split_names,
        "selected_segments": [item[0] for item in segments],
        "skipped_by_event_limit": skipped,
        "fold_definitions": fold_definitions,
        "step_ns": args.step_ns,
        "warmup_minutes": args.warmup_minutes,
        "max_segment_hours": args.max_segment_hours,
        "min_segment_hours": args.min_segment_hours,
        "ridge": args.ridge,
        "model": args.model,
        "random_features": (
            args.random_features
            if args.model == "random-features"
            else 0
        ),
        "seed": args.seed,
        "external_features": (
            str(args.external_features.resolve())
            if args.external_features is not None
            else None
        ),
        "external_feature_names": (
            list(EXTERNAL_FEATURE_NAMES)
            if args.external_features is not None
            else []
        ),
        "spot_features": (
            str(args.spot_features.resolve())
            if args.spot_features is not None
            else None
        ),
        "spot_feed_delay_ms": args.spot_feed_delay_ms,
        "spot_feature_names": (
            list(SPOT_FEATURE_NAMES)
            if args.spot_features is not None
            else []
        ),
        "summary": summary_frame.to_dict(orient="records"),
        "persistent_signal_summary": persistent_summary.to_dict(
            orient="records"
        ),
    }
    (args.out_dir / "walk_forward_summary.json").write_text(
        json.dumps(
            _finite_or_none(output),
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    base_cost = max(costs)
    print("overlapping-return probe, base cost:")
    print(
        summary_frame[
            summary_frame["cost_bps"] == base_cost
        ].head(10).to_string(index=False)
    )
    print("persistent-position upper bound, base cost:")
    print(
        persistent_summary[
            persistent_summary["round_trip_cost_bps"] == base_cost
        ].head(10).to_string(index=False)
    )
    print(f"walk-forward -> {args.out_dir}")


if __name__ == "__main__":
    main()
