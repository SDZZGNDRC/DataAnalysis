"""Cost-aware, leakage-resistant linear probe for RL market observations.

This is a signal diagnostic, not a trading backtest. It fits only on one
manifest split and evaluates fixed coefficients/thresholds on a later split.
The final ``test`` split is blocked by default because repeated inspection
would invalidate it as a holdout.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest

from rl.policy_core import (
    MARKET_FEATURE_INDICES,
    OBSERVATION_NAMES,
    POLICY_SCHEMA_VERSION,
    RLPolicyCore,
)


def _finite_or_none(value):
    if isinstance(value, dict):
        return {str(k): _finite_or_none(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite_or_none(v) for v in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value) if math.isfinite(float(value)) else None
    return value


def _load_manifest(path: Path, split: str, allow_test: bool) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if (
        "schema_version" not in frame
        or not frame["schema_version"].eq("exact-segment-v2").all()
    ):
        raise ValueError("alpha probe requires an exact-segment-v2 manifest")
    if split == "test" and not allow_test:
        raise ValueError(
            "test split is blocked; use a development split or explicitly "
            "pass --allow-test after freezing the experiment"
        )
    selected = frame[frame["split"] == split].copy()
    if selected.empty:
        raise ValueError(f"manifest split {split!r} is empty")
    return selected.sort_values(["actual_start_ns", "seg_index"])


def _eligible_rows(
    frame: pd.DataFrame,
    max_events: int,
    max_segments: int,
) -> tuple[pd.DataFrame, list[dict]]:
    skipped: list[dict] = []
    eligible = frame
    if max_events > 0:
        mask = eligible["event_count"].astype(int) <= max_events
        skipped = eligible.loc[
            ~mask, ["seg_index", "event_count"]
        ].to_dict(orient="records")
        eligible = eligible.loc[mask]
    if eligible.empty:
        raise ValueError("no segments remain after --max-events filtering")
    if max_segments > 0 and len(eligible) > max_segments:
        indices = np.linspace(
            0, len(eligible) - 1, num=max_segments, dtype=int
        )
        eligible = eligible.iloc[np.unique(indices)]
    return eligible, skipped


def _build_hbt(data: np.ndarray, contract: dict):
    asset = (
        BacktestAsset()
        .data([data])
        .linear_asset(1.0)
        .constant_order_latency(10_000_000, 10_000_000)
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
        .trading_value_fee_model(
            contract["maker_fee"], contract["taker_fee"]
        )
        .tick_size(contract["tick_size"])
        .lot_size(contract["lot_size"])
        .last_trades_capacity(1_000_000)
    )
    return HashMapMarketDepthBacktest([asset])


def collect_segment(
    npz_path: str,
    contract: dict,
    *,
    step_ns: int,
    warmup_minutes: float,
    max_segment_hours: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Replay one segment and return observations, mids, and decision timestamps."""
    with np.load(npz_path) as source:
        data = source["data"]
    if max_segment_hours > 0:
        first_ts = int(data["exch_ts"].min())
        cutoff = first_ts + int(max_segment_hours * 3_600_000_000_000)
        data = data[data["exch_ts"] <= cutoff]
    hbt = _build_hbt(data, contract)
    core = RLPolicyCore(
        step_ns=step_ns,
        max_position=10.0 * float(contract["lot_size"]),
        order_qty=float(contract["lot_size"]),
        report_notional=float(
            contract.get(
                "report_notional_usdt", contract["initial_balance"]
            )
        ),
    )
    core.reset(hbt.state_values(0))
    warmup_steps = max(
        core.recommended_warmup_steps,
        int(math.ceil(
            warmup_minutes * 60 * 1_000_000_000 / step_ns
        )),
    )
    features: list[np.ndarray] = []
    mids: list[float] = []
    timestamps_ms: list[int] = []
    steps = 0
    try:
        while True:
            rc = hbt.elapse(step_ns)
            if rc != 0:
                break
            snapshot = core.observe(hbt)
            if steps >= warmup_steps and snapshot.mid > 0:
                features.append(
                    snapshot.obs[list(MARKET_FEATURE_INDICES)].astype(
                        np.float64, copy=True
                    )
                )
                mids.append(float(snapshot.mid))
                timestamps_ms.append(
                    int(hbt.current_timestamp) // 1_000_000
                )
            steps += 1
    finally:
        hbt.close()
    if not features:
        return (
            np.empty(
                (0, len(MARKET_FEATURE_INDICES)), dtype=np.float64
            ),
            np.empty(0, dtype=np.float64),
            np.empty(0, dtype=np.int64),
        )
    return (
        np.vstack(features),
        np.asarray(mids, dtype=np.float64),
        np.asarray(timestamps_ms, dtype=np.int64),
    )


def make_horizon_dataset(
    segments: Iterable[tuple],
    horizon_steps: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    feature_parts = []
    target_parts = []
    segment_parts = []
    for segment in segments:
        segment_id, features, mids = segment[:3]
        if len(mids) <= horizon_steps:
            continue
        feature_parts.append(features[:-horizon_steps])
        target_parts.append(
            (mids[horizon_steps:] / mids[:-horizon_steps] - 1.0) * 10_000
        )
        segment_parts.append(
            np.full(
                len(mids) - horizon_steps,
                segment_id,
                dtype=np.int64,
            )
        )
    if not feature_parts:
        raise ValueError("no samples remain for requested horizon")
    return (
        np.vstack(feature_parts),
        np.concatenate(target_parts),
        np.concatenate(segment_parts),
    )


def fit_ridge_probe(
    features: np.ndarray,
    target_bps: np.ndarray,
    ridge: float,
    tail_fraction: float = 0.1,
) -> dict:
    if not 0 < tail_fraction < 0.5:
        raise ValueError("tail_fraction must be between 0 and 0.5")
    mean = np.nanmean(features, axis=0)
    scale = np.nanstd(features, axis=0)
    scale = np.where(
        np.isfinite(scale) & (scale > 1e-12), scale, 1.0
    )
    x = np.nan_to_num(
        (features - mean) / scale,
        nan=0.0,
        posinf=10.0,
        neginf=-10.0,
    )
    x = np.clip(x, -10.0, 10.0)
    design = np.column_stack([np.ones(len(x)), x])
    gram = design.T @ design / len(design)
    penalty = np.eye(gram.shape[0]) * float(ridge)
    penalty[0, 0] = 0.0
    rhs = design.T @ target_bps / len(design)
    coefficients = np.linalg.solve(gram + penalty, rhs)
    predictions = design @ coefficients
    lower, upper = np.quantile(
        predictions, [tail_fraction, 1.0 - tail_fraction]
    )
    return {
        "model_type": "ridge",
        "mean": mean,
        "scale": scale,
        "coefficients": coefficients,
        "neutral_threshold_bps": float(np.median(predictions)),
        "lower_threshold_bps": float(lower),
        "upper_threshold_bps": float(upper),
    }


def fit_random_feature_probe(
    features: np.ndarray,
    target_bps: np.ndarray,
    ridge: float,
    *,
    n_random_features: int,
    seed: int,
    tail_fraction: float = 0.1,
) -> dict:
    """Fit deterministic tanh random features plus the original features."""
    if n_random_features <= 0:
        raise ValueError("n_random_features must be positive")
    if not 0 < tail_fraction < 0.5:
        raise ValueError("tail_fraction must be between 0 and 0.5")
    mean = np.nanmean(features, axis=0)
    scale = np.nanstd(features, axis=0)
    scale = np.where(
        np.isfinite(scale) & (scale > 1e-12), scale, 1.0
    )
    x = np.nan_to_num(
        (features - mean) / scale,
        nan=0.0,
        posinf=10.0,
        neginf=-10.0,
    )
    x = np.clip(x, -10.0, 10.0)
    rng = np.random.default_rng(seed)
    projection = rng.normal(
        0.0,
        1.0 / math.sqrt(x.shape[1]),
        size=(x.shape[1], n_random_features),
    )
    bias = rng.uniform(
        -np.pi, np.pi, size=n_random_features
    )
    nonlinear = np.tanh(x @ projection + bias)
    design = np.column_stack([np.ones(len(x)), x, nonlinear])
    gram = design.T @ design / len(design)
    penalty = np.eye(gram.shape[0]) * float(ridge)
    penalty[0, 0] = 0.0
    rhs = design.T @ target_bps / len(design)
    coefficients = np.linalg.solve(gram + penalty, rhs)
    predictions = design @ coefficients
    lower, upper = np.quantile(
        predictions, [tail_fraction, 1.0 - tail_fraction]
    )
    return {
        "model_type": "random-features",
        "mean": mean,
        "scale": scale,
        "projection": projection,
        "bias": bias,
        "coefficients": coefficients,
        "linear_feature_count": x.shape[1],
        "neutral_threshold_bps": float(np.median(predictions)),
        "lower_threshold_bps": float(lower),
        "upper_threshold_bps": float(upper),
    }


def predict_probe(features: np.ndarray, model: dict) -> np.ndarray:
    x = np.nan_to_num(
        (features - model["mean"]) / model["scale"],
        nan=0.0,
        posinf=10.0,
        neginf=-10.0,
    )
    x = np.clip(x, -10.0, 10.0)
    if model.get("model_type", "ridge") == "random-features":
        nonlinear = np.tanh(
            x @ model["projection"] + model["bias"]
        )
        design = np.column_stack([x, nonlinear])
    else:
        design = x
    return model["coefficients"][0] + design @ model["coefficients"][1:]


def evaluate_probe(
    target_bps: np.ndarray,
    predictions: np.ndarray,
    segment_ids: np.ndarray,
    *,
    lower_threshold_bps: float,
    upper_threshold_bps: float,
    round_trip_cost_bps: float,
) -> dict:
    positions = np.zeros(len(predictions), dtype=np.float64)
    positions[predictions <= lower_threshold_bps] = -1.0
    positions[predictions >= upper_threshold_bps] = 1.0
    traded = positions != 0
    if np.std(predictions) > 0 and np.std(target_bps) > 0:
        information_coefficient = float(
            np.corrcoef(predictions, target_bps)[0, 1]
        )
    else:
        information_coefficient = float("nan")
    if traded.any():
        signed_gross = positions[traded] * target_bps[traded]
        gross_edge = float(np.mean(signed_gross))
        net_edge = gross_edge - round_trip_cost_bps
        directional_accuracy = float(np.mean(signed_gross > 0))
    else:
        gross_edge = net_edge = directional_accuracy = float("nan")
    segment_net_edges = []
    for segment_id in np.unique(segment_ids):
        mask = (segment_ids == segment_id) & traded
        if mask.any():
            edge = float(
                np.mean(positions[mask] * target_bps[mask])
                - round_trip_cost_bps
            )
            segment_net_edges.append(edge)
    return {
        "samples": int(len(target_bps)),
        "signal_coverage": float(np.mean(traded)),
        "information_coefficient": information_coefficient,
        "directional_accuracy": directional_accuracy,
        "gross_edge_bps": gross_edge,
        "round_trip_cost_bps": float(round_trip_cost_bps),
        "net_edge_bps": net_edge,
        "segments_with_signals": int(len(segment_net_edges)),
        "positive_segment_fraction": (
            float(np.mean(np.asarray(segment_net_edges) > 0))
            if segment_net_edges
            else float("nan")
        ),
        "segment_net_edge_bps": segment_net_edges,
    }


def _cache_path(
    cache_dir: Path,
    row: pd.Series,
    *,
    step_ns: int,
    warmup_minutes: float,
    max_segment_hours: float,
) -> Path:
    schema = POLICY_SCHEMA_VERSION.replace("-", "_")
    source_hash = str(row.get("npz_sha256", "nohash"))[:12]
    warmup_seconds = int(round(warmup_minutes * 60))
    max_seconds = int(round(max_segment_hours * 3600))
    return cache_dir / (
        f"seg_{int(row['seg_index'])}_{source_hash}_{schema}_"
        f"step{step_ns}_warm{warmup_seconds}_max{max_seconds}.npz"
    )


def _load_cached_segment(
    path: Path,
    expected_metadata: dict,
) -> tuple[np.ndarray, np.ndarray, np.ndarray] | None:
    if not path.exists():
        return None
    try:
        with np.load(path) as cached:
            metadata = json.loads(str(cached["metadata"].item()))
            if metadata != expected_metadata:
                return None
            features = cached["features"]
            mids = cached["mids"]
            timestamps_ms = cached["timestamps_ms"]
        if (
            features.ndim != 2
            or features.shape[1] != len(MARKET_FEATURE_INDICES)
            or len(features) != len(mids)
            or len(features) != len(timestamps_ms)
            or not np.isfinite(features).all()
            or not np.isfinite(mids).all()
            or (
                len(timestamps_ms) > 1
                and np.any(np.diff(timestamps_ms) <= 0)
            )
        ):
            return None
        return features, mids, timestamps_ms
    except Exception:
        return None


def _cache_metadata(
    row: pd.Series,
    *,
    step_ns: int,
    warmup_minutes: float,
    max_segment_hours: float,
) -> dict:
    return {
        "cache_schema_version": "rl-feature-cache-v2",
        "policy_schema_version": POLICY_SCHEMA_VERSION,
        "source_npz_sha256": str(row.get("npz_sha256", "")),
        "segment_id": int(row["seg_index"]),
        "step_ns": int(step_ns),
        "warmup_minutes": float(warmup_minutes),
        "max_segment_hours": float(max_segment_hours),
        "feature_indices": list(MARKET_FEATURE_INDICES),
    }


def collect_rows(
    rows: pd.DataFrame,
    contract: dict,
    *,
    step_ns: int,
    warmup_minutes: float,
    max_segment_hours: float,
    cache_dir: Path | None,
    label: str = "collect",
) -> list[tuple[int, np.ndarray, np.ndarray, np.ndarray]]:
    result = []
    for _, row in rows.iterrows():
        segment_id = int(row["seg_index"])
        metadata = _cache_metadata(
            row,
            step_ns=step_ns,
            warmup_minutes=warmup_minutes,
            max_segment_hours=max_segment_hours,
        )
        cached = None
        cache_path = None
        if cache_dir is not None:
            cache_path = _cache_path(
                cache_dir,
                row,
                step_ns=step_ns,
                warmup_minutes=warmup_minutes,
                max_segment_hours=max_segment_hours,
            )
            cached = _load_cached_segment(cache_path, metadata)
        print(
            f"[{label}] segment={segment_id} "
            f"events={int(row['event_count']):,} "
            f"source={'cache' if cached is not None else 'replay'}",
            flush=True,
        )
        if cached is None:
            features, mids, timestamps_ms = collect_segment(
                str(row["npz_path"]),
                contract,
                step_ns=step_ns,
                warmup_minutes=warmup_minutes,
                max_segment_hours=max_segment_hours,
            )
            if cache_path is not None and len(mids):
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                temporary = cache_path.with_suffix(".tmp.npz")
                np.savez_compressed(
                    temporary,
                    features=features,
                    mids=mids,
                    timestamps_ms=timestamps_ms,
                    metadata=np.asarray(
                        json.dumps(metadata, sort_keys=True)
                    ),
                )
                temporary.replace(cache_path)
        else:
            features, mids, timestamps_ms = cached
        if len(mids):
            result.append((
                segment_id,
                features,
                mids,
                timestamps_ms,
            ))
    if not result:
        raise RuntimeError(f"{label} produced no usable samples")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument(
        "--contract",
        type=Path,
        default=PROJECT_ROOT / "contracts" / "btc_usdt_swap.json",
    )
    parser.add_argument("--fit-split", default="train")
    parser.add_argument("--eval-split", default="val")
    parser.add_argument("--allow-test", action="store_true")
    parser.add_argument("--step-ns", type=int, default=1_000_000_000)
    parser.add_argument("--warmup-minutes", type=float, default=15.0)
    parser.add_argument("--max-segment-hours", type=float, default=2.0)
    parser.add_argument(
        "--max-events",
        type=int,
        default=15_000_000,
        help=(
            "skip larger compressed segments to bound peak replay memory; "
            "0 disables"
        ),
    )
    parser.add_argument("--fit-max-segments", type=int, default=12)
    parser.add_argument("--eval-max-segments", type=int, default=0)
    parser.add_argument("--horizons-seconds", default="10,30,120")
    parser.add_argument("--ridge", type=float, default=1e-3)
    parser.add_argument(
        "--signal-tail-fraction",
        type=float,
        default=0.1,
        help="fraction selected in each prediction tail using fit data",
    )
    parser.add_argument(
        "--model",
        choices=("ridge", "random-features"),
        default="ridge",
    )
    parser.add_argument("--random-features", type=int, default=128)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="optional validated feature cache directory",
    )
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()

    if args.fit_split == args.eval_split:
        raise ValueError("fit and evaluation splits must be different")
    if args.step_ns <= 0:
        raise ValueError("--step-ns must be positive")

    contract = json.loads(args.contract.read_text(encoding="utf-8"))
    fit_all = _load_manifest(
        args.manifest, args.fit_split, args.allow_test
    )
    eval_all = _load_manifest(
        args.manifest, args.eval_split, args.allow_test
    )
    fit_rows, fit_skipped = _eligible_rows(
        fit_all, args.max_events, args.fit_max_segments
    )
    eval_rows, eval_skipped = _eligible_rows(
        eval_all, args.max_events, args.eval_max_segments
    )
    fit_segments = collect_rows(
        fit_rows,
        contract,
        step_ns=args.step_ns,
        warmup_minutes=args.warmup_minutes,
        max_segment_hours=args.max_segment_hours,
        cache_dir=args.cache_dir,
        label="fit",
    )
    eval_segments = collect_rows(
        eval_rows,
        contract,
        step_ns=args.step_ns,
        warmup_minutes=args.warmup_minutes,
        max_segment_hours=args.max_segment_hours,
        cache_dir=args.cache_dir,
        label="eval",
    )

    feature_names = [
        OBSERVATION_NAMES[i] for i in MARKET_FEATURE_INDICES
    ]
    horizon_seconds = [
        int(item.strip())
        for item in args.horizons_seconds.split(",")
        if item.strip()
    ]
    round_trip_cost_bps = (
        2.0 * float(contract["maker_fee"]) * 10_000
    )
    results = []
    coefficients = {}
    for horizon in horizon_seconds:
        horizon_steps = int(math.ceil(
            horizon * 1_000_000_000 / args.step_ns
        ))
        effective_horizon = (
            horizon_steps * args.step_ns / 1_000_000_000
        )
        x_fit, y_fit, _ = make_horizon_dataset(
            fit_segments, horizon_steps
        )
        x_eval, y_eval, eval_segment_ids = make_horizon_dataset(
            eval_segments, horizon_steps
        )
        if args.model == "random-features":
            model = fit_random_feature_probe(
                x_fit,
                y_fit,
                args.ridge,
                n_random_features=args.random_features,
                seed=args.seed + horizon,
                tail_fraction=args.signal_tail_fraction,
            )
        else:
            model = fit_ridge_probe(
                x_fit,
                y_fit,
                args.ridge,
                tail_fraction=args.signal_tail_fraction,
            )
        predictions = predict_probe(x_eval, model)
        metrics = evaluate_probe(
            y_eval,
            predictions,
            eval_segment_ids,
            lower_threshold_bps=model["lower_threshold_bps"],
            upper_threshold_bps=model["upper_threshold_bps"],
            round_trip_cost_bps=round_trip_cost_bps,
        )
        metrics.update({
            "requested_horizon_seconds": horizon,
            "effective_horizon_seconds": effective_horizon,
            "fit_samples": int(len(y_fit)),
            "lower_threshold_bps": model["lower_threshold_bps"],
            "upper_threshold_bps": model["upper_threshold_bps"],
        })
        results.append(metrics)
        coefficients[str(horizon)] = {
            name: float(value)
            for name, value in zip(
                feature_names,
                model["coefficients"][1:1 + len(feature_names)],
            )
        }

    output = {
        "probe_schema_version": "rl-alpha-probe-v1",
        "warning": (
            "Signal diagnostic only; overlapping forward returns are not an "
            "executable strategy PnL or Sharpe estimate."
        ),
        "manifest": str(args.manifest.resolve()),
        "fit_split": args.fit_split,
        "eval_split": args.eval_split,
        "fit_segments": [item[0] for item in fit_segments],
        "eval_segments": [item[0] for item in eval_segments],
        "fit_skipped_by_event_limit": fit_skipped,
        "eval_skipped_by_event_limit": eval_skipped,
        "step_ns": args.step_ns,
        "max_segment_hours": args.max_segment_hours,
        "max_events": args.max_events,
        "cache_dir": (
            str(args.cache_dir.resolve())
            if args.cache_dir is not None
            else None
        ),
        "model": args.model,
        "ridge": args.ridge,
        "signal_tail_fraction": args.signal_tail_fraction,
        "random_features": (
            args.random_features
            if args.model == "random-features"
            else 0
        ),
        "seed": args.seed,
        "feature_names": feature_names,
        "results": results,
        "standardised_coefficients": coefficients,
    }
    args.out_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.out_dir / "alpha_probe.json"
    csv_path = args.out_dir / "alpha_probe.csv"
    json_path.write_text(
        json.dumps(
            _finite_or_none(output),
            indent=2,
            ensure_ascii=False,
        ) + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(results).drop(
        columns=["segment_net_edge_bps"], errors="ignore"
    ).to_csv(csv_path, index=False)
    print(f"probe -> {json_path}")
    print(pd.DataFrame(results).to_string(index=False))


if __name__ == "__main__":
    main()
