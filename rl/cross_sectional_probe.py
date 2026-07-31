"""Development-only cross-sectional alpha probe for multiple OKX swaps.

This is a daily, close-to-close signal screen. It is not an executable HFT
backtest. The probe keeps the portfolio dollar neutral and evaluates both an
optimistic maker-cost scenario and a taker-plus-half-spread scenario.
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

from rl.walk_forward_probe import expanding_folds

BASE_FEATURE_NAMES = (
    "relative_return_1d",
    "relative_return_3d",
    "relative_return_7d",
    "residual_return_1d",
)
TICKER_FEATURE_NAMES = (
    "relative_log_volume_change_1d",
    "relative_log_spread_level",
)
DERIVATIVES_FEATURE_NAMES = (
    "relative_log_oi_change_1d",
)


def cross_sectional_zscore(frame: pd.DataFrame) -> pd.DataFrame:
    mean = frame.mean(axis=1)
    scale = frame.std(axis=1, ddof=0).replace(0.0, 1.0)
    return frame.sub(mean, axis=0).div(scale, axis=0).fillna(0.0)


def build_feature_panel(
    marks: pd.DataFrame,
    instruments: list[str],
    *,
    market_instrument: str,
    max_staleness_ms: float,
    ticker_samples: pd.DataFrame | None = None,
    derivatives_samples: pd.DataFrame | None = None,
    max_oi_staleness_ms: float = 60_000,
    min_cross_section_instruments: int = 20,
) -> tuple[
    pd.DataFrame,
    pd.Series,
    pd.Series,
    pd.Series,
    list[str],
    list[str],
]:
    mark_subset = marks[marks["instrument"].isin(
        set(instruments) | {market_instrument}
    )].copy()
    price = mark_subset.pivot(
        index="directory_date",
        columns="instrument",
        values="mark_px",
    ).sort_index()
    mark_quality = (
        mark_subset.pivot(
            index="directory_date",
            columns="instrument",
            values="staleness_ms",
        ).reindex(index=price.index)
        <= max_staleness_ms
    )
    cutoff_by_date = (
        mark_subset.groupby(
            "directory_date"
        )["sample_cutoff_ms"].first()
        .sort_index()
    )
    interval_hours = cutoff_by_date.diff() / 3_600_000
    interval_quality = interval_hours.between(18.0, 30.0)
    missing = set(instruments) - set(price.columns)
    if missing:
        raise ValueError(f"missing instruments: {sorted(missing)}")
    if market_instrument not in price.columns:
        raise ValueError("market instrument missing from daily samples")

    log_price = np.log(price[instruments])
    return_1d = log_price.diff()
    market_return = np.log(price[market_instrument]).diff()
    asset_mark_quality = mark_quality[instruments]
    market_mark_quality = mark_quality[market_instrument]
    return_1d = return_1d.where(
        asset_mark_quality
        & asset_mark_quality.shift(1, fill_value=False)
    )
    market_return = market_return.where(
        market_mark_quality
        & market_mark_quality.shift(1, fill_value=False)
    )
    residual_1d = return_1d.sub(
        market_return, axis=0
    )
    raw_features = {
        "relative_return_1d": return_1d,
        "relative_return_3d": log_price.diff(3).where(
            asset_mark_quality
            & asset_mark_quality.shift(3, fill_value=False)
        ),
        "relative_return_7d": log_price.diff(7).where(
            asset_mark_quality
            & asset_mark_quality.shift(7, fill_value=False)
        ),
        "residual_return_1d": residual_1d,
    }
    feature_names = list(BASE_FEATURE_NAMES)
    ticker_feature_quality = None
    if ticker_samples is not None:
        ticker_subset = ticker_samples[
            ticker_samples["instrument"].isin(instruments)
        ].copy()
        ticker_volume = ticker_subset.pivot(
            index="directory_date",
            columns="instrument",
            values="quote_volume_24h",
        ).reindex(index=price.index, columns=instruments)
        ticker_spread = ticker_subset.pivot(
            index="directory_date",
            columns="instrument",
            values="spread_bps",
        ).reindex(index=price.index, columns=instruments)
        ticker_quality = (
            ticker_subset.pivot(
                index="directory_date",
                columns="instrument",
                values="staleness_ms",
            ).reindex(index=price.index, columns=instruments)
            <= max_staleness_ms
        )
        ticker_cutoff = ticker_subset.groupby(
            "directory_date"
        )["sample_cutoff_ms"].first().reindex(price.index)
        ticker_date_quality = ticker_cutoff.eq(
            cutoff_by_date.reindex(price.index)
        )
        ticker_quality = ticker_quality.mul(
            ticker_date_quality, axis=0
        ).astype(bool)
        ticker_feature_quality = (
            ticker_quality
            & ticker_quality.shift(1, fill_value=False)
        )
        ticker_volume = ticker_volume.where(ticker_volume > 0)
        ticker_spread = ticker_spread.where(ticker_spread >= 0)
        raw_features.update({
            "relative_log_volume_change_1d": (
                np.log(ticker_volume).diff()
            ).where(ticker_feature_quality),
            "relative_log_spread_level": np.log(
                ticker_spread.clip(lower=0.01)
            ).where(ticker_quality),
        })
        feature_names.extend(TICKER_FEATURE_NAMES)
    if derivatives_samples is not None:
        derivatives_subset = derivatives_samples[
            derivatives_samples["instrument"].isin(instruments)
        ].copy()
        oi_ccy = derivatives_subset.pivot(
            index="directory_date",
            columns="instrument",
            values="oi_ccy",
        ).reindex(index=price.index, columns=instruments)
        oi_quality = (
            derivatives_subset.pivot(
                index="directory_date",
                columns="instrument",
                values="oi_staleness_ms",
            ).reindex(index=price.index, columns=instruments)
            <= max_oi_staleness_ms
        )
        oi_cutoff = derivatives_subset.groupby(
            "directory_date"
        )["sample_cutoff_ms"].first().reindex(price.index)
        oi_quality = oi_quality.mul(
            oi_cutoff.eq(cutoff_by_date.reindex(price.index)),
            axis=0,
        ).astype(bool)
        oi_change_quality = (
            oi_quality
            & oi_quality.shift(1, fill_value=False)
        )
        raw_features["relative_log_oi_change_1d"] = (
            np.log(oi_ccy.where(oi_ccy > 0)).diff()
        ).where(oi_change_quality)
        feature_names.extend(DERIVATIVES_FEATURE_NAMES)
    standardized = {
        name: cross_sectional_zscore(values)
        for name, values in raw_features.items()
    }
    forward_return_bps = log_price.diff().shift(-1) * 10_000
    forward_return_bps = forward_return_bps.where(
        asset_mark_quality
        & asset_mark_quality.shift(-1, fill_value=False)
    )
    valid_return = return_1d.where(
        asset_mark_quality
        & asset_mark_quality.shift(1, fill_value=False)
    )
    lagged_volatility = valid_return.rolling(
        window=14, min_periods=5
    ).std(ddof=0)

    rows = []
    targets = []
    rank_targets = []
    risk_scales = []
    date_index = list(log_price.index)
    for date_position, date_value in enumerate(date_index):
        required_positions = (
            date_position - 7,
            date_position - 3,
            date_position - 1,
            date_position,
            date_position + 1,
        )
        if date_position < 7 or date_position + 1 >= len(date_index):
            continue
        required_dates = [
            date_index[position]
            for position in required_positions
        ]
        if not interval_quality.reindex(
            required_dates, fill_value=False
        ).all():
            continue
        valid_instruments = []
        for instrument in instruments:
            if (
                not np.isfinite(
                    forward_return_bps.loc[
                        date_value, instrument
                    ]
                )
                or any(
                    not np.isfinite(
                        raw_features[name].loc[
                            date_value, instrument
                        ]
                    )
                    for name in feature_names
                )
                or not np.isfinite(
                    lagged_volatility.loc[
                        date_value, instrument
                    ]
                )
                or lagged_volatility.loc[
                    date_value, instrument
                ] <= 0
            ):
                continue
            valid_instruments.append(instrument)
        if len(valid_instruments) < min_cross_section_instruments:
            continue
        day_target = forward_return_bps.loc[
            date_value, valid_instruments
        ]
        relative_day_target = day_target - day_target.mean()
        rank_day_target = (
            day_target.rank(method="average", pct=True) - 0.5
        )
        for instrument in valid_instruments:
            rows.append({
                "decision_date": date_value,
                "instrument": instrument,
                **{
                    name: float(
                        standardized[name].loc[
                            date_value, instrument
                        ]
                    )
                    for name in feature_names
                },
            })
            targets.append(float(
                relative_day_target.loc[instrument]
            ))
            rank_targets.append(float(
                rank_day_target.loc[instrument]
            ))
            risk_scales.append(float(
                lagged_volatility.loc[date_value, instrument]
            ))
    feature_frame = pd.DataFrame(rows)
    if feature_frame.empty:
        raise ValueError("no complete cross-sectional feature rows")
    index = pd.MultiIndex.from_frame(
        feature_frame[["decision_date", "instrument"]]
    )
    feature_frame = feature_frame.set_index(index)[feature_names]
    target_series = pd.Series(targets, index=index, name="target_bps")
    rank_target_series = pd.Series(
        rank_targets, index=index, name="rank_target"
    )
    risk_scale_series = pd.Series(
        risk_scales, index=index, name="lagged_volatility"
    )
    dates = sorted(feature_frame.index.get_level_values(0).unique())
    return (
        feature_frame,
        target_series,
        rank_target_series,
        risk_scale_series,
        dates,
        feature_names,
    )


def fit_ridge(
    features: np.ndarray,
    target: np.ndarray,
    ridge: float,
) -> np.ndarray:
    design = np.column_stack([np.ones(len(features)), features])
    gram = design.T @ design / len(design)
    penalty = np.eye(gram.shape[0]) * ridge
    penalty[0, 0] = 0.0
    rhs = design.T @ target / len(design)
    return np.linalg.solve(gram + penalty, rhs)


def target_weights(
    predictions: pd.Series,
    *,
    tail_fraction: float,
    risk_scale: pd.Series | None = None,
    max_abs_weight: float | None = None,
) -> pd.Series:
    if not 0 < tail_fraction < 0.5:
        raise ValueError("tail fraction must be between zero and 0.5")
    count = max(1, int(math.floor(len(predictions) * tail_fraction)))
    ordered = predictions.sort_values(kind="stable")
    weights = pd.Series(0.0, index=predictions.index)
    short_index = ordered.index[:count]
    long_index = ordered.index[-count:]

    def side_allocation(index: pd.Index) -> pd.Series:
        if risk_scale is None:
            scores = pd.Series(1.0, index=index)
        else:
            side_risk = risk_scale.reindex(index)
            if (
                side_risk.isna().any()
                or not np.isfinite(side_risk).all()
                or (side_risk <= 0).any()
            ):
                raise ValueError("risk scale must be finite and positive")
            scores = 1.0 / side_risk
        if max_abs_weight is None:
            return scores / scores.sum() * 0.5
        if not 0 < max_abs_weight <= 0.5:
            raise ValueError(
                "max absolute weight must be in (0, 0.5]"
            )
        if len(index) * max_abs_weight < 0.5 - 1e-12:
            raise ValueError(
                "max absolute weight is infeasible for tail size"
            )
        allocation = pd.Series(0.0, index=index)
        remaining = list(index)
        budget = 0.5
        while remaining:
            raw = scores.loc[remaining]
            proposed = raw / raw.sum() * budget
            capped = proposed[proposed > max_abs_weight]
            if capped.empty:
                allocation.loc[remaining] = proposed
                break
            allocation.loc[capped.index] = max_abs_weight
            budget -= len(capped) * max_abs_weight
            remaining = [
                item for item in remaining
                if item not in set(capped.index)
            ]
        return allocation

    weights.loc[short_index] = -side_allocation(short_index)
    weights.loc[long_index] = side_allocation(long_index)
    return weights


def evaluate_fold(
    predictions: pd.Series,
    target_bps: pd.Series,
    spread_bps: pd.Series,
    eval_dates: list[str],
    *,
    tail_fraction: float,
    cost_mode: str,
    risk_scale: pd.Series | None = None,
    max_abs_weight: float | None = None,
) -> tuple[dict, list[dict], list[dict]]:
    if cost_mode not in {"maker", "taker"}:
        raise ValueError("cost mode must be maker or taker")
    instruments = sorted(
        predictions.index.get_level_values(1).unique()
    )
    instrument_spread = spread_bps.groupby(level=1).first()
    previous = pd.Series(0.0, index=instruments)
    daily = []
    positions = []
    for date_value in eval_dates:
        date_predictions = predictions.xs(date_value, level=0)
        full_predictions = date_predictions.reindex(instruments)
        date_risk_scale = (
            None
            if risk_scale is None
            else risk_scale.xs(date_value, level=0)
        )
        weights = target_weights(
            date_predictions,
            tail_fraction=tail_fraction,
            risk_scale=date_risk_scale,
            max_abs_weight=max_abs_weight,
        ).reindex(instruments, fill_value=0.0)
        turnover = (weights - previous).abs()
        date_target = target_bps.xs(
            date_value, level=0
        ).reindex(instruments)
        gross_contribution = (weights * date_target).fillna(0.0)
        gross = float(gross_contribution.sum())
        date_spread = spread_bps.xs(
            date_value, level=0
        ).reindex(instruments).fillna(instrument_spread)
        if cost_mode == "maker":
            asset_cost = turnover * 2.0
        else:
            asset_cost = turnover * (
                7.0 + 0.5 * date_spread
            )
        cost = float(asset_cost.sum())
        position_start = len(positions)
        for instrument in instruments:
            positions.append({
                "decision_date": date_value,
                "instrument": instrument,
                "prediction": float(
                    full_predictions.loc[instrument]
                ),
                "target_bps": float(
                    date_target.loc[instrument]
                ),
                "weight": float(weights.loc[instrument]),
                "turnover": float(turnover.loc[instrument]),
                "gross_contribution_bps": float(
                    gross_contribution.loc[instrument]
                ),
                "cost_bps": float(asset_cost.loc[instrument]),
                "net_contribution_bps": float(
                    gross_contribution.loc[instrument]
                    - asset_cost.loc[instrument]
                ),
            })
        daily.append({
            "decision_date": date_value,
            "gross_pnl_bps": gross,
            "cost_bps": cost,
            "net_pnl_bps": gross - cost,
            "turnover": float(turnover.sum()),
        })
        previous = weights
    exit_turnover = previous.abs()
    final_spread = spread_bps.xs(
        eval_dates[-1], level=0
    ).reindex(instruments).fillna(instrument_spread)
    if cost_mode == "maker":
        exit_asset_cost = exit_turnover * 2.0
    else:
        exit_asset_cost = exit_turnover * (
            7.0 + 0.5 * final_spread
        )
    exit_cost = float(exit_asset_cost.sum())
    daily[-1]["cost_bps"] += exit_cost
    daily[-1]["net_pnl_bps"] -= exit_cost
    daily[-1]["turnover"] += float(exit_turnover.sum())
    for offset, instrument in enumerate(instruments):
        row = positions[position_start + offset]
        row["turnover"] += float(exit_turnover.loc[instrument])
        row["cost_bps"] += float(exit_asset_cost.loc[instrument])
        row["net_contribution_bps"] -= float(
            exit_asset_cost.loc[instrument]
        )
    net = np.asarray(
        [row["net_pnl_bps"] for row in daily],
        dtype=np.float64,
    )
    cumulative = np.cumsum(net)
    drawdown = cumulative - np.maximum.accumulate(
        np.r_[0.0, cumulative]
    )[1:]
    daily_std = float(np.std(net, ddof=1)) if len(net) > 1 else 0.0
    metrics = {
        "days": len(daily),
        "gross_pnl_bps": float(sum(
            row["gross_pnl_bps"] for row in daily
        )),
        "cost_bps": float(sum(row["cost_bps"] for row in daily)),
        "net_pnl_bps": float(net.sum()),
        "daily_sharpe": (
            float(np.mean(net) / daily_std * math.sqrt(365))
            if daily_std > 0
            else float("nan")
        ),
        "positive_day_fraction": float(np.mean(net > 0)),
        "max_drawdown_bps": float(-drawdown.min()),
        "turnover": float(sum(row["turnover"] for row in daily)),
    }
    return metrics, daily, positions


def summarize_folds(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    keys = ["model", "tail_fraction", "cost_mode"]
    for values, group in frame.groupby(keys):
        model, tail_fraction, cost_mode = values
        rows.append({
            "model": model,
            "tail_fraction": float(tail_fraction),
            "cost_mode": cost_mode,
            "folds": len(group),
            "total_net_pnl_bps": float(group["net_pnl_bps"].sum()),
            "median_fold_net_pnl_bps": float(
                group["net_pnl_bps"].median()
            ),
            "worst_fold_net_pnl_bps": float(
                group["net_pnl_bps"].min()
            ),
            "positive_fold_fraction": float(
                (group["net_pnl_bps"] > 0).mean()
            ),
            "median_daily_sharpe": float(
                group["daily_sharpe"].median()
            ),
            "worst_fold_drawdown_bps": float(
                group["max_drawdown_bps"].max()
            ),
        })
    return pd.DataFrame(rows).sort_values(
        [
            "cost_mode",
            "positive_fold_fraction",
            "median_fold_net_pnl_bps",
        ],
        ascending=[True, False, False],
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--liquidity-dir", type=Path, required=True)
    parser.add_argument("--mark-panel-dir", type=Path, required=True)
    parser.add_argument(
        "--ticker-panel-dir",
        type=Path,
        help="optional synchronized daily Ticker panel",
    )
    parser.add_argument(
        "--derivatives-panel-dir",
        type=Path,
        help="optional synchronized daily OI/funding panel",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument(
        "--controls",
        default="BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP",
    )
    parser.add_argument(
        "--exclude-instruments",
        default="",
        help="comma-separated development sensitivity exclusions",
    )
    parser.add_argument(
        "--market-instrument", default="BTC-USDT-SWAP"
    )
    parser.add_argument("--min-train-days", type=int, default=22)
    parser.add_argument("--folds", type=int, default=4)
    parser.add_argument("--tail-fractions", default="0.20,0.30")
    parser.add_argument("--ridge", type=float, default=1e-2)
    parser.add_argument(
        "--ridge-target-mode",
        choices=("raw", "rank"),
        default="raw",
    )
    parser.add_argument(
        "--portfolio-weighting",
        choices=("equal", "inverse-vol"),
        default="equal",
    )
    parser.add_argument(
        "--max-asset-weight",
        type=float,
        default=0.10,
        help="used only with inverse-vol portfolio weighting",
    )
    parser.add_argument(
        "--max-cross-section-staleness-ms",
        type=float,
        default=60_000,
    )
    parser.add_argument(
        "--min-cross-section-instruments",
        type=int,
        default=20,
    )
    parser.add_argument(
        "--max-oi-staleness-ms",
        type=float,
        default=60_000,
    )
    args = parser.parse_args()

    controls = {
        item.strip()
        for item in args.controls.split(",")
        if item.strip()
    }
    excluded_instruments = {
        item.strip()
        for item in args.exclude_instruments.split(",")
        if item.strip()
    }
    liquidity = pd.read_csv(
        args.liquidity_dir / "liquidity_summary.csv"
    ).set_index("instrument")
    marks = pd.read_csv(
        args.mark_panel_dir / "daily_mark_samples.csv"
    )
    mark_metadata = json.loads(
        (args.mark_panel_dir / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    ticker_samples = None
    ticker_metadata = None
    ticker_qualified = None
    if args.ticker_panel_dir is not None:
        ticker_samples = pd.read_csv(
            args.ticker_panel_dir / "daily_ticker_samples.csv"
        )
        ticker_metadata = json.loads(
            (
                args.ticker_panel_dir / "metadata.json"
            ).read_text(encoding="utf-8")
        )
        ticker_qualified = set(
            ticker_metadata["qualified_instruments"]
        )
    derivatives_samples = None
    derivatives_metadata = None
    if args.derivatives_panel_dir is not None:
        derivatives_samples = pd.read_csv(
            args.derivatives_panel_dir
            / "daily_derivatives_samples.csv"
        )
        derivatives_metadata = json.loads(
            (
                args.derivatives_panel_dir / "metadata.json"
            ).read_text(encoding="utf-8")
        )
    instruments = sorted(
        set(mark_metadata["qualified_instruments"])
        - controls
        - excluded_instruments
    )
    if ticker_qualified is not None:
        instruments = sorted(set(instruments) & ticker_qualified)
    (
        features,
        target,
        rank_target,
        risk_scale,
        dates,
        feature_names,
    ) = build_feature_panel(
        marks,
        instruments,
        market_instrument=args.market_instrument,
        max_staleness_ms=args.max_cross_section_staleness_ms,
        ticker_samples=ticker_samples,
        derivatives_samples=derivatives_samples,
        max_oi_staleness_ms=args.max_oi_staleness_ms,
        min_cross_section_instruments=(
            args.min_cross_section_instruments
        ),
    )
    print(
        f"instruments={len(instruments)} "
        f"complete_feature_dates={len(dates)}",
        flush=True,
    )
    static_spread = pd.Series(
        [
            float(liquidity.loc[
                instrument, "p90_spread_bps"
            ])
            for _, instrument in features.index
        ],
        index=features.index,
        name="spread_bps",
    )
    folds = expanding_folds(
        len(dates), args.min_train_days, args.folds
    )
    tail_fractions = [
        float(item)
        for item in args.tail_fractions.split(",")
        if item.strip()
    ]

    fold_rows = []
    daily_rows = []
    position_rows = []
    coefficient_rows = []
    fold_definitions = []
    for fold_index, (fit_slice, eval_slice) in enumerate(folds):
        fit_dates = dates[fit_slice]
        eval_dates = dates[eval_slice]
        fit_mask = features.index.get_level_values(0).isin(
            fit_dates
        )
        eval_mask = features.index.get_level_values(0).isin(
            eval_dates
        )
        x_fit = features.loc[fit_mask].to_numpy()
        training_target = (
            rank_target
            if args.ridge_target_mode == "rank"
            else target
        )
        y_fit = training_target.loc[fit_mask].to_numpy()
        coefficients = fit_ridge(x_fit, y_fit, args.ridge)
        coefficient_rows.append({
            "fold": fold_index,
            "intercept": float(coefficients[0]),
            **{
                name: float(value)
                for name, value in zip(
                    feature_names, coefficients[1:]
                )
            },
        })
        ridge_predictions = pd.Series(
            coefficients[0]
            + features.loc[eval_mask].to_numpy()
            @ coefficients[1:],
            index=features.loc[eval_mask].index,
        )
        predictions = {
            "ridge": ridge_predictions,
            "reversal_1d": -features.loc[
                eval_mask, "relative_return_1d"
            ],
            "momentum_7d": features.loc[
                eval_mask, "relative_return_7d"
            ],
        }
        fold_definitions.append({
            "fold": fold_index,
            "fit_start": fit_dates[0],
            "fit_end": fit_dates[-1],
            "eval_start": eval_dates[0],
            "eval_end": eval_dates[-1],
        })
        for model_name, model_predictions in predictions.items():
            for tail_fraction in tail_fractions:
                for cost_mode in ("maker", "taker"):
                    metrics, daily, positions = evaluate_fold(
                        model_predictions,
                        target.loc[eval_mask],
                        static_spread.loc[eval_mask],
                        eval_dates,
                        tail_fraction=tail_fraction,
                        cost_mode=cost_mode,
                        risk_scale=(
                            risk_scale.loc[eval_mask]
                            if args.portfolio_weighting
                            == "inverse-vol"
                            else None
                        ),
                        max_abs_weight=(
                            args.max_asset_weight
                            if args.portfolio_weighting
                            == "inverse-vol"
                            else None
                        ),
                    )
                    metrics.update({
                        "fold": fold_index,
                        "model": model_name,
                        "tail_fraction": tail_fraction,
                        "cost_mode": cost_mode,
                    })
                    fold_rows.append(metrics)
                    for row in daily:
                        row.update({
                            "fold": fold_index,
                            "model": model_name,
                            "tail_fraction": tail_fraction,
                            "cost_mode": cost_mode,
                        })
                        daily_rows.append(row)
                    for row in positions:
                        row.update({
                            "fold": fold_index,
                            "model": model_name,
                            "tail_fraction": tail_fraction,
                            "cost_mode": cost_mode,
                        })
                        position_rows.append(row)

    fold_frame = pd.DataFrame(fold_rows)
    summary = summarize_folds(fold_frame)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    fold_frame.to_csv(
        args.out_dir / "cross_sectional_folds.csv", index=False
    )
    pd.DataFrame(daily_rows).to_csv(
        args.out_dir / "cross_sectional_daily.csv", index=False
    )
    pd.DataFrame(position_rows).to_csv(
        args.out_dir / "cross_sectional_positions.csv",
        index=False,
    )
    pd.DataFrame(coefficient_rows).to_csv(
        args.out_dir / "ridge_coefficients.csv", index=False
    )
    summary.to_csv(
        args.out_dir / "cross_sectional_summary.csv", index=False
    )
    metadata = {
        "probe_schema_version": "rl-cross-sectional-probe-v1",
        "warning": (
            "Daily close-to-close development alpha screen; not an "
            "executable HFT backtest or final Sharpe."
        ),
        "instruments": instruments,
        "controls": sorted(controls),
        "excluded_instruments": sorted(excluded_instruments),
        "mark_panel_metadata": mark_metadata,
        "feature_names": feature_names,
        "ticker_panel_metadata": ticker_metadata,
        "derivatives_panel_metadata": derivatives_metadata,
        "dates": dates,
        "fold_definitions": fold_definitions,
        "ridge": args.ridge,
        "ridge_target_mode": args.ridge_target_mode,
        "portfolio_weighting": args.portfolio_weighting,
        "max_asset_weight": (
            args.max_asset_weight
            if args.portfolio_weighting == "inverse-vol"
            else None
        ),
        "max_cross_section_staleness_ms": (
            args.max_cross_section_staleness_ms
        ),
        "min_cross_section_instruments": (
            args.min_cross_section_instruments
        ),
        "max_oi_staleness_ms": args.max_oi_staleness_ms,
        "summary": summary.replace(
            {np.nan: None}
        ).to_dict(orient="records"),
    }
    (args.out_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        summary.to_string(index=False),
        flush=True,
    )


if __name__ == "__main__":
    main()
