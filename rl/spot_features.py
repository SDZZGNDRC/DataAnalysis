"""Strictly time-aligned BTC spot features for swap alpha research."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from rl.external_features import backward_asof

SPOT_FEATURE_NAMES = (
    "spot_swap_basis_bps",
    "spot_quote_imbalance",
    "spot_microprice_offset_bps",
    "spot_return_1s_bps",
    "spot_return_5s_bps",
    "spot_return_30s_bps",
    "spot_return_300s_bps",
    "spot_minus_swap_return_1s_bps",
    "spot_minus_swap_return_5s_bps",
    "spot_minus_swap_return_30s_bps",
    "spot_minus_swap_return_300s_bps",
    "spot_is_fresh",
)


@dataclass(frozen=True)
class SpotFeatureStore:
    metadata: dict
    timestamps_ms: np.ndarray
    last_px: np.ndarray
    bid_px: np.ndarray
    ask_px: np.ndarray
    bid_qty: np.ndarray
    ask_qty: np.ndarray

    @classmethod
    def load(cls, path: Path) -> "SpotFeatureStore":
        with np.load(path) as source:
            metadata = json.loads(str(source["metadata"].item()))
            if (
                metadata.get("spot_feature_schema_version")
                != "rl-spot-features-v1"
            ):
                raise ValueError("unsupported spot feature schema")
            store = cls(
                metadata=metadata,
                timestamps_ms=source["spot_ts_ms"].copy(),
                last_px=source["spot_last"].copy(),
                bid_px=source["spot_bidPx"].copy(),
                ask_px=source["spot_askPx"].copy(),
                bid_qty=source["spot_bidSz"].copy(),
                ask_qty=source["spot_askSz"].copy(),
            )
        store.validate()
        return store

    def validate(self) -> None:
        arrays = (
            self.last_px,
            self.bid_px,
            self.ask_px,
            self.bid_qty,
            self.ask_qty,
        )
        if any(len(array) != len(self.timestamps_ms) for array in arrays):
            raise ValueError("spot timestamp/value length mismatch")
        if len(self.timestamps_ms) and np.any(
            np.diff(self.timestamps_ms) <= 0
        ):
            raise ValueError("spot timestamps are not strictly increasing")
        if any(not np.isfinite(array).all() for array in arrays):
            raise ValueError("spot cache contains non-finite values")


def _asof_spot_mid(
    query_ts_ms: np.ndarray,
    store: SpotFeatureStore,
) -> tuple[np.ndarray, np.ndarray]:
    bid, bid_valid = backward_asof(
        query_ts_ms,
        store.timestamps_ms,
        store.bid_px,
        max_staleness_ms=5_000,
    )
    ask, ask_valid = backward_asof(
        query_ts_ms,
        store.timestamps_ms,
        store.ask_px,
        max_staleness_ms=5_000,
    )
    valid = bid_valid & ask_valid & (bid > 0) & (ask >= bid)
    mid = np.zeros(len(query_ts_ms), dtype=np.float64)
    mid[valid] = 0.5 * (bid[valid] + ask[valid])
    return mid, valid


def _return_bps(
    current: np.ndarray,
    previous: np.ndarray,
    valid: np.ndarray,
) -> np.ndarray:
    result = np.zeros(len(current), dtype=np.float64)
    usable = valid & (current > 0) & (previous > 0)
    result[usable] = np.log(
        current[usable] / previous[usable]
    ) * 10_000
    return np.clip(result, -500.0, 500.0)


def augment_segment_with_spot(
    segment: tuple,
    store: SpotFeatureStore,
    *,
    feed_delay_ms: int,
) -> tuple:
    if feed_delay_ms < 0:
        raise ValueError("spot feed delay must be non-negative")
    segment_id, features, swap_mid, timestamps_ms = segment
    available_ts = timestamps_ms - feed_delay_ms
    spot_mid, spot_valid = _asof_spot_mid(available_ts, store)
    bid_qty, bid_qty_valid = backward_asof(
        available_ts,
        store.timestamps_ms,
        store.bid_qty,
        max_staleness_ms=5_000,
    )
    ask_qty, ask_qty_valid = backward_asof(
        available_ts,
        store.timestamps_ms,
        store.ask_qty,
        max_staleness_ms=5_000,
    )
    quote_valid = spot_valid & bid_qty_valid & ask_qty_valid
    quote_imbalance = np.zeros(len(features), dtype=np.float64)
    quantity_sum = bid_qty + ask_qty
    usable_quote = quote_valid & (quantity_sum > 0)
    quote_imbalance[usable_quote] = (
        bid_qty[usable_quote] - ask_qty[usable_quote]
    ) / quantity_sum[usable_quote]

    bid, bid_valid = backward_asof(
        available_ts,
        store.timestamps_ms,
        store.bid_px,
        max_staleness_ms=5_000,
    )
    ask, ask_valid = backward_asof(
        available_ts,
        store.timestamps_ms,
        store.ask_px,
        max_staleness_ms=5_000,
    )
    microprice = np.zeros(len(features), dtype=np.float64)
    micro_valid = (
        usable_quote & bid_valid & ask_valid & (spot_mid > 0)
    )
    microprice[micro_valid] = (
        ask[micro_valid] * bid_qty[micro_valid]
        + bid[micro_valid] * ask_qty[micro_valid]
    ) / quantity_sum[micro_valid]
    micro_offset = _return_bps(
        microprice, spot_mid, micro_valid
    )
    basis = _return_bps(
        spot_mid,
        swap_mid,
        spot_valid & (swap_mid > 0),
    )

    spot_returns = []
    relative_returns = []
    for lag_ms in (1_000, 5_000, 30_000, 300_000):
        previous_spot, previous_spot_valid = _asof_spot_mid(
            available_ts - lag_ms, store
        )
        previous_swap, previous_swap_valid = backward_asof(
            timestamps_ms - lag_ms,
            timestamps_ms,
            swap_mid,
            max_staleness_ms=2_000,
        )
        spot_return = _return_bps(
            spot_mid,
            previous_spot,
            spot_valid & previous_spot_valid,
        )
        swap_return = _return_bps(
            swap_mid,
            previous_swap,
            previous_swap_valid,
        )
        relative = np.zeros(len(features), dtype=np.float64)
        valid_relative = (
            spot_valid
            & previous_spot_valid
            & previous_swap_valid
        )
        relative[valid_relative] = (
            spot_return[valid_relative]
            - swap_return[valid_relative]
        )
        spot_returns.append(spot_return)
        relative_returns.append(
            np.clip(relative, -500.0, 500.0)
        )

    spot_features = np.column_stack([
        basis,
        quote_imbalance,
        micro_offset,
        *spot_returns,
        *relative_returns,
        spot_valid.astype(np.float64),
    ])
    if spot_features.shape[1] != len(SPOT_FEATURE_NAMES):
        raise AssertionError("spot feature schema mismatch")
    return (
        segment_id,
        np.column_stack([features, spot_features]),
        swap_mid,
        timestamps_ms,
    )


def augment_segments_with_spot(
    segments: list[tuple],
    store: SpotFeatureStore,
    *,
    feed_delay_ms: int,
) -> list[tuple]:
    return [
        augment_segment_with_spot(
            segment, store, feed_delay_ms=feed_delay_ms
        )
        for segment in segments
    ]
