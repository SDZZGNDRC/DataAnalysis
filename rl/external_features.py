"""Strict backward-as-of alignment for cached external OKX features."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

EXTERNAL_FEATURE_NAMES = (
    "mark_index_basis_bps",
    "mid_index_basis_bps",
    "mark_mid_deviation_bps",
    "mark_index_basis_change_5m_bps",
    "mark_index_basis_change_1h_bps",
    "open_interest_change_5m_bps",
    "open_interest_change_1h_bps",
    "open_interest_change_4h_bps",
    "funding_rate_bps",
    "funding_premium_bps",
    "open_interest_is_fresh",
    "funding_is_fresh",
)


@dataclass(frozen=True)
class ExternalFeatureStore:
    metadata: dict
    mark_ts_ms: np.ndarray
    mark_px: np.ndarray
    index_ts_ms: np.ndarray
    index_px: np.ndarray
    oi_ts_ms: np.ndarray
    oi_ccy: np.ndarray
    funding_ts_ms: np.ndarray
    funding_rate: np.ndarray
    funding_premium: np.ndarray

    @classmethod
    def load(cls, path: Path) -> "ExternalFeatureStore":
        with np.load(path) as source:
            metadata = json.loads(str(source["metadata"].item()))
            if (
                metadata.get("external_feature_schema_version")
                != "rl-external-features-v1"
            ):
                raise ValueError("unsupported external feature schema")
            store = cls(
                metadata=metadata,
                mark_ts_ms=source["mark_ts_ms"].copy(),
                mark_px=source["mark_markPx"].copy(),
                index_ts_ms=source["index_ts_ms"].copy(),
                index_px=source["index_idxPx"].copy(),
                oi_ts_ms=source["open_interest_ts_ms"].copy(),
                oi_ccy=source["open_interest_oiCcy"].copy(),
                funding_ts_ms=source["funding_ts_ms"].copy(),
                funding_rate=source["funding_fundingRate"].copy(),
                funding_premium=source["funding_premium"].copy(),
            )
        store.validate()
        return store

    def validate(self) -> None:
        pairs = (
            (self.mark_ts_ms, self.mark_px, "mark"),
            (self.index_ts_ms, self.index_px, "index"),
            (self.oi_ts_ms, self.oi_ccy, "open_interest"),
            (self.funding_ts_ms, self.funding_rate, "funding"),
            (self.funding_ts_ms, self.funding_premium, "premium"),
        )
        for timestamps, values, name in pairs:
            if len(timestamps) != len(values):
                raise ValueError(f"{name} timestamp/value length mismatch")
            if len(timestamps) and np.any(np.diff(timestamps) <= 0):
                raise ValueError(f"{name} timestamps are not strictly increasing")
            if not np.isfinite(values).all():
                raise ValueError(f"{name} contains non-finite values")


def backward_asof(
    query_ts_ms: np.ndarray,
    source_ts_ms: np.ndarray,
    source_values: np.ndarray,
    *,
    max_staleness_ms: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return only observations published at or before each query."""
    if len(source_ts_ms) == 0:
        return (
            np.zeros(len(query_ts_ms), dtype=np.float64),
            np.zeros(len(query_ts_ms), dtype=bool),
        )
    indices = np.searchsorted(
        source_ts_ms, query_ts_ms, side="right"
    ) - 1
    valid_index = indices >= 0
    safe_indices = np.maximum(indices, 0)
    staleness = np.where(
        valid_index,
        query_ts_ms - source_ts_ms[safe_indices],
        max_staleness_ms + 1,
    )
    valid = (
        valid_index
        & (staleness >= 0)
        & (staleness <= max_staleness_ms)
    )
    result = np.zeros(len(query_ts_ms), dtype=np.float64)
    result[valid] = source_values[safe_indices[valid]]
    return result, valid


def _safe_basis_bps(
    numerator: np.ndarray,
    denominator: np.ndarray,
    valid: np.ndarray,
) -> np.ndarray:
    output = np.zeros(len(numerator), dtype=np.float64)
    usable = valid & (numerator > 0) & (denominator > 0)
    output[usable] = (
        numerator[usable] / denominator[usable] - 1.0
    ) * 10_000
    return np.clip(output, -200.0, 200.0)


def augment_segment_features(
    segment: tuple,
    store: ExternalFeatureStore,
) -> tuple:
    segment_id, features, mids, timestamps_ms = segment
    mark, mark_valid = backward_asof(
        timestamps_ms,
        store.mark_ts_ms,
        store.mark_px,
        max_staleness_ms=10_000,
    )
    index, index_valid = backward_asof(
        timestamps_ms,
        store.index_ts_ms,
        store.index_px,
        max_staleness_ms=10_000,
    )
    price_valid = mark_valid & index_valid
    mark_index_basis = _safe_basis_bps(
        mark, index, price_valid
    )
    mid_index_basis = _safe_basis_bps(
        mids, index, index_valid
    )
    mark_mid_deviation = _safe_basis_bps(
        mark, mids, mark_valid
    )

    basis_changes = []
    for lag_ms in (5 * 60_000, 60 * 60_000):
        previous_mark, previous_mark_valid = backward_asof(
            timestamps_ms - lag_ms,
            store.mark_ts_ms,
            store.mark_px,
            max_staleness_ms=10_000,
        )
        previous_index, previous_index_valid = backward_asof(
            timestamps_ms - lag_ms,
            store.index_ts_ms,
            store.index_px,
            max_staleness_ms=10_000,
        )
        previous_basis = _safe_basis_bps(
            previous_mark,
            previous_index,
            previous_mark_valid & previous_index_valid,
        )
        valid_change = (
            price_valid
            & previous_mark_valid
            & previous_index_valid
        )
        change = np.zeros(len(features), dtype=np.float64)
        change[valid_change] = (
            mark_index_basis[valid_change]
            - previous_basis[valid_change]
        )
        basis_changes.append(np.clip(change, -200.0, 200.0))

    current_oi, current_oi_valid = backward_asof(
        timestamps_ms,
        store.oi_ts_ms,
        store.oi_ccy,
        max_staleness_ms=5 * 60_000,
    )
    oi_changes = []
    for lag_ms in (5 * 60_000, 60 * 60_000, 4 * 60 * 60_000):
        previous_oi, previous_oi_valid = backward_asof(
            timestamps_ms - lag_ms,
            store.oi_ts_ms,
            store.oi_ccy,
            max_staleness_ms=5 * 60_000,
        )
        valid_change = (
            current_oi_valid
            & previous_oi_valid
            & (current_oi > 0)
            & (previous_oi > 0)
        )
        change = np.zeros(len(features), dtype=np.float64)
        change[valid_change] = (
            np.log(current_oi[valid_change])
            - np.log(previous_oi[valid_change])
        ) * 10_000
        oi_changes.append(np.clip(change, -500.0, 500.0))

    funding_rate, funding_valid = backward_asof(
        timestamps_ms,
        store.funding_ts_ms,
        store.funding_rate,
        max_staleness_ms=10 * 60_000,
    )
    funding_premium, premium_valid = backward_asof(
        timestamps_ms,
        store.funding_ts_ms,
        store.funding_premium,
        max_staleness_ms=10 * 60_000,
    )
    funding_valid &= premium_valid
    external = np.column_stack([
        mark_index_basis,
        mid_index_basis,
        mark_mid_deviation,
        *basis_changes,
        *oi_changes,
        np.clip(funding_rate * 10_000, -100.0, 100.0),
        np.clip(funding_premium * 10_000, -100.0, 100.0),
        current_oi_valid.astype(np.float64),
        funding_valid.astype(np.float64),
    ])
    if external.shape[1] != len(EXTERNAL_FEATURE_NAMES):
        raise AssertionError("external feature schema mismatch")
    return (
        segment_id,
        np.column_stack([features, external]),
        mids,
        timestamps_ms,
    )


def augment_segments(
    segments: list[tuple],
    store: ExternalFeatureStore,
) -> list[tuple]:
    return [
        augment_segment_features(segment, store)
        for segment in segments
    ]
