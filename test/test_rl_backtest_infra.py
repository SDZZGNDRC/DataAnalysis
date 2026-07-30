import json
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

from backtests.grid_search import _expand_grid
from backtests.worker_grid import _json_safe
from evaluation.final_report import aggregate_parameter_tuples
from hftbacktest import BUY, BUY_EVENT, DEPTH_SNAPSHOT_EVENT
from hftbacktest.types import event_dtype
from hftbacktest_okx import process_books_file, process_trades_file
from rl.alpha_probe import (
    evaluate_probe,
    fit_random_feature_probe,
    fit_ridge_probe,
    predict_probe,
)
from rl.policy_core import (
    OBS_DIM,
    OBSERVATION_NAMES,
    RLPolicyCore,
    terminal_liquidation_cost,
)
from rl.walk_forward_probe import (
    evaluate_persistent_signal,
    expanding_folds,
)
from rl.gym_env import activity_gate_penalty, load_manifest
from rl.external_features import backward_asof
from rl.spot_features import (
    SpotFeatureStore,
    augment_segment_with_spot,
)
from scripts.convert_segments_npz import (
    _existing_segment_is_valid,
    _validate_segment_data,
    _write_metadata,
)
from scripts.build_rl_external_features import (
    deduplicate_last_per_second,
    extract_root_entries,
)


def test_memory_converter_filters_exact_millisecond_bounds(tmp_path):
    books = {
        "data": [
            {
                "action": "snapshot",
                "localTs": str(ts),
                "data": [{
                    "ts": str(ts),
                    "bids": [["100", "1", "0", "1"]],
                    "asks": [["101", "1", "0", "1"]],
                }],
            }
            for ts in (999, 1000, 1001, 1002)
        ]
    }
    trades = {
        "data": [
            {
                "localTs": str(ts),
                "data": [{"ts": str(ts), "px": "100.5", "sz": "0.1", "side": "buy"}],
            }
            for ts in (999, 1000, 1001, 1002)
        ]
    }
    books_path = tmp_path / "books.json"
    trades_path = tmp_path / "trades.json"
    books_path.write_text(json.dumps(books), encoding="utf-8")
    trades_path.write_text(json.dumps(trades), encoding="utf-8")

    book_events, _ = process_books_file(
        str(books_path), 0, True, start_ts=1000, end_ts=1001
    )
    trade_events = process_trades_file(
        str(trades_path), 0, start_ts=1000, end_ts=1001
    )
    assert set(book_events["exch_ts"] // 1_000_000) == {1000, 1001}
    assert set(trade_events["exch_ts"] // 1_000_000) == {1000, 1001}


def test_market_data_parsers_ignore_okx_control_notices(tmp_path):
    notice = {
        "localTs": "1001",
        "event": "notice",
        "code": "64008",
        "msg": "service upgrade",
    }
    books = {
        "data": [
            {
                "action": "snapshot",
                "localTs": "1000",
                "data": [{
                    "ts": "1000",
                    "bids": [["100", "1", "0", "1"]],
                    "asks": [["101", "1", "0", "1"]],
                }],
            },
            notice,
        ]
    }
    trades = {
        "data": [
            {
                "localTs": "1000",
                "data": [{
                    "ts": "1000", "px": "100.5", "sz": "0.1", "side": "buy"
                }],
            },
            notice,
        ]
    }
    books_path = tmp_path / "books_notice.json"
    trades_path = tmp_path / "trades_notice.json"
    books_path.write_text(json.dumps(books), encoding="utf-8")
    trades_path.write_text(json.dumps(trades), encoding="utf-8")

    book_events, _ = process_books_file(str(books_path), 0, True)
    trade_events = process_trades_file(str(trades_path), 0)

    assert len(book_events) == 4
    assert len(trade_events) == 1


def test_persisted_npz_hash_detects_same_size_corruption(tmp_path):
    data = np.zeros(2, dtype=event_dtype)
    data[0]["ev"] = DEPTH_SNAPSHOT_EVENT | BUY_EVENT
    data[0]["exch_ts"] = 1_000_000_000
    data[0]["local_ts"] = 1_000_000_000
    data[0]["px"] = 100.0
    data[0]["qty"] = 1.0
    data[1] = data[0]
    data[1]["exch_ts"] = 1_001_000_000
    data[1]["local_ts"] = 1_001_000_000

    path = tmp_path / "segment.npz"
    np.savez_compressed(path, data=data)
    metadata = _validate_segment_data(data, 1000, 1001)
    _write_metadata(path, metadata)
    assert _existing_segment_is_valid(path, 1000, 1001)

    raw = bytearray(path.read_bytes())
    raw[len(raw) // 2] ^= 0x01
    path.write_bytes(raw)
    assert not _existing_segment_is_valid(path, 1000, 1001)


class _Iterator:
    def __init__(self, orders):
        self.orders = list(orders)
        self.index = 0

    def has_next(self):
        return self.index < len(self.orders)

    def get(self):
        order = self.orders[self.index]
        self.index += 1
        return order


class _Orders:
    def __init__(self, orders):
        self._orders = orders

    def values(self):
        return _Iterator(self._orders)


@dataclass
class _Order:
    order_id: int
    side: int = BUY
    leaves_qty: float = 0.02
    price: float = 100.0
    cancellable: bool = True


@dataclass
class _State:
    balance: float = 0.0
    position: float = 0.0
    fee: float = 0.0
    num_trades: float = 0.0
    trading_volume: float = 0.0
    trading_value: float = 0.0


class _Depth:
    best_bid = 100.0
    best_ask = 101.0
    best_bid_qty = 1.0
    best_ask_qty = 1.0
    best_bid_tick = 1000
    best_ask_tick = 1010
    lot_size = 0.01
    tick_size = 0.1

    def ask_qty_at_tick(self, _tick):
        return 1.0

    def bid_qty_at_tick(self, _tick):
        return 1.0


class _Hbt:
    def __init__(self):
        self.current_timestamp = 1_000_000_000
        self.state = _State()
        self.depth_obj = _Depth()
        self.active = []
        self.submissions = []
        self.cancellations = []

    def depth(self, _asset):
        return self.depth_obj

    def state_values(self, _asset):
        return self.state

    def orders(self, _asset):
        return _Orders(self.active)

    def clear_inactive_orders(self, _asset):
        return None

    def last_trades(self, _asset):
        return np.empty(0, dtype=event_dtype)

    def clear_last_trades(self, _asset):
        return None

    def cancel(self, _asset, order_id, _wait):
        self.cancellations.append(order_id)
        return 0

    def submit_buy_order(self, _asset, order_id, price, qty, *_args):
        self.submissions.append(("buy", order_id, price, qty))
        return 0

    def submit_sell_order(self, _asset, order_id, price, qty, *_args):
        self.submissions.append(("sell", order_id, price, qty))
        return 0


def test_rl_core_uses_target_position_and_keeps_young_matching_order():
    hbt = _Hbt()
    core = RLPolicyCore(
        step_ns=500_000_000,
        max_position=0.1,
        order_qty=0.01,
        report_notional=100_000,
    )
    assert core.apply_action(hbt, 0) == 0
    assert hbt.submissions == [("buy", 1_000_000, 100.0, 0.01)]

    hbt.active = [_Order(order_id=1_000_000)]
    hbt.current_timestamp += 2_000_000_000
    assert core.apply_action(hbt, 0) == 0
    assert hbt.cancellations == []
    assert len(hbt.submissions) == 1

    assert core.apply_action(hbt, 2) == 0
    assert hbt.cancellations == [1_000_000]

    hbt.active = []
    assert core.apply_action(hbt, 1) == 0
    assert hbt.submissions[-1][1] == 1_000_001
    diagnostics = core.diagnostics()
    assert diagnostics["decision_count"] == 4
    assert diagnostics["action_count_0"] == 2
    assert diagnostics["action_count_1"] == 1
    assert diagnostics["action_count_2"] == 1
    assert diagnostics["submit_successes"] == 2
    assert diagnostics["cancel_successes"] == 1
    assert diagnostics["signal_cancel_successes"] == 1
    assert diagnostics["mean_cancelled_order_age_ms"] == 2000


def test_rl_core_reprices_only_after_minimum_lifetime():
    hbt = _Hbt()
    core = RLPolicyCore(
        step_ns=2_000_000_000,
        max_position=0.1,
        order_qty=0.01,
        report_notional=100_000,
        min_order_lifetime_ns=5_000_000_000,
        max_order_lifetime_ns=15_000_000_000,
        reprice_threshold_ticks=1,
    )
    assert core.apply_action(hbt, 0) == 0
    hbt.active = [_Order(order_id=1_000_000, price=100.0)]
    hbt.depth_obj.best_bid = 99.8

    hbt.current_timestamp += 4_000_000_000
    assert core.apply_action(hbt, 0) == 0
    assert hbt.cancellations == []

    hbt.current_timestamp += 2_000_000_000
    assert core.apply_action(hbt, 0) == 0
    assert hbt.cancellations == [1_000_000]
    assert core.diagnostics()["reprice_cancel_successes"] == 1


def test_fill_recency_uses_trade_counter_and_terminal_cost_is_charged():
    hbt = _Hbt()
    core = RLPolicyCore(
        step_ns=500_000_000,
        max_position=0.1,
        order_qty=0.01,
        report_notional=100_000,
    )
    core.reset(hbt.state)
    first = core.observe(hbt)
    assert first.obs[17] == 1.0

    hbt.current_timestamp += 500_000_000
    hbt.state.num_trades = 1
    hbt.state.trading_volume = 0.01
    hbt.state.position = 0.01
    second = core.observe(hbt)
    assert second.obs[17] == 0.0
    assert second.traded_qty == 0.01
    assert second.obs[22] == 1.0
    diagnostics = core.diagnostics()
    assert diagnostics["fill_events"] == 1
    assert diagnostics["fill_qty"] == pytest.approx(0.01)

    cost = terminal_liquidation_cost(hbt.state, hbt.depth_obj, taker_fee=0.001)
    assert cost > 0


def test_explicit_parameter_combinations_do_not_form_cartesian_product():
    raw = {
        "_combinations": [
            {"alpha": 1, "beta": 10},
            {"alpha": 2, "beta": 20},
            {"alpha": 1, "beta": 10},
        ]
    }
    assert _expand_grid(raw) == [
        {"alpha": 1, "beta": 10},
        {"alpha": 2, "beta": 20},
    ]


def test_report_aggregation_uses_real_equity_and_cross_segment_returns():
    frame = pd.DataFrame([
        {
            "strategy": "rl_policy", "seg_index": 1, "seg_duration_h": 1,
            "status": "ok", "model_path": "m.zip", "equity": 10.0,
            "return": 0.0001, "sharpe": 10.0, "max_drawdown": 0.0002,
            "num_trades": 2, "fee": 1.0, "liquidation_cost": 0.5,
            "buyhold_return": 0.0, "report_notional": 100_000.0,
        },
        {
            "strategy": "rl_policy", "seg_index": 2, "seg_duration_h": 1,
            "status": "ok", "model_path": "m.zip", "equity": -5.0,
            "return": -0.00005, "sharpe": -3.0, "max_drawdown": 0.0003,
            "num_trades": 1, "fee": 0.5, "liquidation_cost": 0.2,
            "buyhold_return": 0.0, "report_notional": 100_000.0,
        },
    ])
    result, _ = aggregate_parameter_tuples(frame, 100_000.0)
    row = result.iloc[0]
    assert row["sum_equity"] == 5.0
    assert row["total_return"] == 0.00005
    assert row["win_rate"] == 0.5
    assert np.isfinite(row["return_t_stat"])


def test_rl_loader_rejects_legacy_manifest(tmp_path):
    path = tmp_path / "manifest.csv"
    pd.DataFrame([{
        "npz_path": "legacy.npz",
        "split": "train",
    }]).to_csv(path, index=False)
    with pytest.raises(ValueError, match="exact-segment-v2"):
        load_manifest(str(path), "train")


def test_rl_loader_applies_deterministic_resource_filters(tmp_path):
    path = tmp_path / "manifest.csv"
    pd.DataFrame([
        {
            "npz_path": f"{index}.npz",
            "split": "train",
            "schema_version": "exact-segment-v2",
            "actual_start_ns": index,
            "seg_index": index,
            "event_count": event_count,
        }
        for index, event_count in ((3, 30), (1, 10), (2, 20))
    ]).to_csv(path, index=False)

    assert load_manifest(
        str(path), "train", max_events=20, max_segments=1
    ) == ["1.npz"]


def test_alpha_probe_fits_train_only_thresholds_and_charges_cost():
    x_fit = np.arange(100, dtype=float).reshape(-1, 1)
    y_fit = 0.1 * x_fit[:, 0] - 5.0
    model = fit_ridge_probe(x_fit, y_fit, ridge=1e-8)

    x_eval = np.array([[0.0], [5.0], [95.0], [100.0]])
    y_eval = np.array([-6.0, -5.0, 5.0, 6.0])
    predictions = predict_probe(x_eval, model)
    metrics = evaluate_probe(
        y_eval,
        predictions,
        np.array([1, 1, 2, 2]),
        lower_threshold_bps=model["lower_threshold_bps"],
        upper_threshold_bps=model["upper_threshold_bps"],
        round_trip_cost_bps=4.0,
    )

    assert metrics["signal_coverage"] == 1.0
    assert metrics["gross_edge_bps"] == pytest.approx(5.5)
    assert metrics["net_edge_bps"] == pytest.approx(1.5)
    assert metrics["positive_segment_fraction"] == 1.0


def test_activity_gate_changes_validation_score_not_training_reward():
    assert activity_gate_penalty(0, min_trades=1, ineligible_penalty=100_000) == 100_000
    assert activity_gate_penalty(1, min_trades=1, ineligible_penalty=100_000) == 0
    assert activity_gate_penalty(0, min_trades=0, ineligible_penalty=100_000) == 0


def test_v3_observation_schema_and_result_json_are_strict():
    assert len(OBSERVATION_NAMES) == OBS_DIM == 43
    assert _json_safe({"sharpe": np.nan, "count": np.int64(3)}) == {
        "sharpe": None,
        "count": 3,
    }


def test_random_feature_probe_is_deterministic_and_predicts():
    features = np.arange(40, dtype=float).reshape(20, 2)
    target = np.sin(features[:, 0])
    first = fit_random_feature_probe(
        features,
        target,
        ridge=1e-2,
        n_random_features=8,
        seed=7,
    )
    second = fit_random_feature_probe(
        features,
        target,
        ridge=1e-2,
        n_random_features=8,
        seed=7,
    )
    assert np.allclose(
        predict_probe(features, first),
        predict_probe(features, second),
    )


def test_expanding_walk_forward_folds_are_chronological():
    folds = expanding_folds(
        n_segments=14,
        min_train_segments=8,
        n_folds=3,
    )
    assert folds == [
        (slice(0, 8), slice(8, 10)),
        (slice(0, 10), slice(10, 12)),
        (slice(0, 12), slice(12, 14)),
    ]


def test_persistent_signal_charges_turnover_and_terminal_exit():
    features = np.arange(20, dtype=float).reshape(-1, 1)
    target = features[:, 0]
    model = fit_ridge_probe(
        features, target, ridge=1e-8, tail_fraction=0.1
    )
    mids = np.linspace(100.0, 102.0, num=20)
    result = evaluate_persistent_signal(
        [(1, features, mids)],
        model,
        horizon_steps=5,
        one_way_cost_bps=2.0,
    )
    assert result["turnover_units"] > 0
    assert result["cost_bps"] == pytest.approx(
        2.0 * result["turnover_units"]
    )


def test_persistent_barrier_caps_loss_and_blocks_immediate_reentry():
    features = np.arange(100, dtype=float).reshape(-1, 1)
    model = fit_ridge_probe(
        features,
        features[:, 0],
        ridge=1e-8,
        tail_fraction=0.1,
    )
    mids = np.r_[
        np.full(90, 100.0),
        np.linspace(100.0, 98.0, num=10),
    ]
    fixed = evaluate_persistent_signal(
        [(1, features, mids)],
        model,
        horizon_steps=50,
        one_way_cost_bps=0.0,
        exit_mode="fixed",
    )
    guarded = evaluate_persistent_signal(
        [(1, features, mids)],
        model,
        horizon_steps=50,
        one_way_cost_bps=0.0,
        exit_mode="fixed_barrier",
        stop_loss_bps=25.0,
        take_profit_bps=100.0,
    )
    assert guarded["net_pnl_bps"] > fixed["net_pnl_bps"]
    assert guarded["exposure_fraction"] < fixed["exposure_fraction"]
    assert guarded["round_trip_equivalents"] <= (
        fixed["round_trip_equivalents"]
    )


def test_external_feature_parser_filters_instrument_and_keeps_last_second():
    root = {
        "data": [
            {
                "arg": {"instId": "BTC-USDT"},
                "data": [
                    {"ts": "1000", "idxPx": "100"},
                    {"ts": "1500", "idxPx": "101"},
                    {"ts": "2000", "idxPx": "102"},
                ],
            },
            {
                "arg": {"instId": "ETH-USDT"},
                "data": [{"ts": "2000", "idxPx": "999"}],
            },
        ]
    }
    timestamps, values = extract_root_entries(
        root, inst_id="BTC-USDT", fields=("idxPx",)
    )
    timestamps, values = deduplicate_last_per_second(
        timestamps, values
    )
    assert timestamps.tolist() == [1500, 2000]
    assert values["idxPx"].tolist() == [101.0, 102.0]


def test_external_asof_never_reads_future_observation():
    values, valid = backward_asof(
        np.array([500, 1500, 2501]),
        np.array([1000, 2000]),
        np.array([10.0, 20.0]),
        max_staleness_ms=600,
    )
    assert values.tolist() == [0.0, 10.0, 20.0]
    assert valid.tolist() == [False, True, True]


def test_external_asof_accepts_empty_source():
    values, valid = backward_asof(
        np.array([1000, 2000]),
        np.array([], dtype=np.int64),
        np.array([], dtype=np.float64),
        max_staleness_ms=1000,
    )
    assert values.tolist() == [0.0, 0.0]
    assert valid.tolist() == [False, False]


def test_spot_feed_delay_blocks_not_yet_available_update():
    store = SpotFeatureStore(
        metadata={},
        timestamps_ms=np.array([950]),
        last_px=np.array([100.0]),
        bid_px=np.array([99.9]),
        ask_px=np.array([100.1]),
        bid_qty=np.array([2.0]),
        ask_qty=np.array([1.0]),
    )
    segment = (
        1,
        np.zeros((1, 2)),
        np.array([100.0]),
        np.array([1000]),
    )
    delayed = augment_segment_with_spot(
        segment, store, feed_delay_ms=100
    )
    immediate = augment_segment_with_spot(
        segment, store, feed_delay_ms=0
    )
    assert delayed[1][0, -1] == 0.0
    assert immediate[1][0, -1] == 1.0
