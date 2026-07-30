import json
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

from backtests.grid_search import _expand_grid
from evaluation.final_report import aggregate_parameter_tuples
from hftbacktest import BUY, BUY_EVENT, DEPTH_SNAPSHOT_EVENT
from hftbacktest.types import event_dtype
from hftbacktest_okx import process_books_file, process_trades_file
from rl.policy_core import RLPolicyCore, terminal_liquidation_cost
from rl.gym_env import load_manifest
from scripts.convert_segments_npz import (
    _existing_segment_is_valid,
    _validate_segment_data,
    _write_metadata,
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

    def cancel(self, _asset, order_id, _wait):
        self.cancellations.append(order_id)
        return 0

    def submit_buy_order(self, _asset, order_id, price, qty, *_args):
        self.submissions.append(("buy", order_id, price, qty))
        return 0

    def submit_sell_order(self, _asset, order_id, price, qty, *_args):
        self.submissions.append(("sell", order_id, price, qty))
        return 0


def test_rl_core_strong_action_uses_one_unique_two_lot_order_and_cancels():
    hbt = _Hbt()
    core = RLPolicyCore(
        step_ns=500_000_000,
        max_position=0.1,
        order_qty=0.01,
        report_notional=100_000,
    )
    assert core.apply_action(hbt, 0) == 0
    assert hbt.submissions == [("buy", 1_000_000, 100.0, 0.02)]

    hbt.active = [_Order(order_id=1_000_000)]
    assert core.apply_action(hbt, 2) == 0
    assert hbt.cancellations == [1_000_000]

    hbt.active = []
    assert core.apply_action(hbt, 1) == 0
    assert hbt.submissions[-1][1] == 1_000_001


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
