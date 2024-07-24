import pandas as pd
import pytest
from pathlib import Path
import os
import numpy as np

from .TA import TA

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    ask_ta = TA(
        N = 5, instId=instId,
        side='ask',
        start=start, end=end, path=path
    )
    bid_ta = TA(
        N = 5, instId=instId,
        side='bid',
        start=start, end=end, path=path
    )
    
    assert np.isclose(ask_ta[pd.Timestamp(   0, unit='ms')], 1.5)
    assert np.isclose(ask_ta[pd.Timestamp(1000, unit='ms')], 3.0)
    assert np.isclose(ask_ta[pd.Timestamp(2000, unit='ms')], 3.0)
    assert np.isclose(bid_ta[pd.Timestamp(   0, unit='ms')], 1.5)
    assert np.isclose(bid_ta[pd.Timestamp(1000, unit='ms')], 1.5)
    assert np.isclose(bid_ta[pd.Timestamp(2000, unit='ms')], 8.7)

def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    ask_ta = TA(
        N = 4, instId=instId,
        side='ask',
        start=start, end=end, path=path
    )
    bid_ta = TA(
        N = 4, instId=instId,
        side='bid',
        start=start, end=end, path=path
    )
    
    assert np.isclose(ask_ta[pd.Timestamp(   0, unit='ms')], 1.4)
    assert np.isclose(ask_ta[pd.Timestamp(1000, unit='ms')], 2.5)
    assert np.isclose(ask_ta[pd.Timestamp(2000, unit='ms')], 2.5)
    assert np.isclose(bid_ta[pd.Timestamp(   0, unit='ms')], 1.4)
    assert np.isclose(bid_ta[pd.Timestamp(1000, unit='ms')], 1.0)
    assert np.isclose(bid_ta[pd.Timestamp(2000, unit='ms')], 1.1)

def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 4000, 1000
    
    ask_ta = TA(
        N = 4, instId=instId,
        side='ask',
        start=start, end=end, path=path
    )
    bid_ta = TA(
        N = 4, instId=instId,
        side='bid',
        start=start, end=end, path=path
    )

    assert np.isclose(ask_ta[pd.Timestamp(   0, unit='ms')], 1.4)
    assert np.isclose(ask_ta[pd.Timestamp(1000, unit='ms')], 2.5)
    assert np.isclose(ask_ta[pd.Timestamp(2000, unit='ms')], 2.5)
    assert np.isclose(ask_ta[pd.Timestamp(3000, unit='ms')], 2.5)
    assert np.isclose(ask_ta[pd.Timestamp(4000, unit='ms')], 1.5)
    assert np.isclose(bid_ta[pd.Timestamp(   0, unit='ms')], 1.4)
    assert np.isclose(bid_ta[pd.Timestamp(1000, unit='ms')], 1.0)
    assert np.isclose(bid_ta[pd.Timestamp(2000, unit='ms')], 1.1)
    assert np.isclose(bid_ta[pd.Timestamp(3000, unit='ms')], 1.1)
    assert np.isclose(bid_ta[pd.Timestamp(4000, unit='ms')], 0.8)
