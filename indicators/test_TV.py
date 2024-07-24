import pandas as pd
import pytest
from pathlib import Path
import os
import numpy as np

from .TV import TV

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    ask_tv = TV(
        N = 5, instId=instId,
        side='ask',
        start=start, end=end, path=path
    )
    bid_tv = TV(
        N = 5, instId=instId,
        side='bid',
        start=start, end=end, path=path
    )
    
    assert np.isclose(ask_tv[pd.Timestamp(   0, unit='ms')], 152)
    assert np.isclose(ask_tv[pd.Timestamp(1000, unit='ms')], 306.09999999999997)
    assert np.isclose(ask_tv[pd.Timestamp(2000, unit='ms')], 306.09999999999997)
    assert np.isclose(bid_tv[pd.Timestamp(   0, unit='ms')], 146.5)
    assert np.isclose(bid_tv[pd.Timestamp(1000, unit='ms')], 146.5)
    assert np.isclose(bid_tv[pd.Timestamp(2000, unit='ms')], 848.5)


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    ask_tv = TV(
        N = 4, instId=instId,
        side='ask',
        start=start, end=end, path=path
    )
    bid_tv = TV(
        N = 4, instId=instId,
        side='bid',
        start=start, end=end, path=path
    )
    
    assert np.isclose(ask_tv[pd.Timestamp(   0, unit='ms')], 141.6)
    assert np.isclose(ask_tv[pd.Timestamp(1000, unit='ms')], 254.09999999999997)
    assert np.isclose(ask_tv[pd.Timestamp(2000, unit='ms')], 254.09999999999997)
    assert np.isclose(bid_tv[pd.Timestamp(   0, unit='ms')], 137.0)
    assert np.isclose(bid_tv[pd.Timestamp(1000, unit='ms')], 97.0)
    assert np.isclose(bid_tv[pd.Timestamp(2000, unit='ms')], 107.3)

def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 4000, 1000
    
    ask_tv = TV(
        N = 4, instId=instId,
        side='ask',
        start=start, end=end, path=path
    )
    bid_tv = TV(
        N = 4, instId=instId,
        side='bid',
        start=start, end=end, path=path
    )
    
    assert np.isclose(ask_tv[pd.Timestamp(   0, unit='ms')], 141.6)
    assert np.isclose(ask_tv[pd.Timestamp(1000, unit='ms')], 254.09999999999997)
    assert np.isclose(ask_tv[pd.Timestamp(2000, unit='ms')], 254.09999999999997)
    assert np.isclose(ask_tv[pd.Timestamp(3000, unit='ms')], 254.09999999999997)
    assert np.isclose(ask_tv[pd.Timestamp(4000, unit='ms')], 153.2)
    assert np.isclose(bid_tv[pd.Timestamp(   0, unit='ms')], 137.0)
    assert np.isclose(bid_tv[pd.Timestamp(1000, unit='ms')], 97.0)
    assert np.isclose(bid_tv[pd.Timestamp(2000, unit='ms')], 107.3)
    assert np.isclose(bid_tv[pd.Timestamp(3000, unit='ms')], 107.3)
    assert np.isclose(bid_tv[pd.Timestamp(4000, unit='ms')], 78.05)
