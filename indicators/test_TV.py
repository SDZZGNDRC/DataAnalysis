import pandas as pd
import pytest
from pathlib import Path
import os

from .TV import TV

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    tv = TV(
        N = 5, instId=instId,
        start=start, end=end, path=path
    )
    tv_gap = tv.gap
    
    assert tv[pd.Timestamp(   0, unit='ms')] == (152, 146.5)
    assert tv[pd.Timestamp(1000, unit='ms')] == (306.09999999999997, 146.5)
    assert tv[pd.Timestamp(2000, unit='ms')] == (306.09999999999997, 848.5)
    
    assert tv_gap[pd.Timestamp(   0, unit='ms')] == 152 - 146.5
    assert tv_gap[pd.Timestamp(1000, unit='ms')] == 306.09999999999997 - 146.5
    assert tv_gap[pd.Timestamp(2000, unit='ms')] == 306.09999999999997 - 848.5


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    tv = TV(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    tv_gap = tv.gap
    
    assert tv[pd.Timestamp(   0, unit='ms')] == (141.6, 137.0)
    assert tv[pd.Timestamp(1000, unit='ms')] == (254.09999999999997, 97.0)
    assert tv[pd.Timestamp(2000, unit='ms')] == (254.09999999999997, 107.3)
    
    assert tv_gap[pd.Timestamp(   0, unit='ms')] == 141.6 - 137.0
    assert tv_gap[pd.Timestamp(1000, unit='ms')] == 254.09999999999997 - 97.0
    assert tv_gap[pd.Timestamp(2000, unit='ms')] == 254.09999999999997 - 107.3

def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 4000, 1000
    
    tv = TV(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    tv_gap = tv.gap
    
    assert tv[pd.Timestamp(   0, unit='ms')] == (141.6, 137.0)
    assert tv[pd.Timestamp(1000, unit='ms')] == (254.09999999999997, 97.0)
    assert tv[pd.Timestamp(2000, unit='ms')] == (254.09999999999997, 107.3)
    assert tv[pd.Timestamp(3000, unit='ms')] == (254.09999999999997, 107.3)
    assert tv[pd.Timestamp(4000, unit='ms')] == (153.2, 78.05)
    
    assert tv_gap[pd.Timestamp(   0, unit='ms')] == 141.6 - 137.0
    assert tv_gap[pd.Timestamp(1000, unit='ms')] == 254.09999999999997 - 97.0
    assert tv_gap[pd.Timestamp(2000, unit='ms')] == 254.09999999999997 - 107.3
    assert tv_gap[pd.Timestamp(3000, unit='ms')] == 254.09999999999997 - 107.3
    assert tv_gap[pd.Timestamp(4000, unit='ms')] == 153.2 - 78.05


