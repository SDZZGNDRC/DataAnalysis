import pandas as pd
import pytest
from pathlib import Path
import os

from .TA import TA

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    ta = TA(
        N = 5, instId=instId,
        start=start, end=end, path=path
    )
    
    assert ta[pd.Timestamp(   0, unit='ms')] == (1.5, 1.5)
    assert ta[pd.Timestamp(1000, unit='ms')] == (3.0, 1.5)
    assert ta[pd.Timestamp(2000, unit='ms')] == (3.0, 8.700000000000001)


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    ta = TA(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert ta[pd.Timestamp(   0, unit='ms')] == (1.4, 1.4)
    assert ta[pd.Timestamp(1000, unit='ms')] == (2.5, 0.9999999999999999)
    assert ta[pd.Timestamp(2000, unit='ms')] == (2.5, 1.1)

def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 4000, 1000
    
    ta = TA(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert ta[pd.Timestamp(   0, unit='ms')] == (1.4, 1.4)
    assert ta[pd.Timestamp(1000, unit='ms')] == (2.5, 0.9999999999999999)
    assert ta[pd.Timestamp(2000, unit='ms')] == (2.5, 1.1)
    assert ta[pd.Timestamp(3000, unit='ms')] == (2.5, 1.1)
    assert ta[pd.Timestamp(4000, unit='ms')] == (1.5, 0.8)


