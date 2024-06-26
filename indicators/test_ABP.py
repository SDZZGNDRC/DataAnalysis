import pandas as pd
import pytest
from pathlib import Path
import os

from .ABP import ABP

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    abp = ABP(
        N = 5, instId=instId,
        start=start, end=end, path=path
    )
    
    assert abp[pd.Timestamp(   0, unit='ms')] == (101.33333333333333, 97.66666666666667)
    assert abp[pd.Timestamp(1000, unit='ms')] == (102.03333333333332, 97.66666666666667)
    assert abp[pd.Timestamp(2000, unit='ms')] == (102.03333333333332, 97.5287356321839)


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    abp = ABP(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert abp[pd.Timestamp(   0, unit='ms')] == (101.14285714285714, 97.85714285714286)
    assert abp[pd.Timestamp(1000, unit='ms')] == (101.63999999999999, 97.00000000000001)
    assert abp[pd.Timestamp(2000, unit='ms')] == (101.63999999999999, 97.54545454545453)

def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 4000, 1000
    
    abp = ABP(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert abp[pd.Timestamp(   0, unit='ms')] == (101.14285714285714, 97.85714285714286)
    assert abp[pd.Timestamp(1000, unit='ms')] == (101.63999999999999, 97.00000000000001)
    assert abp[pd.Timestamp(2000, unit='ms')] == (101.63999999999999, 97.54545454545453)
    assert abp[pd.Timestamp(3000, unit='ms')] == (101.63999999999999, 97.54545454545453)
    assert abp[pd.Timestamp(4000, unit='ms')] == (102.13333333333333, 97.56249999999999)


