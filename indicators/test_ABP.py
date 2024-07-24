import pandas as pd
from pathlib import Path
import os
import numpy as np

from .ABP import ABP

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    ask_abp = ABP(
        N = 5, instId=instId,
        side='ask',
        start=start, end=end, path=path
    )
    bid_abp = ABP(
        N = 5, instId=instId,
        side='bid',
        start=start, end=end, path=path
    )
    
    assert np.isclose(ask_abp[pd.Timestamp(   0, unit='ms')], 101.33333333333333)
    assert np.isclose(ask_abp[pd.Timestamp(1000, unit='ms')], 102.03333333333332)
    assert np.isclose(ask_abp[pd.Timestamp(2000, unit='ms')], 102.03333333333332)
    assert np.isclose(bid_abp[pd.Timestamp(   0, unit='ms')], 97.66666666666667)
    assert np.isclose(bid_abp[pd.Timestamp(1000, unit='ms')], 97.66666666666667)
    assert np.isclose(bid_abp[pd.Timestamp(2000, unit='ms')], 97.5287356321839)


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 2000, 1000
    
    ask_abp = ABP(
        N = 4, instId=instId,
        side='ask',
        start=start, end=end, path=path
    )
    bid_abp = ABP(
        N = 4, instId=instId,
        side='bid',
        start=start, end=end, path=path
    )
    
    assert np.isclose(ask_abp[pd.Timestamp(   0, unit='ms')], 101.14285714285714)
    assert np.isclose(ask_abp[pd.Timestamp(1000, unit='ms')], 101.63999999999999)
    assert np.isclose(ask_abp[pd.Timestamp(2000, unit='ms')], 101.63999999999999)
    assert np.isclose(bid_abp[pd.Timestamp(   0, unit='ms')], 97.85714285714286)
    assert np.isclose(bid_abp[pd.Timestamp(1000, unit='ms')], 97.00000000000001)
    assert np.isclose(bid_abp[pd.Timestamp(2000, unit='ms')], 97.54545454545453)

def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, _ = 0, 4000, 1000
    
    ask_abp = ABP(
        N = 4, instId=instId,
        side='ask',
        start=start, end=end, path=path
    )
    bid_abp = ABP(
        N = 4, instId=instId,
        side='bid',
        start=start, end=end, path=path
    )
    
    assert np.isclose(ask_abp[pd.Timestamp(   0, unit='ms')], 101.14285714285714)
    assert np.isclose(ask_abp[pd.Timestamp(1000, unit='ms')], 101.63999999999999)
    assert np.isclose(ask_abp[pd.Timestamp(2000, unit='ms')], 101.63999999999999)
    assert np.isclose(ask_abp[pd.Timestamp(3000, unit='ms')], 101.63999999999999)
    assert np.isclose(ask_abp[pd.Timestamp(4000, unit='ms')], 102.13333333333333)
    assert np.isclose(bid_abp[pd.Timestamp(   0, unit='ms')], 97.85714285714286)
    assert np.isclose(bid_abp[pd.Timestamp(1000, unit='ms')], 97.00000000000001)
    assert np.isclose(bid_abp[pd.Timestamp(2000, unit='ms')], 97.54545454545453)
    assert np.isclose(bid_abp[pd.Timestamp(3000, unit='ms')], 97.54545454545453)
    assert np.isclose(bid_abp[pd.Timestamp(4000, unit='ms')], 97.56249999999999)
