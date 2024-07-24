import pytest
from pathlib import Path
import os
import numpy as np

from .AAP import AAP

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 2000, 1000
    
    aap_ask = AAP(
        N=5, instId=instId,
        start=start, end=end, path=path,
        side='ask'
    )
    
    aap_bid = AAP(
        N=5, instId=instId,
        start=start, end=end, path=path,
        side='bid'
    )
    
    assert np.isclose(aap_ask[0], 102)
    assert np.isclose(aap_ask[1000], 102)
    assert np.isclose(aap_ask[2000], 102)

    assert np.isclose(aap_bid[0], 97)
    assert np.isclose(aap_bid[1000], 97)
    assert np.isclose(aap_bid[2000], 97)


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 2000, 1000
    
    aap_ask = AAP(
        N=4, instId=instId,
        start=start, end=end, path=path,
        side='ask'
    )
    
    aap_bid = AAP(
        N=4, instId=instId,
        start=start, end=end, path=path,
        side='bid'
    )
    
    assert np.isclose(aap_ask[0], 101.5)
    assert np.isclose(aap_ask[1000], 101.5)
    assert np.isclose(aap_ask[2000], 101.5)

    assert np.isclose(aap_bid[0], 97.5)
    assert np.isclose(aap_bid[1000], 96.5)
    assert np.isclose(aap_bid[2000], 97.5)


def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 4000, 1000
    
    aap_ask = AAP(
        N=4, instId=instId,
        start=start, end=end, path=path,
        side='ask'
    )
    
    aap_bid = AAP(
        N=4, instId=instId,
        start=start, end=end, path=path,
        side='bid'
    )
    
    assert np.isclose(aap_ask[0], 101.5)
    assert np.isclose(aap_ask[1000], 101.5)
    assert np.isclose(aap_ask[2000], 101.5)
    assert np.isclose(aap_ask[3000], 101.5)
    assert np.isclose(aap_ask[4000], 101.75)

    assert np.isclose(aap_bid[0], 97.5)
    assert np.isclose(aap_bid[1000], 96.5)
    assert np.isclose(aap_bid[2000], 97.5)
    assert np.isclose(aap_bid[3000], 97.5)
    assert np.isclose(aap_bid[4000], 97.875)
