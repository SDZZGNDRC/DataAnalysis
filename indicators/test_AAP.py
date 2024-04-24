import pytest
from pathlib import Path
import os

from .AAP import AAP

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 2000, 1000
    
    aap = AAP(
        N = 5, instId=instId,
        start=start, end=end, path=path
    )
    
    assert aap[   0] == (102, 97)
    assert aap[1000] == (102, 97)
    assert aap[2000] == (102, 97)


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 2000, 1000
    
    aap = AAP(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert aap[   0] == (101.5, 97.5)
    assert aap[1000] == (101.5, 96.5)
    assert aap[2000] == (101.5, 97.5)

def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 4000, 1000
    
    aap = AAP(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert aap[   0] == (101.5, 97.5)
    assert aap[1000] == (101.5, 96.5)
    assert aap[2000] == (101.5, 97.5)
    assert aap[3000] == (101.5, 97.5)
    assert aap[4000] == (101.75, 97.875)


