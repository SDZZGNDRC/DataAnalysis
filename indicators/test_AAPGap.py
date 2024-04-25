import pytest
from pathlib import Path
import os

from .AAPGap import AAPGap

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 2000, 1000
    
    aapgap = AAPGap(
        N = 5, instId=instId,
        start=start, end=end, path=path
    )
    
    assert aapgap[   0] == 102 - 97
    assert aapgap[1000] == 102 - 97
    assert aapgap[2000] == 102 - 97


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 2000, 1000
    
    aapgap = AAPGap(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert aapgap[   0] == 101.5 - 97.5
    assert aapgap[1000] == 101.5 - 96.5
    assert aapgap[2000] == 101.5 - 97.5

def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 4000, 1000
    
    aapgap = AAPGap(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert aapgap[   0] == 101.5 - 97.5
    assert aapgap[1000] == 101.5 - 96.5
    assert aapgap[2000] == 101.5 - 97.5
    assert aapgap[3000] == 101.5 - 97.5
    assert aapgap[4000] == 101.75 - 97.875


