import pytest
from pathlib import Path
import os

from .ABPGap import ABPGap

def test_case_1():
    instId = 'CASE1-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 2000, 1000
    
    abpgap = ABPGap(
        N = 5, instId=instId,
        start=start, end=end, path=path
    )
    
    assert abpgap[   0] == 101.33333333333333 - 97.66666666666667
    assert abpgap[1000] == 102.03333333333332 - 97.66666666666667
    assert abpgap[2000] == 102.03333333333332 - 97.5287356321839


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 2000, 1000
    
    abpgap = ABPGap(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert abpgap[   0] == 101.14285714285714 - 97.85714285714286
    assert abpgap[1000] == 101.63999999999999 - 97.00000000000001
    assert abpgap[2000] == 101.63999999999999 - 97.54545454545453

def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 0, 4000, 1000
    
    abpgap = ABPGap(
        N = 4, instId=instId,
        start=start, end=end, path=path
    )
    
    assert abpgap[   0] == 101.14285714285714 - 97.85714285714286
    assert abpgap[1000] == 101.63999999999999 - 97.00000000000001
    assert abpgap[2000] == 101.63999999999999 - 97.54545454545453
    assert abpgap[3000] == 101.63999999999999 - 97.54545454545453
    assert abpgap[4000] == 102.13333333333333 - 97.56249999999999


