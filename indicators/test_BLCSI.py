import pytest
from pathlib import Path
import os

from .BLCSI import BLCSI


def test_case_0():
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / 'SINE-USDT'
    start, end, step = 1000, 149000, 1000
    blcsi = BLCSI(
        instId='SINE-USDT',
        start=start, end=end, 
        path=path, step=step
    )
    assert all([blcsi[i*step+start] == 10 for i in range(len(blcsi))])

def test_case_1():
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / 'CASE1-USDT'
    start, end, step = 1000, 2000, 1000
    blcsi = BLCSI(
        instId='CASE1-USDT',
        start=start, end=end, 
        path=path, step=step
    )
    assert all([blcsi[i*step+start] == 5 for i in range(len(blcsi))])


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 1000, 2000, 1000
    blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, step=step
    )
    assert blcsi[1000] == 11
    assert blcsi[2000] == 6


def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 1000, 4000, 1000
    blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, step=step
    )
    assert blcsi[1000] == 11
    assert blcsi[2000] == 6
    assert blcsi[3000] == 0
    assert blcsi[4000] == 7


