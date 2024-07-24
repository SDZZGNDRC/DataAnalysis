import pytest
from pathlib import Path
import os

from .BLCSI import BLCSI


def test_case_0():
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / 'SINE-USDT'
    start, end, step = 1000, 149000, 1000
    ask_blcsi = BLCSI(
        instId='SINE-USDT',
        start=start, end=end, 
        path=path, side='ask', step=step
    )
    bid_blcsi = BLCSI(
        instId='SINE-USDT',
        start=start, end=end, 
        path=path, side='bid', step=step
    )
    assert all([ask_blcsi[i*step+start] == 5 for i in range(len(ask_blcsi))])
    assert all([bid_blcsi[i*step+start] == 5 for i in range(len(bid_blcsi))])

def test_case_1():
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / 'CASE1-USDT'
    start, end, step = 1000, 2000, 1000
    ask_blcsi = BLCSI(
        instId='CASE1-USDT',
        start=start, end=end, 
        path=path, side='ask', step=step
    )
    bid_blcsi = BLCSI(
        instId='CASE1-USDT',
        start=start, end=end, 
        path=path, side='bid', step=step
    )
    assert ask_blcsi[1000] == 5
    assert bid_blcsi[1000] == 0
    assert ask_blcsi[2000] == 0
    assert bid_blcsi[2000] == 5


def test_case_2():
    instId = 'CASE2-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 1000, 2000, 1000
    ask_blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, side='ask', step=step
    )
    bid_blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, side='bid', step=step
    )
    assert ask_blcsi[1000] == 6
    assert bid_blcsi[1000] == 5
    assert ask_blcsi[2000] == 1
    assert bid_blcsi[2000] == 5


def test_case_3():
    instId = 'CASE3-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 1000, 4000, 1000
    ask_blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, side='ask', step=step
    )
    bid_blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, side='bid', step=step
    )
    assert ask_blcsi[1000] == 6
    assert bid_blcsi[1000] == 5
    assert ask_blcsi[2000] == 1
    assert bid_blcsi[2000] == 5
    assert ask_blcsi[3000] == 0
    assert bid_blcsi[3000] == 0
    assert ask_blcsi[4000] == 4
    assert bid_blcsi[4000] == 3


def test_case_4():
    instId = 'TRIANGLE-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 1000, 149000, 1000
    blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, step=step
    )
    assert all([blcsi[i*step+start] == 10 for i in range(len(blcsi))])


def test_case_5():
    instId = 'SQUARE-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 1000, 999000, 1000
    blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, step=step
    )
    assert all([blcsi[i*step+start] == 0 for i in range(4)])
    assert blcsi[167000] == 10
    assert blcsi[168000] == 0
    assert blcsi[333000] == 10
    assert blcsi[334000] == 0
    assert blcsi[500000] == 10
    assert blcsi[501000] == 0


def test_case_6():
    instId = 'GAUSSIAN-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 1000, 999000, 1000
    blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, step=step
    )
    assert all([blcsi[i*step+start] == 10 for i in range((231000-start)//step)])
    assert blcsi[231000] == 0
    assert all([blcsi[i*step+232000] == 10 for i in range((243000-232000)//step)])
    assert blcsi[243000] == 0
    assert all([blcsi[i*step+244000] == 10 for i in range((252000-244000)//step)])
    assert blcsi[252000] == 0


def test_case_7():
    instId = 'PIECEWISE-LINEAR-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 1000, 999000, 1000
    blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, step=step
    )
    assert all([blcsi[i*step+start] == 10 for i in range((end-start)//step)])


def test_case_8():
    instId = 'CONST-USDT'
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / instId
    start, end, step = 1000, 999000, 1000
    blcsi = BLCSI(
        instId=instId,
        start=start, end=end, 
        path=path, step=step
    )
    assert all([blcsi[i*step+start] == 0 for i in range((end-start)//step)])
