import pytest
from pathlib import Path
import os

from .BLCSI import BLCSI


def test_case_1():
    path = Path(os.path.abspath(__file__)).parent.parent / 'test' / 'SINE-USDT'
    start, end, step = 1000, 149000, 1000
    blcsi = BLCSI(
        instId='SINE-USDT',
        start=start, end=end, 
        path=path, step=step
    )
    assert all([blcsi[i*step+start] == 10 for i in range(len(blcsi))])





