import glob
import os
import math
from pathlib import Path
from typing import Dict, List, Literal, Tuple, Union, Callable, Optional
import pandas as pd
from numpy import ndarray 
import numpy as np
from copy import deepcopy

from pybacktest.src.books import Book
from pybacktest.src.bookcore import BookCore
from pybacktest.src.simTime import SimTime



class TA:
    def __init__(
                self, N: int, instId: str,
                start: int, end: int,
                path: Path, max_interval: int = 10_000, 
                step: int = 1000, check_instId: bool = True) -> None:
        if N <= 0:
            raise ValueError(f"N must be positive, but {N} was given.")
        self.N = N
        self.instId = instId
        if start % step != 0:
            raise ValueError(f"start must be a multiple of {step}")
        self.start = start
        
        if end % step != 0:
            raise ValueError(f"end must be a multiple of {step}")
        self.end = end
        self.path = path
        self.max_interval = max_interval
        self.step = step
        self.check_instId = check_instId
        self._data_ask: Optional[ndarray] = None
        self._data_bid: Optional[ndarray] = None
        self._ts: Optional[ndarray] = None
        
        self._gen()


    def _calc(self, bookcore: BookCore) -> float:
        L = bookcore.depth_asks
        if L < self.N:
            raise ValueError(f"N must be smaller than max-depth, but {L} < {self.N}")
        asks = bookcore.asks[:self.N]
        ask_abp = sum(map(lambda x: x.price*x.amount, asks)) / sum(map(lambda x: x.amount, asks))
        L = bookcore.depth_bids
        if L < self.N:
            raise ValueError(f"N must be smaller than max-depth, but {L} < {self.N}")
        bids = bookcore.bids[:self.N]
        bid_abp = sum(map(lambda x: x.price*x.amount, bids)) / sum(map(lambda x: x.amount, bids))
        
        return tuple([ask_abp, bid_abp])
    

    def _gen(self) -> None:
        raise NotImplementedError()





















