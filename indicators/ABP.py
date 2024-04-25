'''
Amount-Based Price
'''
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


class ABP:
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
    
    def _calc(self, bookcore: BookCore) -> Tuple[float, float]:
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
        # TODO: fill the missing data points
        if self._data_ask:
            return
        simTime = SimTime(self.start, self.end)
        book = Book(
            self.instId, simTime, self.path,
            self.max_interval, self.check_instId
        )
        data: List[Tuple[float, float]] = []
        while True:
            cur = book.core
            data.append(self._calc(cur))
            if simTime + self.step <= self.end:
                simTime.add(self.step)
            else:
                break
        self._ts = np.array(range(self.start, self.end + 1, self.step), dtype=int)
        self._data_ask = np.array(list(map(lambda x: x[0], data)), dtype=float)
        self._data_bid = np.array(list(map(lambda x: x[1], data)), dtype=float)


    def __getitem__(self, key):
        if isinstance(key, int):
            # Check if the key is within the range and aligned with the step
            if not (self.start <= key <= self.end and (key - self.start) % self.step == 0):
                raise IndexError("Key must be within the range of start and end, and aligned with the step.")
            # Calculate the index in the data array
            key_index = (key - self.start) // self.step
            return (self._data_ask[key_index], self._data_bid[key_index])
        elif isinstance(key, slice):
            # Calculate the start, stop, and step for the slice
            start, stop, step = key.indices(self.end)
            # Ensure that the slice is aligned with the BLCSI step
            if start % self.step != 0 or (stop is not None and stop % self.step != 0 and stop != self.end):
                raise ValueError("Slice start and stop must be aligned with the BLCSI step.")
            # Calculate the corresponding slice on the _data array
            data_slice = slice(
                (start - self.start) // self.step,
                None if stop is None else (stop - self.start) // self.step,
                None if step is None else step // self.step
            )
            return list(zip(self._data_ask[data_slice], self._data_bid[data_slice]))
        elif isinstance(key, pd.Timestamp):
            raise NotImplementedError()
        else:
            raise TypeError("Invalid key type. Key must be an integer or a slice.")
    
    
    def __iter__(self):
        return iter(zip(deepcopy(self._ts), deepcopy(self._data_ask), deepcopy(self._data_bid)))
    
    
    def __len__(self):
        return len(self._data_ask)


