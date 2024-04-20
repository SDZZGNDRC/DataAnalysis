import glob
import os
import math
from pathlib import Path
from typing import Dict, List, Tuple, Union, Callable, Optional
import pandas as pd
from numpy import ndarray 
import numpy as np
from copy import deepcopy

# from pybacktest.src.bookcore import *
from pybacktest.src.books import Book
from pybacktest.src.bookcore import BookCore
from pybacktest.src.simTime import SimTime

class BLCSI:
    def __init__(
                self, instId: str,
                start: int, end: int,
                path: Path, max_interval: int = 10_000, 
                step: int = 1000, check_instId: bool = True) -> None:
        self.instId = instId
        if start % step != 0:
            raise ValueError(f"start must be a multiple of {step}")
        self.start = start
        
        # The calculating of BLCSI[i] refers to the book[i-1] and book[i], 
        # so BLCSI[0] refers to the book[-1], which is at `start - step`.
        self.true_start = start - step
        if end % step != 0:
            raise ValueError(f"end must be a multiple of {step}")
        self.end = end
        self.path = path
        self.max_interval = max_interval
        self.step = step
        self.check_instId = check_instId
        self._data: Optional[ndarray] = None
        self._ts: Optional[ndarray] = None
        
        self._gen()
    
    def _diff(self, bookcore_1: BookCore, bookcore_2: BookCore) -> int:
        '''
        Calculate the number of different booklevel.
        '''
        res = 0
        # Asks
        L = min(bookcore_1.depth_asks, bookcore_2.depth_asks)
        asks_1, asks_2 = bookcore_1.asks[:L], bookcore_2.asks[:L]
        for i in range(L):
            if not asks_1[i].true_eq(asks_2[i]):
                res += 1
        res += max(bookcore_1.depth_asks, bookcore_2.depth_asks) - L
        # print(res)
        # Bids
        L = min(bookcore_1.depth_bids, bookcore_2.depth_bids)
        bids_1, bids_2 = bookcore_1.bids[:L], bookcore_2.bids[:L]
        for i in range(L):
            if not bids_1[i].true_eq(bids_2[i]):
                res += 1
        res += max(bookcore_1.depth_bids, bookcore_2.depth_bids) - L
        # print(bookcore_1)
        # print(bookcore_2)
        # print(res, L)
        # exit()
        return res
    
    def _gen(self) -> None:
        if self._data:
            return
        simTime = SimTime(self.true_start, self.end)
        book = Book(
            self.instId, simTime, self.path,
            self.max_interval, self.check_instId
        )
        last = book.core
        data = []
        simTime.add(self.step)
        while True:
            cur = book.core
            data.append(self._diff(cur, last))
            last = cur
            if simTime + self.step <= self.end:
                simTime.add(self.step)
            else:
                break
        self._ts = np.array(range(self.start, self.end + 1, self.step), dtype=int)
        self._data = np.array(data, dtype=int)

    def __getitem__(self, key):
        if isinstance(key, int):
            # Check if the key is within the range and aligned with the step
            if not (self.start <= key <= self.end and (key - self.start) % self.step == 0):
                raise IndexError("Key must be within the range of start and end, and aligned with the step.")
            # Calculate the index in the data array
            key_index = (key - self.start) // self.step
            return self._data[key_index]
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
            return self._data[data_slice]
        else:
            raise TypeError("Invalid key type. Key must be an integer or a slice.")
    
    
    def __iter__(self):
        return iter(zip(deepcopy(self._ts), deepcopy(self._data)))
    
    
    def __len__(self):
        return len(self._data)






