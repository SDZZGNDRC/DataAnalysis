'''
Amount-Based Price
'''
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
                side: Literal['ask', 'bid'] = 'ask',
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
        if side not in ['ask', 'bid']:
            raise ValueError(f"side must be 'ask' or 'bid', but {side} was given.")
        self.end = end
        self.path = path
        self.max_interval = max_interval
        self.side = side
        self.step = step
        self.check_instId = check_instId
        self._data: Optional[pd.Series] = None
        
        self._gen()
    
    def _calc(self, bookcore: BookCore) -> float:
        res = 0.0
        if self.side == 'ask':
            L = bookcore.depth_asks
            if L < self.N:
                raise ValueError(f"N must be smaller than max-depth, but {L} < {self.N}")
            asks = bookcore.asks[:self.N]
            res = sum(map(lambda x: x.price*x.amount, asks)) / sum(map(lambda x: x.amount, asks))
        elif self.side == 'bid':
            L = bookcore.depth_bids
            if L < self.N:
                raise ValueError(f"N must be smaller than max-depth, but {L} < {self.N}")
            bids = bookcore.bids[:self.N]
            res = sum(map(lambda x: x.price*x.amount, bids)) / sum(map(lambda x: x.amount, bids))
        
        return res
    
    
    def _gen(self) -> None:
        if self._data:
            return
        simTime = SimTime(self.start, self.end)
        book = Book(
            self.instId, simTime, self.path,
            self.max_interval, self.check_instId
        )
        data: Dict[pd.Timestamp, float] = {}
        idx = []
        while True:
            cur = book.core
            val = self._calc(cur)
            data[simTime.to_Timestamp()] = val
            idx.append(simTime.to_Timestamp())
            if simTime + self.step <= self.end:
                simTime.add(self.step)
            else:
                break
        self._data = pd.Series(data, index=idx)
        


    def __getitem__(self, key):
        if isinstance(key, int):
            return self._data[pd.Timestamp(key, unit='ms')]
        elif isinstance(key, slice):
            return self._data[key]
        elif isinstance(key, pd.Timestamp):
            return self._data[key]
        else:
            raise TypeError("Invalid key type. Key must be an integer or a slice.")
    
    
    def __iter__(self):
        return iter(deepcopy(self._data))
    
    
    def __len__(self):
        return len(self._data)


    @property
    def data(self) -> pd.Series:
        return deepcopy(self._data)
