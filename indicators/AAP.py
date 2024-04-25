'''
Amount-Based Price
'''
import glob
import os
import math
from pathlib import Path
from typing import Dict, List, Literal, Tuple, Union, Optional
import pandas as pd
import numpy as np
from copy import deepcopy

from pybacktest.src.books import Book
from pybacktest.src.bookcore import BookCore
from pybacktest.src.simTime import SimTime


class AAP:
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
        self._data_ask: Optional[pd.Series] = None
        self._data_bid: Optional[pd.Series] = None
        
        self._gen()
    
    def _calc(self, bookcore: BookCore) -> Tuple[float, float]:
        L = bookcore.depth_asks
        if L < self.N:
            raise ValueError(f"N must be smaller than max-depth, but {L} < {self.N}")
        asks = bookcore.asks[:self.N]
        ask_aap = sum(map(lambda x: x.price, asks)) / self.N
        L = bookcore.depth_bids
        if L < self.N:
            raise ValueError(f"N must be smaller than max-depth, but {L} < {self.N}")
        bids = bookcore.bids[:self.N]
        bid_aap = sum(map(lambda x: x.price, bids)) / self.N
        
        return tuple([ask_aap, bid_aap])
    
    
    def _gen(self) -> None:
        if self._data_ask:
            return
        simTime = SimTime(self.start, self.end)
        book = Book(
            self.instId, simTime, self.path,
            self.max_interval, self.check_instId
        )
        data_ask: Dict[pd.Timestamp, float] = {}
        data_bid: Dict[pd.Timestamp, float] = {}
        idx: List[pd.Timestamp] = []
        while True:
            cur = book.core
            ask, bid = self._calc(cur)
            data_ask[simTime.to_Timestamp()] = ask
            data_bid[simTime.to_Timestamp()] = bid
            idx.append(simTime.to_Timestamp())
            if simTime + self.step <= self.end:
                simTime.add(self.step)
            else:
                break
        self._data_ask = pd.Series(data_ask, index=idx)
        self._data_bid = pd.Series(data_bid, index=idx)


    def __getitem__(self, key):
        if isinstance(key, int):
            return (self._data_ask[pd.Timestamp(key, unit='ms')], self._data_bid[pd.Timestamp(key, unit='ms')])
        elif isinstance(key, slice):
            return list(zip(self._data_ask[key], self._data_bid[key]))
        elif isinstance(key, pd.Timestamp):
            return (self._data_ask[key], self._data_bid[key])
        else:
            raise TypeError("Invalid key type. Key must be an integer or a slice.")
    
    
    def __iter__(self):
        return iter(deepcopy(self._data_ask), deepcopy(self._data_bid))
    
    
    def __len__(self):
        return len(self._data_ask)


    def ask(self, deepcopy: bool = False) -> pd.Series:
        """
        Returns the series of asks.
        """
        if deepcopy:
            return self._data_ask.copy()
        else:
            return self._data_ask

    def bid(self, deepcopy: bool = False) -> pd.Series:
        """
        Returns the series of bids.
        """
        if deepcopy:
            return self._data_bid.copy()
        else:
            return self._data_bid

    @property
    def gap(self) -> pd.Series:
        """
        Returns the gap between asks and bids.
        """
        return self._data_ask - self._data_bid


