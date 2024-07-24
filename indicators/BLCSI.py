from pathlib import Path
from typing import Dict, List, Tuple, Literal, Optional
import pandas as pd

from pybacktest.src.books import Book
from pybacktest.src.bookcore import BookCore
from pybacktest.src.simTime import SimTime

class BLCSI:
    def __init__(
                self, instId: str,
                start: int, end: int,
                path: Path, max_interval: int = 10_000, 
                side: Literal['ask', 'bid', 'total'] = 'total',
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
        if side not in ['ask', 'bid', 'total']:
            raise ValueError(f"side must be 'ask', 'bid' or 'total', not {side}")
        self.end = end
        self.path = path
        self.max_interval = max_interval
        self.side = side
        self.step = step
        self.check_instId = check_instId
        self._data: Optional[pd.Series] = None
        
        self._gen()
    
    def _diff(self, bookcore_1: BookCore, bookcore_2: BookCore) -> Tuple[int, int]:
        '''
        Calculate the number of different booklevel.
        '''
        res = 0
        # Asks
        if self.side == 'ask':
            L = min(bookcore_1.depth_asks, bookcore_2.depth_asks)
            asks_1, asks_2 = bookcore_1.asks[:L], bookcore_2.asks[:L]
            for i in range(L):
                if not asks_1[i].true_eq(asks_2[i]):
                    res += 1
            res += max(bookcore_1.depth_asks, bookcore_2.depth_asks) - L
        elif self.side == 'bid': # Bids
            L = min(bookcore_1.depth_bids, bookcore_2.depth_bids)
            bids_1, bids_2 = bookcore_1.bids[:L], bookcore_2.bids[:L]
            for i in range(L):
                if not bids_1[i].true_eq(bids_2[i]):
                    res += 1
            res += max(bookcore_1.depth_bids, bookcore_2.depth_bids) - L
        elif self.side == 'total': # Ask + Bid
            L = min(bookcore_1.depth_asks, bookcore_2.depth_asks)
            asks_1, asks_2 = bookcore_1.asks[:L], bookcore_2.asks[:L]
            for i in range(L):
                if not asks_1[i].true_eq(asks_2[i]):
                    res += 1
            res += max(bookcore_1.depth_asks, bookcore_2.depth_asks) - L
            L = min(bookcore_1.depth_bids, bookcore_2.depth_bids)
            bids_1, bids_2 = bookcore_1.bids[:L], bookcore_2.bids[:L]
            for i in range(L):
                if not bids_1[i].true_eq(bids_2[i]):
                    res += 1
            res += max(bookcore_1.depth_bids, bookcore_2.depth_bids) - L
        else:
            raise ValueError(f'Unknown side: {self.side}')
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
        data: Dict[pd.Timestamp, int] = {}
        idx: List[pd.Timestamp] = []
        simTime.add(self.step)
        while True:
            cur = book.core
            data[simTime.to_Timestamp()] = self._diff(cur, last)
            last = cur
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
        return iter(self._data.copy())
    
    
    def __len__(self):
        return len(self._data)

    @property
    def data(self) -> pd.Series:
        return self._data.copy()




