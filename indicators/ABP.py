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
        """
        初始化ABP类实例。

        参数:
            N (int): 用于计算的订单簿深度。
            instId (str): 合约的标识符。
            start (int): 起始时间戳（毫秒）。
            end (int): 结束时间戳（毫秒）。
            path (Path): 数据存储路径。
            max_interval (int, 可选): 最大时间间隔。默认值为10,000毫秒。
            side (Literal['ask', 'bid'], 可选): 计算价格的方向，'ask'表示卖单，'bid'表示买单。默认值为'ask'。
            step (int, 可选): 时间步长, 默认值为1000毫秒。
            check_instId (bool, 可选): 是否检查合约标识符。默认值为True。
        """
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
        """
        计算当前时刻的基于数量的价格。

        参数:
            bookcore (BookCore): 当前订单簿的核心数据。

        返回:
            float: 计算得到的基于数量的价格。
        """
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
            res = sum(map(lambda x: x.price * x.amount, bids)) / sum(map(lambda x: x.amount, bids))
        else:
            raise ValueError(f"side must be 'ask' or 'bid', but {self.side}")
        return res
    
    
    def _gen(self) -> None:
        """
        生成基于数量的价格数据序列。
        """
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
        """
        通过键（时间戳或切片）获取相应的基于数量的价格。

        参数:
            key (int, slice, pd.Timestamp): 查询的键。

        返回:
            float: 对应时间戳的价格值。

        异常:
            TypeError: 如果键的类型无效。
        """
        if isinstance(key, int):
            return self._data[pd.Timestamp(key, unit='ms')]
        elif isinstance(key, slice):
            return self._data[key]
        elif isinstance(key, pd.Timestamp):
            return self._data[key]
        else:
            raise TypeError("Invalid key type. Key must be an integer or a slice.")
    
    
    def __iter__(self):
        """
        返回数据序列的迭代器。

        返回:
            iterator: 基于数量的价格数据的迭代器。
        """
        return iter(deepcopy(self._data))
    
    
    def __len__(self):
        """
        返回数据序列的长度。

        返回:
            int: 数据点的数量。
        """
        return len(self._data)
    

    @property
    def data(self) -> pd.Series:
        """
        获取数据序列的副本。

        返回:
            pd.Series: 基于数量的价格数据。
        """
        return deepcopy(self._data)
