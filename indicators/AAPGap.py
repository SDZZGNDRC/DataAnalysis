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

from .AAP import AAP

class AAPGap:
    def __init__(
                self, N: int, instId: str,
                start: int, end: int,
                path: Path, max_interval: int = 10_000, 
                step: int = 1000, check_instId: bool = True) -> None:
        self._aapgap = AAP(
            N=N, instId=instId, start=start, end=end, path=path,
            max_interval=max_interval, step=step,
            check_instId=check_instId
        )

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._aapgap[key][0] - self._aapgap[key][1]
        elif isinstance(key, slice):
            return list(map(lambda x: x[0] - x[1], self._aapgap[key]))
        else:
            raise TypeError("Invalid key type. Key must be an integer or a slice.")
    
    def __iter__(self):
        # TODO: implement this
        raise NotImplementedError()
    
    def __len__(self):
        # TODO: implement this
        raise NotImplementedError()


