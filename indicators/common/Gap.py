import glob
import os
import math
from pathlib import Path
from typing import Dict, List, Literal, Tuple, Union, Callable, Optional
import pandas as pd
from numpy import ndarray 
import numpy as np
from copy import deepcopy

from pybacktest.books import Book
from pybacktest.bookcore import BookCore
from pybacktest.simTime import SimTime


class Gap:
    def __init__(self, ts1: pd.Series, ts2: pd.Series, fill_value: float = 0) -> None:
        self._ts = ts1.sub(ts2, fill_value=fill_value)
    
    def __getitem__(self, key):
        return self._ts[key]





