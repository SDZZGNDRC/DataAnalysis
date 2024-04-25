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


class Gap:
    def __init__(self, ts1: pd.Series, ts2: pd.Series) -> None:
        self._ts = ts1 - ts2
    
    def __getitem__(self, key):
        return self._ts[key]





