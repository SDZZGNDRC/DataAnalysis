# This script tempt to calculate the variance of a product.
import sys
from typing import List
sys.path.append('D:\\Project')
from pybacktest.src.simTime import SimTime
from pybacktest.src.books import Book

class Price:
    '''Calculate the change of price in the past period'''
    def __init__(self, instId: str, start: int, end: int, path: str) -> None:
        self.instId = instId
        self.start = start
        self.end = end
        self.path = path

    
    def run(self, eval_step: int = 1000) -> None:
        simTime = SimTime(self.start, self.end)
        book = Book(self.instId, self.simTime, self.path, False)
        prices: List[float] = []
        while True:
            prices.append((book.asks[0]+book.bids[0])/2)
            if simTime+eval_step <= self.end:
                simTime.add(eval_step)
            else:
                break
        return prices

