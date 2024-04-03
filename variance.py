# This script tempt to calculate the variance of a product.
import hashlib
import json
import os
import sys
from typing import List, Tuple

from matplotlib import pyplot as plt
sys.path.append('D:\\Project')
from pybacktest.src.simTime import SimTime
from pybacktest.src.books import Book

import numpy as np

def find_price_change_intervals(data, minutes=5, change=0.05):
    # 将时间戳从毫秒转换为分钟
    data_in_minutes = [[int(x[0] / (1000 * 60)), x[1]] for x in data]
    
    # 按分钟对价格进行分组
    prices_by_minute = {}
    for timestamp, price in data_in_minutes:
        if timestamp not in prices_by_minute:
            prices_by_minute[timestamp] = []
        prices_by_minute[timestamp].append(price)
    
    # 对每分钟的价格求平均值
    average_prices_by_minute = {k: np.mean(v) for k, v in prices_by_minute.items()}
    
    # 计算每分钟的价格变化
    price_changes = {}
    timestamps = sorted(average_prices_by_minute.keys())
    for i in range(len(timestamps) - minutes):
        start_timestamp = timestamps[i]
        end_timestamp = timestamps[i + minutes]
        # Calculate the price change using min and max price within the interval
        prices_within_interval = [average_prices_by_minute[t] for t in range(start_timestamp, end_timestamp + 1)]
        min_price = min(prices_within_interval)
        max_price = max(prices_within_interval)
        price_change = (max_price - min_price) / min_price
        price_changes[(start_timestamp, end_timestamp)] = price_change
    
    # 找出价格变化超过5%的时间区间
    significant_changes = {k: v for k, v in price_changes.items() if abs(v) > change}
    
    return significant_changes


class Price:
    '''Calculate the change of price in the past period'''
    def __init__(self, instId: str, start: int, end: int, path: str) -> None:
        self.instId = instId
        self.start = start
        self.end = end
        self.path = path

    
    def run(self, eval_step: int = 1000) -> List[Tuple[int, float]]:
        params = self.instId+str(self.start)+str(self.end)+self.path+str(eval_step)
        cache_name = hashlib.md5(params.encode()).hexdigest()
        cache_path = os.path.join('.\\tmp', cache_name+'.json')
        if os.path.exists(cache_path):
            print('Use cached file')
            return json.load(open(cache_path, 'r'))['px']
        else:
            print(f'Can not find {cache_path}, start to calculate')
        
        simTime = SimTime(self.start, self.end)
        book = Book(self.instId, simTime, self.path, check_instId=False)
        prices: List[Tuple[int, float]] = []
        while True:
            prices.append((int(simTime), (book.asks[0].price+book.bids[0].price)/2))
            if (int(simTime) - self.start) % 3600*1000 == 0:
                print(f'Up to {int(simTime)}')
            if simTime+eval_step <= self.end:
                simTime.add(eval_step)
            else:
                break
        
        with open(cache_path, 'w') as fout:
            fout.write(json.dumps({'px': prices}))
        
        return prices


if __name__ == '__main__':
    instId = 'BTC-USDT-TWEEK'
    start = 1691983094209
    end = 1692064648400
    path = 'E:\\out3\\books\\BTC-USDT-TWEEK'
    p = Price(instId, start, end, path)
    px = p.run()
    significant_changes = find_price_change_intervals(px)
    print(significant_changes)
    px_np = np.array(px)
    plt.plot(px_np[:,0], px_np[:,1])
    plt.show()
