import glob
import bisect
from multiprocessing.pool import Pool
import zlib
from numpy import int32
from pandas import DataFrame, Series
import ctypes
from typing import List, Tuple
import dask.dataframe as dd


dataset = {}
for files_path in glob.glob('e:\\out2\\*'):
    dataset[files_path.split('\\')[-1]] = files_path


class BooksItem:
    def __init__(self,
                price: str,
                size: str,
                numOrders: int,
                side: str,
                ) -> None:
        self.price = price
        self.size = size
        self.numOrders = numOrders
        self.side = side
    
    def __str__(self) -> str:
        # if self.size == '0':
        #     raise Exception('zero size')
        return ':'.join([self.price, self.size])

    def __lt__(self, other):
        return float(self.price) < float(other.price)
    

class Books:
    def __init__(self, instId: str) -> None:
        self.instId = instId
        self.bids: List[BooksItem] = []
        self.asks: List[BooksItem] = []
    
    def update(self, rawRecord: Series) -> None:
        if rawRecord['instId'] != self.instId:
            raise ValueError('instId mismatch')
        
        new_booksItem = BooksItem(
                            rawRecord['price'], 
                            rawRecord['size'], 
                            rawRecord['numOrders'], 
                            rawRecord['side']
                        )
        if rawRecord['side'] not in ['bid', 'ask']:
            raise ValueError('side should be either bid or ask')
        target = self.bids if rawRecord['side'] == 'bid' else self.asks
        if rawRecord['side'] == 'bid':
            # asks are sorted in descending order
            target.reverse()

        dest_level = next(filter(lambda x: x.price == new_booksItem.price, target), None)
        if dest_level is None:
            if new_booksItem.size == '0':
                # print(f'deprecated booksItem-> {new_booksItem.price}:{new_booksItem.size}')
                if rawRecord['side'] == 'bid':
                    target.reverse()
                return
            inserted_point = bisect.bisect_left(target, new_booksItem)
            target.insert(inserted_point, new_booksItem)
        else:
            if new_booksItem.size == '0':
                # print(f'{new_booksItem.price}: {new_booksItem.size}')
                target.remove(dest_level)
            else:
                dest_level.size = new_booksItem.size
                dest_level.numOrders = new_booksItem.numOrders

        if rawRecord['side'] == 'bid':
            # reverse back
            target.reverse()

    def update_many(self, ddf: DataFrame):
        if ddf.iloc[0]['action'] == 'snapshot':
            # print('Encounted snapshot action......')
            self.clear()

        for _, row in ddf.iterrows():
            self.update(row)

    def clear(self):
        self.bids = []
        self.asks = []
    
    def checksum_text(self) -> str:
        bids = self.bids[:25]
        asks = self.asks[:25]

        checksum_string = ''
        for i in range(max(len(bids), len(asks))):
            if i < len(bids):
                checksum_string = ':'.join([checksum_string, str(bids[i])])
            if i < len(asks):
                checksum_string = ':'.join([checksum_string, str(asks[i])])
        checksum_string = checksum_string.strip(':')
        return checksum_string
    
    @property
    def checksum(self) -> int:
        checksum_string = self.checksum_text()
        # print(f'checksum_string: {checksum_string}')
        # Calculate checksum with CRC32
        raw_checksum = ctypes.c_int32(zlib.crc32(checksum_string.encode('utf-8'))).value
        return raw_checksum


def verify(dataset_path: str) -> int:
    ddf = dd.read_parquet(dataset_path)
    ddf = ddf.compute()
    instId = ddf.head(1).iloc[0]['instId']
    books = Books(instId)
    
    print(f'{instId} -> Total num of rows: {len(ddf)})')
    
    passed_counter = 0
    failed_counter = 0
    current_checksum = None
    last_ts = 0
    for timestamp, sub_ddf in ddf.groupby('timestamp'):
        if timestamp - last_ts > 60*1000: # 数据中断
            if sub_ddf['action'].iloc[0] != 'snapshot':
                continue
            # else:
                # print(f'{instId} -> Restart at ts({timestamp})----------------<')
        
        last_ts = timestamp
        
        correct_checksum = sub_ddf['checksum'].iloc[0]
        books.update_many(sub_ddf)
        
        cal_checksum = books.checksum
        if cal_checksum != correct_checksum:
            failed_counter += 1
            # print((f"incorrect checksum at ts({timestamp}): {cal_checksum} != {correct_checksum}"))
            # print(f'Current: {passed_counter} / {failed_counter} : {100*passed_counter/(passed_counter+failed_counter)}%')
            # print(f'{instId} -> Current checksum_text: {books.checksum_text()}')
            # raise Exception(f"{instId} -> incorrect checksum at ts({timestamp}): {cal_checksum} != {correct_checksum}")
        else:
            passed_counter += 1
        if (passed_counter+failed_counter) % 1000000 == 0:
            print(f'{instId} -> Current: {passed_counter} / {failed_counter} : {100*passed_counter/(passed_counter+failed_counter)}%')
    print(f'{instId} -> Result: {passed_counter} / {failed_counter} : {100*passed_counter/(passed_counter+failed_counter)}%')
    return failed_counter

if __name__ == '__main__':
    params = [
        'ETH-USDT-230623-400',
        'ETH-USDT-230630-400',
        'ETH-USDT-230714-400',
        'ETH-USDT-230721-400',
        'ETH-USDT-230728-400',
        'ETH-USDT-230804-400',
        'ETH-USDT-230811-400',
        'ETH-USDT-230929-400',
        'ETH-USDT-231229-400',
        'LTC-USDT-230623-400',
        'LTC-USDT-230714-400',
        'LTC-USDT-230721-400',
        'LTC-USDT-230804-400',
        'LTC-USDT-230811-400',
        'LTC-USDT-230929-400',
        'LTC-USDT-231229-400',
        'LTC-USD-230630-400',
        'LTC-USD-230714-400',
        'LTC-USD-230721-400',
        'LTC-USD-230728-400',
        'LTC-USD-230804-400',
        'LTC-USD-230811-400',
        'LTC-USD-230929-400',
        'LTC-USD-231229-400',
    ]
    tasks = [ dataset[x] for x in params]
    with Pool(6) as p:
        results = p.map(verify, tasks)
    print(results)