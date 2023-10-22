import os
from pathlib import Path
import sys
import time
sys.path.append('D:\\Project')
from pybacktest.src.bookcore import BookCore, PD_DF_DTYPES
import pandas as pd
from typing import List, Dict, Tuple

class DataSetFactory:
    def __init__(self, instId: str, path: Path, chunk_size: int = 1_000_000) -> None:
        self.out_path = path
        # Make sure the path existed. 
        os.makedirs(path, exist_ok=True)
        self.chunk_size = chunk_size
        self._bc = BookCore(instId)
        self.counted_ts = 0
        self.cur_df = pd.DataFrame(columns=PD_DF_DTYPES.index)
        self.cur_ts = -1
        self.partition_id = 0
        
        
    def update(self, row: pd.Series) -> None:
        start_time = time.time()
        self._bc.set(row)

        if self.cur_ts != row['timestamp']:
            if self.counted_ts % self.chunk_size == 0 and self.counted_ts >= 1:
                if not self.cur_df.empty:
                    self._dump() # dump a partitioned parquet file.
                self.cur_df = self.get_snapshot(row['timestamp'])
            elif self.counted_ts == 1: # Finished loading; get snapshot
                self.cur_df = self.get_snapshot(self.cur_ts)
                self.cur_df.loc[len(self.cur_df)] = row
            else: # A new ts point within partitioned parquet file.
                self.cur_df.loc[len(self.cur_df)] = row
            
            if self.counted_ts == 0 and row['action'] != 'snapshot':
                print("[ERROR] The first data point must be snapshot")
                exit(-1)
            
            self.cur_ts = row['timestamp']
            self.counted_ts += 1
            print(f'current ts point {self.counted_ts}')
        else:
            self.cur_df.loc[len(self.cur_df)] = row
            elapsed_time = time.time() - start_time
            print(f'1: {elapsed_time:.5f}')

        elapsed_time = time.time() - start_time
        print(f'2: {elapsed_time:.5f}')
    
    
    def close(self) -> None:
        if not self.cur_df.empty:
            self._dump()
            self.cur_df = None
        
    
    # Generate partitioned parquet files
    def _dump(self) -> None:
        start_ts = self.cur_df['timestamp'].min()
        end_ts = self.cur_df['timestamp'].max()
        filename = f'part-{self.partition_id}-{start_ts}-{end_ts}.parquet'
        self.cur_df.to_parquet(
            self.out_path / filename, 
            engine='pyarrow', 
            compression='gzip', 
            index=False
        )
        print(f'Dump a partition to {self.out_path / filename}')
    
    
    def get_snapshot(self, ts: int) -> pd.DataFrame:
        snapshot = pd.DataFrame(columns=PD_DF_DTYPES.index)
        
        for bl in self._bc.asks:
            snapshot.loc[len(snapshot)] = {
                'price': bl.price, 
                'size': bl.amount, 
                'numOrders': bl.count,
                'action': 'snapshot'
                }
        for bl in self._bc.bids:
            snapshot.loc[len(snapshot)] = {
                'price': bl.price, 
                'size': bl.amount, 
                'numOrders': bl.count,
                'action': 'snapshot'
                }
        
        snapshot['timestamp'] = ts
        return snapshot



class DataSetFactory2:
    def __init__(self, instId: str, path: Path, chunk_size: int = 100_000) -> None:
        self.out_path = path
        # Make sure the path existed. 
        os.makedirs(path, exist_ok=True)
        self.chunk_size = chunk_size
        self._bc = BookCore(instId)
        self.counted_ts = 0
        self.cur_chunk = []
        self.cur_ts = -1
        self.partition_id = 0
        
        
    def update(self, row: dict) -> None:
        # start_time = time.time()
        self._bc.set(row)

        if self.cur_ts != row['timestamp']:
            if self.counted_ts % self.chunk_size == 0 and self.counted_ts >= 1:
                if self.cur_chunk:
                    self._dump() # dump a partitioned parquet file.
                self.cur_chunk = self.get_snapshot(row['timestamp'])
                # self.counted_ts = 0
            elif self.counted_ts == 1: # Finished loading; get snapshot
                self.cur_chunk = self.get_snapshot(self.cur_ts)
                self.cur_chunk.append(row)
            else: # A new ts point within partitioned parquet file.
                self.cur_chunk.append(row)

            if self.counted_ts == 0 and row['action'] != 'snapshot':
                print("[ERROR] The first data point must be snapshot")
                exit(-1)
            
            self.cur_ts = row['timestamp']
            self.counted_ts += 1
            if self.counted_ts % (self.chunk_size/100) == 0:
                print(f'{(self.counted_ts%self.chunk_size)/self.chunk_size*100:.3f}%')
        else:
            self.cur_chunk.append(row)
            # elapsed_time = time.time() - start_time
            # print(f'1: {elapsed_time:.5f}')

        # elapsed_time = time.time() - start_time
        # print(f'2: {elapsed_time:.5f}')
    
    
    def close(self) -> None:
        if self.cur_chunk:
            self._dump()
            self.cur_chunk = None
        
    
    # Generate partitioned parquet files
    def _dump(self) -> None:
        cur_df = pd.DataFrame(self.cur_chunk)
        start_ts = cur_df['timestamp'].min()
        end_ts = cur_df['timestamp'].max()
        filename = f'part-{self.partition_id}-{start_ts}-{end_ts}.parquet'
        self.partition_id += 1
        cur_df.to_parquet(
            self.out_path / filename, 
            engine='pyarrow', 
            compression='gzip', 
            index=False
        )
        print(f'Dump a partition to {self.out_path / filename}')
    
    
    def get_snapshot(self, ts: int) -> list:
        snapshot = []
        for bl in self._bc.asks:
            snapshot.append({
                'price': bl.price, 
                'size': bl.amount, 
                'numOrders': bl.count,
                'side': 'ask',
                'action': 'snapshot',
                'timestamp': ts
                })
        for bl in self._bc.bids:
            snapshot.append({
                'price': bl.price, 
                'size': bl.amount, 
                'numOrders': bl.count,
                'side': 'bid',
                'action': 'snapshot',
                'timestamp': ts
                })
        
        return snapshot


