import os
from pathlib import Path
import sys
sys.path.append('D:\\Project')
from pybacktest.src.bookcore import BookCore
import pandas as pd
from typing import List, Dict, Literal, Tuple

class DataSetFactory:
    def __init__(self, instId: str, path: Path, chunk_size: int = 100_000, check_instId: bool = True) -> None:
        self.out_path = path
        # Make sure the path existed. 
        os.makedirs(path, exist_ok=True)
        assert chunk_size >= 2
        self.chunk_size = chunk_size
        self._bc = BookCore(instId, check_instId)
        self.instId = instId
        self.counted_ts = 0
        self.cur_chunk = []
        self.cur_ts = -1
        self.partition_id = 0
        self.state: Literal['INIT', 'LOAD'] = 'INIT'
        
    
    def update(self, new_row: dict) -> None:
        if self.counted_ts == 0 and new_row['action'] != 'snapshot':
            print("[ERROR] The first data point must be snapshot")
            exit(-1)
        
        if self.state == 'INIT':
            if self.cur_ts != new_row['timestamp']:
                if self.counted_ts % self.chunk_size == 1: # Finished INIT
                    self.state = 'LOAD'
                    self.cur_chunk = self.get_snapshot(self.cur_ts)
                    self.cur_chunk.append(new_row)
                self.counted_ts += 1
                self.cur_ts = new_row['timestamp']
            else: # Wait for INIT to finished
                pass
        elif self.state == 'LOAD':
            if self.cur_ts != new_row['timestamp']:
                if self.counted_ts % self.chunk_size == 0: # Finished LOAD
                    self.state = 'INIT'
                    self._dump() # Generate partition file
                else:
                    self.cur_chunk.append(new_row)
                self.cur_ts = new_row['timestamp']
                self.counted_ts += 1
                
                if self.counted_ts % (self.chunk_size/100) == 0:
                    print(f'Loaded {(self.counted_ts%self.chunk_size)/self.chunk_size*100:.1f}%')
            else:
                self.cur_chunk.append(new_row)
            
        else:
            print(f'UNKNOWN STATE: {self.state}')
            exit(-1)
        self._bc.set(new_row)
    
    
    def close(self) -> None:
        if self.cur_chunk:
            self._dump()
            self.cur_chunk = None
        
    
    # Generate partitioned parquet files
    def _dump(self) -> None:
        cur_df = pd.DataFrame(self.cur_chunk)
        start_ts = cur_df['timestamp'].min()
        end_ts = cur_df['timestamp'].max()
        cur_df['instId'] = self.instId
        filename = f'part-{self.partition_id}-{start_ts}-{end_ts}.parquet'
        self.partition_id += 1
        cur_df.to_parquet(
            self.out_path / filename, 
            engine='pyarrow', 
            compression='gzip', 
            index=False
        )
        self.cur_chunk = None
        print(f'Dump a partition to {self.out_path / filename}')

    
    
    def get_snapshot(self, ts: int) -> list:
        snapshot = []
        snapshot.extend([
            {
                'price': bl.price, 
                'size': bl.amount, 
                'numOrders': bl.count,
                'side': 'ask',
                'action': 'snapshot',
                'timestamp': ts
            } for bl in self._bc.asks
        ])
        snapshot.extend([
            {
                'price': bl.price, 
                'size': bl.amount, 
                'numOrders': bl.count,
                'side': 'bid',
                'action': 'snapshot',
                'timestamp': ts
            } for bl in self._bc.bids
        ])
        
        return snapshot


