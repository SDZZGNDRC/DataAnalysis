import os
from pathlib import Path
from pybacktest.src.bookcore import BookCore
import pandas as pd
from typing import Literal

class DataSetFactory:
    # NOTICE: Only support Books now!
    """
    A factory class for creating and managing data chunks from a continuous data stream.

    This class handles the creation of data chunks from a stream of data rows (e.g., trading data)
    and generates partitioned parquet files for efficient storage and access. It ensures that the
    first data row is a snapshot and manages the state between initialization and loading phases.

    Attributes:
        out_path (Path): The output directory path where the partitioned parquet files are saved.
        chunk_size (int): The number of data rows to include in each data chunk (default is 100,000).
        instId (str): The instrument identifier for the data being processed.
        counted_ts (int): The number of timestamps processed in the current state.
        cur_chunk (list): The current chunk of data rows being processed.
        cur_ts (int): The current timestamp being processed.
        partition_id (int): An identifier for the current partition.
        state (Literal['INIT', 'LOAD']): The current state of the data processing ('INIT' or 'LOAD').

    Methods:
        update(new_row: dict): Updates the current data chunk with a new row of data.
        close(): Finalizes the data chunking process and ensures the last chunk is dumped.
        _dump(): Internal method to dump the current data chunk to a parquet file.
        get_snapshot(ts: int): Retrieves a snapshot of the current state of the order book.

    Raises:
        Exception: If the first data point is not a snapshot.
        Exception: If the chunk_size is less than 2 during initialization.
    """
    def __init__(self, instId: str, path: Path, chunk_size: int = 100_000, check_instId: bool = True, no_snapshot: bool = False, depth: int = 400) -> None:
        """
        Initializes the DataSetFactory with the given parameters and sets the initial state.

        Parameters:
            instId (str): The instrument identifier for the data being processed.
            path (Path): The output directory path where the partitioned parquet files will be saved.
            chunk_size (int): The number of data rows to include in each data chunk (default is 100,000).
            check_instId (bool): Flag to indicate whether to check the instrument identifier (default is True).
            no_snapshot (bool): Flag to indicate whether 
        """
        self.out_path = path
        # Make sure the path existed. 
        os.makedirs(path, exist_ok=True)
        assert chunk_size >= 2
        if chunk_size < 2:
            raise Exception('chunk_size must bigger than 1')
        self.chunk_size = chunk_size
        self._bc = BookCore(instId, check_instId)
        self.instId = instId
        self.no_snapshot = no_snapshot
        self.depth = depth
        self.counted_ts = 0
        self.cur_chunk = []
        self.cur_ts = -1
        self.partition_id = 0
        self.state: Literal['INIT', 'LOAD'] = 'INIT'
        
    
    def update(self, new_row: dict) -> None:
        """
        Updates the current data chunk with a new row of data.

        This method handles the transition between the 'INIT' and 'LOAD' states, appending new rows
        to the current chunk, and triggering the dump process when the chunk is complete.

        Parameters:
            new_row (dict): A dictionary representing the new row of data to be added to the current chunk.

        Raises:
            Exception: If the first data point is not a snapshot.
            Exception: If an unknown state is encountered.
        """
        if self.counted_ts == 0 and new_row['action'] != 'snapshot' and not self.no_snapshot:
            raise Exception('The first data point must be snapshot')
        if not (self.no_snapshot and not self._bc.filled(self.depth)):
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
                    if self.cur_ts > new_row['timestamp']:
                        raise ValueError("the inserted row's timestamp should bigger than the last one")
                    self.cur_chunk.append(new_row)
                
            else:
                raise Exception(f'unknown state {self.state}')
        self._bc.set(new_row)
    
    
    def close(self) -> None:
        """
        Finalizes the data chunking process.

        This method ensures that the last chunk is dumped if it contains any data.
        """
        if self.cur_chunk:
            self._dump()
            self.cur_chunk = None
        
    
    # Generate partitioned parquet files
    def _dump(self) -> None:
        """
        Internal method to dump the current data chunk to a parquet file.

        This method creates a DataFrame from the current chunk of data, generates a filename based on
        the timestamp range and partition identifier, and writes the DataFrame to a parquet file in the
        specified output directory.
        """
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
        """
        Retrieves a snapshot of the current state of the order book.

        This method compiles a list of the current asks and bids in the order book at a given timestamp.

        Parameters:
            ts (int): The timestamp for which the snapshot is to be retrieved.

        Returns:
            list: A list of dictionaries, each representing an order book entry at the given timestamp.
        """
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


