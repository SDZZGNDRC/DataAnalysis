import time
import glob
import os
import math
from pathlib import Path
from typing import Dict, List, Literal, Tuple, Union, Optional
import pandas as pd
import numpy as np
from copy import deepcopy

from datetime import datetime, timezone
import matplotlib.pyplot as plt

import multiprocessing
from functools import partial

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WritePrecision

from pybacktest.src.books import Book
from pybacktest.src.bookcore import BookCore
from pybacktest.src.simTime import SimTime

def unix_ms_to_iso(unix_timestamp_ms):
    # Convert milliseconds to seconds
    unix_timestamp_s = unix_timestamp_ms / 1000.0
    
    # Create a datetime object from the Unix timestamp
    dt = datetime.fromtimestamp(unix_timestamp_s, tz=timezone.utc)
    
    # Convert to ISO format
    iso_format = dt.isoformat()
    
    return iso_format

class BL:
    '''
    BookLevel
    '''
    def __init__(
                self, N: Union[int,List[int]], instId: str,
                start: int, end: int,
                path: Path, max_interval: int = 10_000, 
                side: Literal['ask', 'bid'] = 'ask',
                step: int = 1000, check_instId: bool = True) -> None:
        if isinstance(N, list):
            if len(N) == 0:
                raise ValueError("N must be a non-empty list of integers.")
            self.Ns = list(map(int,N))
        else:
            if N <= 0:
                raise ValueError(f"N must be positive, but {N} was given.")
            self.Ns = [i for i in range(N)]
        self.Ns.sort()
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
    
    @staticmethod
    def _calc(bookcore: BookCore, Ns: List[int], side: str) -> Dict[int,float]:
        res: Dict[int,float] = {}
        Ns.sort()
        if side == 'ask':
            L = bookcore.depth_asks
            if L < Ns[-1]:
                raise ValueError(f"Ns[-1] must be smaller than max-depth {L}, but {L} < {Ns[-1]}")
            asks = bookcore.asks[:Ns[-1]+1]
            for n in Ns:
                res[n] = asks[n]
        elif side == 'bid':
            L = bookcore.depth_bids
            if L < N:
                raise ValueError(f"Ns[-1] must be smaller than max-depth {L}, but {L} < {Ns[-1]}")
            bids = bookcore.bids[:Ns[-1]+1]
            for n in Ns:
                res[n] = bids[n]
        else:
            raise ValueError(f"side must be 'ask' or 'bid', but {side} given...")
        return res
    
    
    def _gen(self) -> None:
        start_time = time.time()

        if self._data:
            return
        print('BL: generating data...')

        # Find relevant parquet files
        relevant_files = self._get_relevant_files()
        print(f'relevant_files: {relevant_files}')

        # Determine the number of processes to use
        num_processes = min(multiprocessing.cpu_count(), len(relevant_files))
        print(f'num_processes: {num_processes}')

        # Split the files into chunks for each process
        file_chunks = self._split_files(relevant_files, num_processes)
        print(f'file_chunks: {file_chunks}')

        # Create a pool of worker processes
        with multiprocessing.Pool(processes=num_processes) as pool:
            # Use partial to create a function with fixed arguments
            partial_process_chunk = partial(self._process_chunk, instId=self.instId, Ns=self.Ns, 
                                            side=self.side, step=self.step, check_instId=self.check_instId)
            
            # Map the chunks to the worker processes
            results = pool.map(partial_process_chunk, file_chunks)

        # Combine results from all processes
        combined_data = {}
        for n in self.Ns:
            combined_data[n] = pd.concat(results[n]).sort_index()

        self._data = combined_data
        print('BL: data generation complete.')
        print(self._data.shape)

        end_time = time.time()
        total_time = end_time - start_time
        num_entries = len(self._data[self.Ns[0]]) * len(self.Ns)
        avg_time_per_entry = total_time / num_entries if num_entries > 0 else 0

        print(f"Total time consumption: {total_time:.2f} seconds")
        print(f"Number of entries generated: {num_entries}")
        print(f"Average time per entry: {avg_time_per_entry:.8f} seconds")

    def _get_relevant_files(self):
        relevant_files = []
        for file in glob.glob(os.path.join(self.path, 'part-*-*-*.parquet')):
            start, end = map(int, os.path.splitext(os.path.basename(file))[0].split('-')[2:])
            if (start <= self.end and end >= self.start) or (self.start <= end and self.end >= start):
                relevant_files.append(file)
        return sorted(relevant_files)

    def _split_files(self, files, num_chunks):
        chunk_size = len(files) // num_chunks
        return [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]

    @staticmethod
    def _process_chunk(file_chunk, instId, Ns, side, step, check_instId) -> Dict[int,pd.Series]:
        start, end = map(int, os.path.splitext(os.path.basename(file_chunk[0]))[0].split('-')[2:])
        simTime = SimTime(start,end)
        book = Book(instId, simTime, Path(os.path.dirname(file_chunk[0])), check_instId=check_instId)
        data = {n: {} for n in Ns}
        # print(f'_process_chunk: original start {start}, end {end}')
        for file in file_chunk:
            start, end = map(int, os.path.splitext(os.path.basename(file))[0].split('-')[-2:])
            if start % step != 0:
                start = start - (start % step) + step
            if end % step != 0:
                end   = end - (end % step)
            if simTime < start:
                simTime.set(start)
            # print(f'_process_chunk: {start}, {end}')
            while True:
                cur = book.core
                val = BL._calc(cur, Ns, side)
                for n in Ns:
                    data[n][simTime.to_Timestamp()] = val[n]
                # print(f'_process_chunk: {simTime.to_Timestamp()}')
                if simTime + step <= min(end, simTime.end):
                    simTime.add(step)
                else:
                    break
        # print(f'_process_chunk: {data}')
        res = {}
        for n in Ns:
            res[n] = pd.Series(data[n])
        return res

    # def __getitem__(self, key):
    #     if isinstance(key, int):
    #         return self._data[pd.Timestamp(key, unit='ms')]
    #     elif isinstance(key, slice):
    #         return self._data[key]
    #     elif isinstance(key, pd.Timestamp):
    #         return self._data[key]
    #     else:
    #         raise TypeError("Invalid key type. Key must be an integer or a slice.")
    
    
    # def __iter__(self):
    #     return iter(deepcopy(self._data))
    
    
    # def __len__(self):
    #     return len(self._data)

    def dump(self, dest: Literal['influxdb'], 
            url: str, token: str, org: str, bucket: str) -> None:
        '''
        Upload data to destination, currently only influxdb is supported.
        
        Parameters:
        - dest: Destination type (currently only 'influxdb' is supported)
        - url: InfluxDB server URL
        - token: InfluxDB authentication token
        - org: InfluxDB organization
        - bucket: InfluxDB bucket name
        '''
        if dest != 'influxdb':
            raise ValueError(f"Unsupported destination: {dest}. Only 'influxdb' is currently supported.")

        if self._data is None or len(self._data) == 0:
            print("No data available to dump. Make sure to generate data first.")
            return

        client = InfluxDBClient(url=url, token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        print(f'BL: uploading data to {dest}...')
        
        total_points = len(self._data)
        for i, (timestamp, value) in enumerate(self._data.items(), 1):
            # FIXME: Use batch-write instead.
            point = Point("BL") \
                .tag("instId", self.instId) \
                .tag("side", self.side) \
                .tag("N", self.N) \
                .field("value", value) \
                .time(timestamp.isoformat(),write_precision=WritePrecision.S)
            write_api.write(bucket=bucket, record=point)
            
            # Print progress every 10% or for every point if total_points < 10
            if total_points >= 10 and (i % (total_points // 10) == 0 or i == total_points):
                progress = (i / total_points) * 100
                print(f"Uploading data: {progress:.1f}%")
            elif total_points < 10:
                progress = (i / total_points) * 100
                print(f"Uploading data: {progress:.1f}%")
        
        print('AAP: data upload complete.')
        client.close()
        print(f"Data successfully uploaded to InfluxDB bucket: {bucket}")

    @property
    def data(self) -> pd.Series:
        return deepcopy(self._data)



if __name__ == "__main__":
    N = 20
    instId = 'BTC-USDT-400'
    start = 1690825850000
    end   = 1693571964000
    path = Path(r'E:\out3\books\BTC-USDT-400')
    side = 'ask'
    dest = 'influxdb'
    url = 'http://localhost:8086'
    token = '4hlxwboUuip5etIZgB_OeFaOhX4Rs8J-a7Y0Nn25gojN6Opxoeb84kRBFh4x67S3Rce4pSBgCjfVMHDD5w-UUQ=='
    org = 'CryptoSpider'
    bucket = 'test3'

    # Create an AAP instance
    aap = AAP(N=N, instId=instId, start=start, end=end, path=path, side=side)

    # Print out the number of unique timestamps
    print(f"Number of unique timestamps: {len(aap.data.index.unique())}")

    # Dump the data to InfluxDB
    aap.dump(dest=dest, url=url, token=token, org=org, bucket=bucket)

    # Query the data from InfluxDB and plot it
    query = f'''
        from(bucket: "{bucket}")
        |> range(start: {unix_ms_to_iso(start)}, stop: {unix_ms_to_iso(end)})
        |> filter(fn: (r) => r["_measurement"] == "AAP")
        |> filter(fn: (r) => r["N"] == "20")
        |> filter(fn: (r) => r["instId"] == "BTC-USDT-400")
        |> filter(fn: (r) => r["side"] == "ask")
        |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()

    result = query_api.query_data_frame(query=query)
    print(len(result))
    if (isinstance(result, pd.DataFrame) and result.empty) or result is None:
        print("No data retrieved from InfluxDB")
    else:
        # Convert _time column to datetime
        result['_time'] = pd.to_datetime(result['_time'])

        # Plot the data
        plt.figure(figsize=(12, 6))
        plt.plot(result['_time'], result['value'])
        plt.title(f"AAP for {instId} ({side} side, N={N}) - Data from InfluxDB")
        plt.xlabel("Timestamp")
        plt.ylabel("AAP Value")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    client.close()