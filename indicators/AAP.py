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

class AAP:
    def __init__(
                self, N: int, instId: str,
                start: int, end: int,
                path: Path, max_interval: int = 10_000, 
                side: Literal['ask', 'bid'] = 'ask',
                step: int = 1000, check_instId: bool = True) -> None:
        if N <= 0:
            raise ValueError(f"N must be positive, but {N} was given.")
        self.N = N
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
    
    def _calc(self, bookcore: BookCore) -> float:
        res = 0.0
        if self.side == 'ask':
            L = bookcore.depth_asks
            if L < self.N:
                raise ValueError(f"N must be smaller than max-depth, but {L} < {self.N}")
            asks = bookcore.asks[:self.N]
            res = sum(map(lambda x: x.price, asks)) / self.N
        elif self.side == 'bid':
            L = bookcore.depth_bids
            if L < self.N:
                raise ValueError(f"N must be smaller than max-depth, but {L} < {self.N}")
            bids = bookcore.bids[:self.N]
            res = sum(map(lambda x: x.price, bids)) / self.N
        else:
            raise ValueError(f"side must be 'ask' or 'bid', but {self.side}")
        return res
    
    
    def _gen(self) -> None:
        # TODO: Profiling this method
        if self._data:
            return
        print('AAP: generating data...')
        simTime = SimTime(self.start, self.end)
        book = Book(
            self.instId, simTime, self.path,
            self.max_interval, self.check_instId
        )
        data: Dict[pd.Timestamp, float] = {}
        idx = []
        
        total_steps = (self.end - self.start) // self.step
        progress_step = max(1, total_steps // 10)  # Update progress every 10% or at least once
        
        for current_step in range(total_steps + 1):
            cur = book.core
            val = self._calc(cur)
            data[simTime.to_Timestamp()] = val
            idx.append(simTime.to_Timestamp())
            
            # Print progress
            if current_step % progress_step == 0 or current_step == total_steps:
                progress = (current_step / total_steps) * 100
                print(f"Generating data: {progress:.1f}%")
            
            if simTime + self.step <= self.end:
                simTime.add(self.step)
            else:
                break
        
        self._data = pd.Series(data, index=idx)
        print('AAP: data generation complete.')

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._data[pd.Timestamp(key, unit='ms')]
        elif isinstance(key, slice):
            return self._data[key]
        elif isinstance(key, pd.Timestamp):
            return self._data[key]
        else:
            raise TypeError("Invalid key type. Key must be an integer or a slice.")
    
    
    def __iter__(self):
        return iter(deepcopy(self._data))
    
    
    def __len__(self):
        return len(self._data)

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

        if self._data is None:
            raise ValueError("No data available to dump. Make sure to generate data first.")

        client = InfluxDBClient(url=url, token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        print(f'AAP: uploading data to {dest}...')
        
        total_points = len(self._data)
        for i, (timestamp, value) in enumerate(self._data.items(), 1):
            # print(f'debug: {timestamp}')
            point = Point("AAP") \
                .tag("instId", self.instId) \
                .tag("side", self.side) \
                .tag("N", self.N) \
                .field("value", value) \
                .time(timestamp.isoformat(),write_precision=WritePrecision.S)
            # print(f"Uploading point: {point}")
            write_api.write(bucket=bucket, record=point)
            
            # Print progress every 10%
            if i % (total_points // 10) == 0 or i == total_points:
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
    end   = 1690826850000
    path = Path(r'E:\out3\books\BTC-USDT-400')
    side = 'ask'
    dest = 'influxdb'
    url = 'http://localhost:8086'
    token = '4hlxwboUuip5etIZgB_OeFaOhX4Rs8J-a7Y0Nn25gojN6Opxoeb84kRBFh4x67S3Rce4pSBgCjfVMHDD5w-UUQ=='
    org = 'CryptoSpider'
    bucket = 'test2'

    # Create an AAP instance
    aap = AAP(N=N, instId=instId, start=start, end=end, path=path, side=side)

    # Print out the number of unique timestamps
    print(f"Number of unique timestamps: {len(aap.data.index.unique())}")

    # Print the unique timestamps
    print(f"Unique timestamps: {aap.data.index.unique()}")

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