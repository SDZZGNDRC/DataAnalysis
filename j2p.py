from multiprocessing import Pool
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any, Tuple
import os
import json
import sys

from parse_dataFileName import parse_dataFileName

def get_tasks(dataSource: str, json_path: Path, output_path: Path) -> List[Tuple[Path, Path]]:
    files = filter(
        lambda x: x.startswith(f'OKX-{dataSource}-') and x.endswith('.json'),
        json_path.glob(f'OKX-{dataSource}-*.json')
    )
    if len(files) == 0:
        print(f'Can not find json files matching `OKX-{dataSource}-*.json` under: {json_path}')
        exit(-1)
    files = filter(
        lambda x: parse_dataFileName(x.name)['dataSource'] == dataSource, 
        files
    )
    tasks: List[Tuple[Path, Path]] = list(map(
        lambda x: (x, output_path / x.name.replace('.json', '.parquet')),
        files
    ))
    return tasks

def trades(json_path: Path, output_path: Path):
    tasks = get_tasks('Trades', json_path, output_path)
    
    def flatten_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
        # Initialize an empty list to store the flattened data
        flattened_data: List[List[Any]] = []
        columns = ['instId', 'timestamp', 'tradeId', 'px', 'sz', 'side']
        
        # Loop through each data object
        for obj in data:
            # length of the 'data' list should be 1
            if len(obj['data']) != 1:
                raise ValueError(f"Length of the 'data' list should be 1. Got {len(obj['data'])}")
            
            instId = obj['data'][0]['instId']
            timestamp = int(obj['data'][0]['ts'])
            tradeId = int(obj['data'][0]['tradeId'])
            px = float(obj['data'][0]['px'])
            sz = float(obj['data'][0]['sz'])
            side = obj['data'][0]['side']
            
            flattened_data.append([instId, timestamp, tradeId, px, sz, side])
        # Sort the flattened_data by timestamp
        flattened_data.sort(key=lambda x: x[1])
        return pd.DataFrame(flattened_data, columns=columns)
    def json2parquet(argvs: Tuple[str, str]) -> None:
        '''
        Convert a json file to a parquet file.
        '''
        json_file, parquet_file = argvs
        with open(json_file) as f:
            raw_data = json.load(f)
            data = raw_data['data']
        df = flatten_data(data)
        df.to_parquet(parquet_file, compression='gzip', index=False)
    # get the number of cpu cores to use
    num_cpu = os.cpu_count()
    with Pool(num_cpu) as p:
        p.map(json2parquet, tasks)

def tickers(json_path: Path, output_path: Path):
    tasks = get_tasks('Tickers', json_path, output_path)
    
    def flatten_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
        # Initialize an empty list to store the flattened data
        flattened_data: List[List[Any]] = []
        columns = [
            'instType', 'instId', 'timestamp',
            'last', 'lastSz', 'askPx', 'askSz', 'bidPx', 'bidSz',
            'open24h', 'high24h', 'low24h', 'volCcy24h', 'vol24h',
            'sodUtc0', 'sodUtc8'
        ]
        
        # Loop through each data object
        for obj in data:
            # length of the 'data' list should be 1
            if len(obj['data']) != 1:
                raise ValueError(f"Length of the 'data' list should be 1. Got {len(obj['data'])}")
            
            instType = obj['data'][0]['instType']
            instId = obj['data'][0]['instId']
            timestamp = int(obj['data'][0]['ts'])
            last = float(obj['data'][0]['last'])
            lastSz = float(obj['data'][0]['lastSz'])
            askPx = float(obj['data'][0]['askPx'])
            askSz = float(obj['data'][0]['askSz'])
            bidPx = float(obj['data'][0]['bidPx'])
            bidSz = float(obj['data'][0]['bidSz'])
            open24h = float(obj['data'][0]['open24h'])
            high24h = float(obj['data'][0]['high24h'])
            low24h = float(obj['data'][0]['low24h'])
            volCcy24h = float(obj['data'][0]['volCcy24h'])
            vol24h = float(obj['data'][0]['vol24h'])
            sodUtc0 = float(obj['data'][0]['sodUtc0'])
            sodUtc8 = float(obj['data'][0]['sodUtc8'])
            
            flattened_data.append([
                instType, instId, timestamp,
                last, lastSz, askPx, askSz, bidPx, bidSz, 
                open24h, high24h, low24h, volCcy24h, vol24h, 
                sodUtc0, sodUtc8
            ])
        # Sort the flattened_data by timestamp
        flattened_data.sort(key=lambda x: x[2])
        return pd.DataFrame(flattened_data, columns=columns)
    def json2parquet(argvs: Tuple[str, str]) -> None:
        '''
        Convert a json file to a parquet file.
        '''
        json_file, parquet_file = argvs
        with open(json_file) as f:
            raw_data = json.load(f)
            data = raw_data['data']
        df = flatten_data(data)
        df.to_parquet(parquet_file, compression='gzip', index=False)
    # get the number of cpu cores to use
    num_cpu = os.cpu_count()
    with Pool(num_cpu) as p:
        p.map(json2parquet, tasks)

def openInterest(json_path: Path, output_path: Path):
    tasks = get_tasks('OpenInterest', json_path, output_path)
    
    def flatten_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
        # Initialize an empty list to store the flattened data
        flattened_data: List[List[Any]] = []
        columns = ['instType', 'instId', 'timestamp', 'oi', 'oiCcy']
        
        # Loop through each data object
        for obj in data:
            # length of the 'data' list should be 1
            if len(obj['data']) != 1:
                raise ValueError(f"Length of the 'data' list should be 1. Got {len(obj['data'])}")
            
            instType = obj['data'][0]['instType']
            instId = obj['data'][0]['instId']
            timestamp = int(obj['data'][0]['ts'])
            oi = int(obj['data'][0]['oi'])
            oiCcy = float(obj['data'][0]['oiCcy'])
            
            flattened_data.append([instType, instId, timestamp, oi, oiCcy])
        # Sort the flattened_data by timestamp
        flattened_data.sort(key=lambda x: x[2])
        return pd.DataFrame(flattened_data, columns=columns)
    def json2parquet(argvs: Tuple[str, str]) -> None:
        '''
        Convert a json file to a parquet file.
        '''
        json_file, parquet_file = argvs
        with open(json_file) as f:
            raw_data = json.load(f)
            data = raw_data['data']
        df = flatten_data(data)
        df.to_parquet(parquet_file, compression='gzip', index=False)
    # get the number of cpu cores to use
    num_cpu = os.cpu_count()
    with Pool(num_cpu) as p:
        p.map(json2parquet, tasks)


def markPrice(json_path: Path, output_path: Path):
    tasks = get_tasks('MarkPrice', json_path, output_path)
    
    def flatten_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
        # Initialize an empty list to store the flattened data
        flattened_data: List[List[Any]] = []
        columns = ['instType', 'instId', 'timestamp', 'markPx']
        
        # Loop through each data object
        for obj in data:
            # length of the 'data' list should be 1
            if len(obj['data']) != 1:
                raise ValueError(f"Length of the 'data' list should be 1. Got {len(obj['data'])}")
            
            instType = obj['data'][0]['instType']
            instId = obj['data'][0]['instId']
            timestamp = int(obj['data'][0]['ts'])
            markPx = float(obj['data'][0]['markPx'])
            
            flattened_data.append([instType, instId, timestamp, markPx])
        # Sort the flattened_data by timestamp
        flattened_data.sort(key=lambda x: x[2])
        return pd.DataFrame(flattened_data, columns=columns)
    def json2parquet(argvs: Tuple[str, str]) -> None:
        '''
        Convert a json file to a parquet file.
        '''
        json_file, parquet_file = argvs
        with open(json_file) as f:
            raw_data = json.load(f)
            data = raw_data['data']
        df = flatten_data(data)
        df.to_parquet(parquet_file, compression='gzip', index=False)
    # get the number of cpu cores to use
    num_cpu = os.cpu_count()
    with Pool(num_cpu) as p:
        p.map(json2parquet, tasks)

def liquidationOrders(json_path: Path, output_path: Path):
    tasks = get_tasks('LiquidationOrders', json_path, output_path)

    def flatten_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
        # Initialize an empty list to store the flattened data
        flattened_data: List[List[Any]] = []
        columns = ['instType', 'instId', 'timestamp', 'uly', 'side', 'posSide', 'bkPx', 'sz', 'bkLoss', 'ccy']
        # Loop through each data object
        for obj in data:
            # length of the 'data' list should be 1
            if len(obj['data']) != 1:
                raise ValueError(f"Length of the 'data' list should be 1. Got {len(obj['data'])}")
            
            instType = obj['data'][0]['instType']
            instId = obj['data'][0]['instId']
            uly = obj['data'][0]['uly']

            for d in obj['data'][0]['details']:
                side = d.get('side', '')
                posSide = d.get('posSide', '')
                bkPx = d.get('bkPx', '')
                sz = d.get('sz', '')
                bkLoss = d.get('bkLoss', '')
                ccy = d.get('ccy', '')
                ts = int(d.get('ts', ''))
                flattened_data.append(
                    [instType, instId, ts, uly, side, posSide, bkPx, sz, bkLoss, ccy],
                )
        # Sort the flattened_data by timestamp
        flattened_data.sort(key=lambda x: x[2])
        return pd.DataFrame(flattened_data, columns=columns)
    def json2parquet(argvs: Tuple[str, str]) -> None:
        '''
        Convert a json file to a parquet file.
        '''
        json_file, parquet_file = argvs
        with open(json_file) as f:
            raw_data = json.load(f)
            data = raw_data['data']
        df = flatten_data(data)
        df.to_parquet(parquet_file, compression='gzip', index=False)
    # get the number of cpu cores to use
    num_cpu = os.cpu_count()
    with Pool(num_cpu) as p:
        p.map(json2parquet, tasks)

def indexTickers(json_path: Path, output_path: Path) -> None:
    tasks = get_tasks('IndexTickers', json_path, output_path)
    
    def flatten_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
        # Initialize an empty list to store the flattened data
        flattened_data: List[List[Any]] = []
        
        # Loop through each data object
        for obj in data:
            # length of the 'data' list should be 1
            if len(obj['data']) != 1:
                raise ValueError(f"Length of the 'data' list should be 1. Got {len(obj['data'])}")
            
            instId = obj['arg']['instId']
            timestamp = int(obj['data'][0]['ts'])

            idxPx   = float(obj['data'][0]['idxPx'])
            high24h = float(obj['data'][0]['high24h'])
            low24h  = float(obj['data'][0]['low24h'])
            open24h = float(obj['data'][0]['open24h'])
            sodUtc0 = float(obj['data'][0]['sodUtc0'])
            sodUtc8 = float(obj['data'][0]['sodUtc8'])
            flattened_data.append([instId, timestamp, idxPx, high24h, low24h, open24h, sodUtc0, sodUtc8])
        # Sort the flattened_data by timestamp
        flattened_data.sort(key=lambda x: x[1])
        return pd.DataFrame(flattened_data, columns=[ 'instId', 'timestamp', 'idxPx', 'high24h', 'low24h', 'open24h', 'sodUtc0', 'sodUtc8'])

    def json2parquet(argvs: Tuple[str, str]) -> None:
        '''
        Convert a json file to a parquet file.
        '''
        json_file, parquet_file = argvs
        with open(json_file) as f:
            raw_data = json.load(f)
            data = raw_data['data']
        df = flatten_data(data)
        df.to_parquet(parquet_file, compression='gzip', index=False)
    # get the number of cpu cores to use
    num_cpu = os.cpu_count()
    with Pool(num_cpu) as p:
        p.map(json2parquet, tasks)

def books(json_path: Path, output_path: Path) -> None:
    '''
    json_path and output_path must be dirs and are existed.
    '''
    tasks = get_tasks('Books', json_path, output_path)
    
    def flatten_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
        # Initialize an empty list to store the flattened data
        flattened_data: List[List[Any]] = []
        
        # Loop through each data object
        for obj in data:
            # length of the 'data' list should be 1
            if len(obj['data']) != 1:
                raise ValueError(f"Length of the 'data' list should be 1. Got {len(obj['data'])}")
            
            instId = obj['arg']['instId']
            timestamp = int(obj['data'][0]['ts'])
            action = obj['action']
            checksum = int(obj['data'][0]['checksum'])
            
            if 'prevSeqId' in obj['data'][0]:
                prevSeqId = obj['data'][0]['prevSeqId']
            else:
                prevSeqId = None
            
            if 'seqId' in obj['data'][0]:
                seqId = obj['data'][0]['seqId']
            else:
                seqId = None
            
            # Loop through the asks and bids
            for side, orders in [('ask', obj['data'][0]['asks']), ('bid', obj['data'][0]['bids'])]:
                # Loop through each order
                for order in orders:
                    price, size, _, numOrders = order # NOTICE: the third element is deprecated according to the OKX API documentation
                    # Append the flattened order to the list
                    flattened_data.append([instId, price, size, int(numOrders), side, int(timestamp), prevSeqId, seqId, action, checksum])
        # Sort the flattened_data by timestamp
        flattened_data.sort(key=lambda x: x[5])
        return pd.DataFrame(flattened_data, columns=[ 'instId', 'price', 'size', 'numOrders', 'side', 'timestamp', 'prevSeqId', 'seqId', 'action', 'checksum'])
    
    def json2parquet(argvs: Tuple[str, str]) -> None:
        '''
        Convert a json file to a parquet file.
        '''
        json_file, parquet_file = argvs
        with open(json_file) as f:
            raw_data = json.load(f)
            data = raw_data['data']
        df = flatten_data(data)
        df.to_parquet(parquet_file, compression='gzip', index=False)
    # get the number of cpu cores to use
    num_cpu = os.cpu_count()
    with Pool(num_cpu) as p:
        p.map(json2parquet, tasks)




if __name__ == '__main__':
    data_types = {
        'books': books,
        'indexTickers': indexTickers,
        'liquidationOrders': liquidationOrders,
        'markPrice': markPrice,
        'openInterest': openInterest,
        'tickers': tickers,
        'trades': trades
    }
    if len(sys.argv[1:]) != 3:
        print('Usage: python script_name.py <data_type> <input_directory> <output_directory>')
        print('Arguments:')
        print('  data_type: Type of data to convert.')
        print('\n'.join(map(lambda x: f'\t* {x}', data_types.keys())))
        print('  input_directory: Path to the directory containing the JSON files to be converted to Parquet.')
        print('  output_directory: Path to the directory where the Parquet files will be saved.')
        exit(0)
    json_path, output_path = Path(sys.argv[2]), Path(sys.argv[3])
    data_type = sys.argv[1]
    if data_type not in data_types:
        print('Invalid data type.')
        exit(-1)
    if json_path.is_file():
        print('Input must be a directory')
        exit(-1)
    json_path.mkdir(exist_ok=True)
    if output_path.is_file():
        print('Output must be a directory')
        exit(-1)
    output_path.mkdir(exist_ok=True)
    
    data_types[data_type](json_path, output_path)


