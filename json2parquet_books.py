from multiprocessing import Pool
import shutil
import sys
import pandas as pd
from typing import List, Dict, Any, Tuple
import os
import json



def parse_dataFileName(fileBaseName: str) -> dict:
    # example: OKX-Books-1INCH-USD-SWAP-400-1689297329268-1689298999939.7z
    items = os.path.splitext(fileBaseName)[0].split('-')
    if len(items) < 6 or (not fileBaseName.startswith('OKX-Books')):
        raise Exception('Invalid file name: ' + fileBaseName)
    
    exchange = items[0]
    dataSource = items[1]
    startTimestamp, endTimestamp = items[-2], items[-1]
    dsID = '-'.join(items[2:-2])
    
    return {
        'exchange': exchange,
        'dataSource': dataSource,
        'dsID': dsID,
        'startTimestamp': startTimestamp,
        'endTimestamp': endTimestamp,
    }

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
    return pd.DataFrame(flattened_data, columns=[ 'instId', 'price', 'size', 'numOrders', 'side', 'timestamp', 'prevSeqId', 'seqId', 'action', 'checksum'])

def json2parquet(json_file: str, parquet_file: str) -> None:
    '''
    Convert a json file to a parquet file.
    '''
    # json_file, parquet_file = files
    with open(json_file) as f:
        raw_data = json.load(f)
        data = raw_data['data']
    
    df = flatten_data(data)
    df.to_parquet(parquet_file, compression='gzip', index=False)

if __name__ == '__main__':
    if len(sys.argv[1:]) != 2:
        print('Please input two argv')
        exit(-1)
    jsonDir, destPath = sys.argv[1], sys.argv[2]
    if not (os.path.isdir(jsonDir) and os.path.isdir(destPath)):
        print('Please input directories')
        exit(-1)
    if not os.path.exists(jsonDir):
        print(f'jsonDir {jsonDir} not existed')
        exit(-1)

    os.makedirs(destPath, exist_ok=True)

    fileList: Dict[str, str] = {}

    for root, dirs, files in os.walk(jsonDir):
        for file in files:
            if file.endswith('.json'):
                fileList[file] = os.path.join(root, file)

    files_meta: Dict[str, List[str]] = {
        'exchange': [],
        'dataSource': [],
        'dsID': [],
        'startTimestamp': [],
        'endTimestamp': [],
        'filePath': [],
    }

    for fileBaseName, filePath in fileList.items():
        file_meta = parse_dataFileName(fileBaseName)
        files_meta['exchange'].append(file_meta['exchange'])
        files_meta['dataSource'].append(file_meta['dataSource'])
        files_meta['dsID'].append(file_meta['dsID'])
        files_meta['startTimestamp'].append(file_meta['startTimestamp'])
        files_meta['endTimestamp'].append(file_meta['endTimestamp'])
        files_meta['filePath'].append(filePath)

    files_metaData = pd.DataFrame(files_meta)

    tasks: List[Tuple[str, str]] = []
    for books_file in files_metaData[files_metaData['dataSource'] == 'Books']['filePath']:
        path, json_fileBaseName = os.path.split(books_file)
        parquet_file = os.path.join(destPath, json_fileBaseName.replace('.json', '.parquet'))
        tasks.append((books_file, parquet_file))
    
    # r = process_map(json2parquet, tasks, max_workers=6, chunksize=600)
    with Pool(6) as p:
        p.map(json2parquet, tasks)

