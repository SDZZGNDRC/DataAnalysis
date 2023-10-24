from pathlib import Path
import sys
import time

from futures_alias import is_this_week
sys.path.append('D:\\Project')
from dataset_factory import DataSetFactory
import pandas as pd
import numpy as np
import os
import glob
from typing import List, Dict, Tuple

def split_dataframe(df, chunk_size) -> List[pd.DataFrame]:
    # Sort the dataframe by timestamp
    df.sort_values('timestamp', inplace=True)

    # Create a new column 'chunk_id' which is the cumulative sum of the difference between 
    # current and previous timestamp divided by the chunk_size
    df['chunk_id'] = (df['timestamp'].diff() > 0).cumsum()

    # Create chunks where the number of unique timestamps is at least chunk_size
    chunks = []
    for _, chunk in df.groupby(np.floor(df['chunk_id'] / chunk_size)):
        chunks.append(chunk.drop(columns='chunk_id'))

    return chunks


def gen(task):
    out_path, files = task
    full_df = pd.concat(
        pd.read_parquet(parquet_file)
        for parquet_file in files
    ).sort_values('timestamp')
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    
    # Drop the columns that are not needed; Change type of some columns
    full_df = full_df.drop(columns=['checksum', 'prevSeqId', 'seqId'])
    full_df['price'] = pd.to_numeric(full_df['price'], errors='raise')
    full_df['size'] = pd.to_numeric(full_df['size'], errors='raise')
    
    # Split the full_df into chunks
    # TODO: Ensure that each chunk have at least one snapshot

    # NOTICE: The chunk_size here is the distinct numbers of timestamps.
    chunk_size = 1_000_000 # The number of rows in each chunk
    chunks = split_dataframe(full_df, chunk_size)
    
    for i, chunk in enumerate(chunks):
        start_ts = chunk['timestamp'].min()
        end_ts = chunk['timestamp'].max()

        # Create the filename for this chunk
        filename = f'part-{i}-{start_ts}-{end_ts}.parquet'
        
        chunk.to_parquet(os.path.join(out_path, filename), engine='pyarrow', compression='gzip', index=False)

def gen2(task: Tuple[str, List[str]], chunk_size: int = 100_000):
    out_path, files = task
    dsId = os.path.basename(out_path.rstrip('\\'))
    instId = dsId.removesuffix('-400')
    meet_snapshot = False
    
    dataset_fact = DataSetFactory(instId, Path(out_path), chunk_size, False)
    
    total_files = len(files)

    for i, parquet_file in enumerate(files):
        print(f'Processing \t{i+1}/{total_files}')
        cur_df = pd.read_parquet(parquet_file).sort_values('timestamp')
        cur_df = cur_df.drop('prevSeqId', axis=1)
        cur_df = cur_df.drop('seqId', axis=1)
        cur_df = cur_df.drop('checksum', axis=1)
        cur_df['price'] = cur_df['price'].astype('float64')
        cur_df['size'] = cur_df['size'].astype('float64')
        cur_df['numOrders'] = cur_df['numOrders'].astype('int64')
        cur_df['timestamp'] = cur_df['timestamp'].astype('int64')
        cur_records = cur_df.to_dict(orient='records')

        for row in cur_records:
            if (not meet_snapshot) and row['action'] == 'snapshot':
                meet_snapshot = True
                tmp_ts = row['timestamp']
                print(f'meet snapshot at {tmp_ts}')
            elif (not meet_snapshot) and row['action'] != 'snapshot':
                continue
            
            dataset_fact.update(row)
        
    dataset_fact.close()
    if not meet_snapshot:
        print(f'Can not find any snapshot in any files')
        exit(-1)


def get_spot():
    files = glob.glob(r'E:\temp\parquet\BTC-USDT-400\OKX-Books-BTC-USDT-400-*.parquet')
    files = sorted(files)
    assert files
    gen2((r'E:\out3\books\BTC-USDT-400', files), chunk_size=500_000)
    

def get_futures_weekly():
    files = glob.glob('E:\\temp\\parquet\\BTC-USDT-FUTURES\\OKX-Books-BTC-USDT-*-400-*.parquet')
    files = sorted(files)
    assert files
    new_files = []
    for file in files:
        p = os.path.basename(file).split('-')
        date_s = p[4]
        start_ts = int(p[-2])/1000
        if is_this_week(start_ts, date_s):
            new_files.append(file)
    assert new_files
    gen2(('E:\\out3\\books\\BTC-USDT-TWEEK', new_files), chunk_size=500_000)

if __name__ == '__main__':
    get_futures_weekly()

    # grouped_files: Dict[str, List[str]] = {}
    # for file in files:
    #     items = file.split('-')
    #     dsID = '-'.join(items[2:-2])
        
    #     if dsID in grouped_files:
    #         grouped_files[dsID].append(file)
    #     else:
    #         grouped_files[dsID] = [file]


    # root_path = r'E:\out3'
    # tasks = []
    # for dsID, files in grouped_files.items():
    #     out_path = os.path.join(root_path, dsID)
        
    #     tasks.append((out_path, files))
    
    
    # finished_count = 0
    # chunked_size = 36

    # for task in tasks[:12]:
    #     gen(task)
    #     finished_count += 1
    #     print(f'{finished_count}/{len(tasks)}')

    # tasks = tasks[140:]
    # for chunked_tasks in [tasks[i:i + chunked_size] for i in range(0, len(tasks), chunked_size)]:
    #     # Create 6 processes to process the tasks
    #     p = Pool(3)
    #     p.map(gen, chunked_tasks)
    #     p.close()
    #     finished_count += len(chunked_tasks)
    #     print(f'{finished_count}/{len(tasks)}')
    #     del p

