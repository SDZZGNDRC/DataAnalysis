from pathlib import Path

from .futures_alias import is_this_week
from .dataset_factory import DataSetFactory
import pandas as pd
from pandas import DataFrame
import numpy as np
import os
import glob
from typing import List, Dict, Tuple

def split_dataframe(df: DataFrame, chunk_size: int) -> List[pd.DataFrame]:
    """
    Split a DataFrame into chunks based on a chunk size with respect to unique timestamps.

    The DataFrame is first sorted by the 'timestamp' column. It is then split into chunks where each chunk contains at least 'chunk_size' number of unique timestamps.

    Args:
        df (DataFrame): The DataFrame to be split. It must contain a 'timestamp' column.
        chunk_size (int): The minimum number of unique timestamps each chunk should have.

    Returns:
        List[DataFrame]: A list of DataFrame chunks, each with at least 'chunk_size' unique timestamps.

    Raises:
        ValueError: If the 'timestamp' column is not found in the DataFrame.

    Example:
        >>> df = pd.DataFrame({
        ...     'timestamp': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        ...     'data': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
        ... })
        >>> chunks = split_dataframe(df, 3)
        >>> len(chunks)
        4

    Note:
        - The function assumes that the 'timestamp' column exists in the DataFrame.
        - The resulting chunks will not contain the 'chunk_id' column used for splitting.
    """
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


def gen(task: Tuple[str, List[str]], chunk_size: int = 100_000):
    # NOTICE: Only support books now.
    """
    处理一组parquet文件并通过过滤和转换数据生成数据集。

    此函数接收一个包含输出路径和parquet文件路径列表的元组，以及一个可选的块大小参数。
    它通过按时间戳排序数据、移除不必要的列和转换数据类型来处理每个文件。
    然后使用处理后的记录更新数据集工厂。如果在任何文件中没有遇到'snapshot'操作，函数将抛出异常。

    Args:
    - task: 包含以下元素的元组：
        - out_path (str): 处理后数据集存储的路径。
        - files (List[str]): 要处理的parquet文件路径列表。
    - chunk_size (int, optional): DataSetFactory的块大小。默认为100,000。

    Raises:
    - Exception: 如果在任何文件中没有找到'snapshot'操作，将抛出异常，表明数据集不完整。

    Returns:
    - None: 此函数不返回任何值。结果是在指定输出路径生成处理后的数据集。
    """
    out_path, files = task
    dsId = Path(out_path).name # Remove tailing `\\` and `/`
    instId = dsId.removesuffix('-400')
    meet_snapshot = False
    
    dataset_fact = DataSetFactory(instId, Path(out_path), chunk_size, True)
    
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
                print(f"meet snapshot at {row['timestamp']} in file {parquet_file}")
            elif (not meet_snapshot) and row['action'] != 'snapshot': # Skip the records that are located before the `snapshot`.
                continue
            
            dataset_fact.update(row)
        
    dataset_fact.close()
    if not meet_snapshot:
        raise Exception('Can not find any snapshot in any files')


def get_spot():
    files = glob.glob(r'E:\temp\parquet\BTC-USDT-400\OKX-Books-BTC-USDT-400-*.parquet')
    files = sorted(files)
    assert files
    gen((r'E:\out3\books\BTC-USDT-400', files), chunk_size=500_000)
    

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
    gen(('E:\\out3\\books\\BTC-USDT-TWEEK', new_files), chunk_size=500_000)

if __name__ == '__main__':
    # get_futures_weekly()
    pass
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

