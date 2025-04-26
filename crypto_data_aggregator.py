from pathlib import Path
from typing import List
import logging
import time

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import argparse

from DataFile.data_name import DataName
from tqdm import tqdm

logger = logging.getLogger(__name__)

def dp_ts(dp) -> int:
    return int(dp['data'][0]['ts'])

def aggregator(json_files: List[str], output_dir: str, overlapped_threshold: int, chunk: int): 
    json_files.sort()
    
    if not json_files:
        logger.warning('json_files is empty')
        return
    
    if not(overlapped_threshold > 0 and chunk > 0):
        raise Exception(f"overlapped_threshold({overlapped_threshold}) or chunk({chunk}) should be bigger than 0")
    
    if len(set(DataName(Path(jf).name).prefix for jf in json_files)) != 1:
        raise Exception("all json file should have the same prefix")
    file_prefix = DataName(Path(json_files[0]).name).prefix
    
    dp_buffer = [] # always sorted!
    generated_counter = 0
    
    # initialize `dp_buffer`
    start = -1
    for i, jf in enumerate(json_files):
        with open(jf, 'rb') as f:
            jf_root = orjson.loads(f.read())
        data_points = sorted(jf_root['data'], key=dp_ts)
        if not data_points:
            continue
        dp_buffer = data_points
        start = i + 1
        break
    
    # start to generate parquet files in chunk
    for jf in tqdm(json_files[start:], desc="Processing JSON files"):
        with open(jf, 'rb') as f:
            jf_root = orjson.loads(f.read())
        data_points = sorted(jf_root['data'], key=dp_ts)
        
        if dp_ts(dp_buffer[-1]) < dp_ts(data_points[-1]): # Not overlapped, extend directly
            dp_buffer.extend(data_points)
        elif dp_ts(data_points[-1]) < dp_ts(dp_buffer[-1]): # overlapped
            dp_buffer.extend(data_points)
            dp_buffer.sort(key=dp_ts)
            logger.info(f'overlapped: {jf}')
        else: # equal? this should not exist
            raise Exception(f"some datapoint {data_points[-1]} in file {jf} is equal to the datapoint in the last file")
        
        # generate a new parquet file if needed
        while len(dp_buffer) > overlapped_threshold + chunk:
            start_ts = dp_ts(dp_buffer[0])
            end_ts = dp_ts(dp_buffer[chunk-1])
            parquet_file = f'{file_prefix}-{start_ts}-{end_ts}.parquet'
            df = pd.DataFrame(dp_buffer[:chunk])
            table = pa.Table.from_pandas(df)
            pq.write_table(table, Path(output_dir)/Path(parquet_file), compression='ZSTD', compression_level=5)
            logger.info(f'generated: {Path(output_dir)/Path(parquet_file)}')
            generated_counter += 1
            
            dp_buffer = dp_buffer[chunk:]
    
    # generated remained datapoint as a new parquet file
    if dp_buffer:
        start_ts = dp_ts(dp_buffer[0])
        end_ts = dp_ts(dp_buffer[-1])
        parquet_file = f'{file_prefix}-{start_ts}-{end_ts}.parquet'
        df = pd.DataFrame(dp_buffer)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, Path(output_dir)/Path(parquet_file), compression='ZSTD', compression_level=5)
        logger.info(f'generated: {Path(output_dir)/Path(parquet_file)}')
        generated_counter += 1
    
    logger.info(f'all json files are processed, generated {generated_counter} parquet files')


def main():
    start_time = time.time()

    # 配置日志输出到文件
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename='aggregator.log', # 日志文件名
        filemode='w' # 写入模式，'w' 表示覆盖，'a' 表示追加
    )

    parser = argparse.ArgumentParser(
        description="Aggregate JSON files into parquet files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--files', nargs='+', help="List of specific JSON files to process.")
    group.add_argument('--dir', help="Directory containing JSON files to process recursively.")
    parser.add_argument('--output', help="Output path of the parquet files.")
    parser.add_argument('--overlapped', type=int, default=1000, help="The max overlapped gap.")
    parser.add_argument('--chunk', type=int, default=1_000_000, help="The number of row in each generated parquet file.")
    parser.add_argument('--prefix', help="Optional prefix for output parquet files when --dir is used.")

    args = parser.parse_args()
    
    # 处理输出目录
    if not args.output:
        raise ValueError("Output directory must be specified")
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 收集JSON文件
    json_files = []
    if args.files:
        json_files = [str(Path(f).absolute()) for f in args.files]
    elif args.dir:
        all_json_files = Path(args.dir).rglob('*.json')
        if args.prefix:
            json_files = [str(f.absolute()) for f in all_json_files if f.name.startswith(args.prefix)]
        else:
            json_files = [str(f.absolute()) for f in all_json_files]
    
    if not json_files:
        logger.warning("No JSON files found to process")
        return
    
    logger.info(f'Found {len(json_files)} json files')
    
    # 调用聚合函数
    aggregator(
        json_files=json_files,
        output_dir=str(output_dir),
        overlapped_threshold=args.overlapped,
        chunk=args.chunk
    )
    
    logger.info(f"Total processing time: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
