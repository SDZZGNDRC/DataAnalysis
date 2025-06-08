from pathlib import Path
from typing import List
import logging
import logging.handlers
import time
import heapq
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import traceback
import sys

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import argparse

from DataFile.data_name import DataName
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from agg_utils import Books
import agg_utils

logger = logging.getLogger(__name__)

# 全局日志队列，避免重复创建
log_queue = mp.Queue()

def read_jf(jf: str):
    with open(jf, 'rb') as f:
        content = f.read()
    MAX_RETRY = 10000
    for _ in range(MAX_RETRY):
        try:
            try:
                root = orjson.loads(content)
                return root['data']
            except orjson.JSONDecodeError as e:
                # 尝试修复JSON文件 - 移除错误位置附近的字符
                if "unexpected character" in str(e) and "char" in str(e):
                    # 从错误信息中提取字符位置
                    error_pos_str = str(e).split("char ")[-1].strip(")")
                    try:
                        error_pos = int(error_pos_str)
                        if chr(content[error_pos]) == ',':
                            fixed_content = content[:error_pos] + content[error_pos+1:]
                        # elif chr(content[error_pos]) == '{':
                        #     fixed_content = content[:error_pos-1] + content[error_pos:]
                        else:
                            raise Exception(f'unknown error: {content[error_pos-10:error_pos+10]}')
                        # logger.warning(f"尝试修复文件 {jf} 中的JSON，移除了位置 {safe_start}-{safe_end} 的内容：{content[safe_start:safe_end]}")
                        content = fixed_content
                        continue # 继续循环，以处理其他错误
                    except (ValueError, orjson.JSONDecodeError) as fix_error:
                        print(f"无法修复文件 {jf} 中的JSON: {str(fix_error)}")
                        print(f'跳过文件 {jf}')
                        raise fix_error
                else:
                    raise
        except Exception as e:
            print(f"处理文件 {jf} 时出错: {str(e)}")
            raise
    raise Exception(f'修复重试次数超过最大值{MAX_RETRY}')

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
    
    map_func = agg_utils.category_map[DataName(Path(json_files[0]).name).category]
    process_id = mp.current_process().name

    start = -1
    for i, jf in enumerate(json_files):
        try:
            data_points = map_func(read_jf(jf))
            if not data_points:
                continue
            dp_buffer = data_points
            start = i + 1
            break
        except Exception as e:
            logger.error(f"[{process_id}] 处理文件 {jf} 时出错: {str(e)}")
            continue
    
    for jf in tqdm(json_files[start:], desc=f"[{process_id}] Processing {file_prefix}"):
        try:
            try:
                data_points = map_func(read_jf(jf))
            except Exception as e:
                logger.error(f"[{process_id}] 处理文件 {jf} 时出错: {str(e)}")
                continue
            if not data_points:
                logger.warning(f"[{process_id}] 文件 {jf} 没有数据点")
                continue
                
            if dp_buffer[-1]['ts'] < data_points[-1]['ts']: # Not overlapped, extend directly
                dp_buffer.extend(data_points)
            elif data_points[-1]['ts'] < dp_buffer[-1]['ts']: # overlapped
                # Use heapq.merge for efficient merging of sorted lists
                dp_buffer = list(heapq.merge(dp_buffer, data_points, key=lambda dp: dp['ts']))
                logger.info(f'[{process_id}] overlapped: {jf}')
            else: # equal? this should not exist
                logger.warning(f"[{process_id}] 文件 {jf} 中的数据点 {data_points[-1]} 与上一个文件中的数据点时间戳相同，跳过")
                continue
        except Exception as e:
            logger.error(f"[{process_id}] 处理文件 {jf} 时发生未预期错误: {str(e)}")
            continue
        
        # generate a new parquet file if needed
        while len(dp_buffer) > overlapped_threshold + chunk:
            start_ts = dp_buffer[0]['ts']
            end_ts = dp_buffer[chunk-1]['ts']
            parquet_file = f'{file_prefix}-{start_ts}-{end_ts}.parquet'
            table = pa.Table.from_pylist(dp_buffer[:chunk])
            output_path = Path(output_dir)/Path(parquet_file)
            pq.write_table(table, output_path, compression='ZSTD', compression_level=2)
            # logger.info(f'[{process_id}] generated: {output_path}')
            generated_counter += 1
            dp_buffer = dp_buffer[chunk:]
    
    # generated remained datapoint as a new parquet file
    if dp_buffer:
        start_ts = dp_buffer[0]['ts']
        end_ts = dp_buffer[-1]['ts']
        parquet_file = f'{file_prefix}-{start_ts}-{end_ts}.parquet'
        table = pa.Table.from_pylist(dp_buffer)
        output_path = Path(output_dir)/Path(parquet_file)
        pq.write_table(table, output_path, compression='ZSTD', compression_level=2)
        # logger.info(f'[{process_id}] generated: {output_path}')
        generated_counter += 1
    
    # logger.info(f'[{process_id}] {len(json_files)} json files with prefix <{file_prefix}> processed; gen \t{generated_counter} parquet')
    return generated_counter

def configure_main_logger():
    """配置主进程的日志记录器"""
    log_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s')
    
    # 文件处理器
    file_handler = logging.FileHandler('aggregator.log', mode='w', encoding='utf-8')  # 使用追加模式
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)
    
    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(logging.handlers.QueueHandler(log_queue))  # 主进程也使用队列

def logger_listener_process(queue):
    """日志监听进程，处理队列中的日志记录"""
    # 配置文件处理器
    log_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler('aggregator.log', mode='w', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)
    
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    root_logger.setLevel(logging.DEBUG)
    
    while True:
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            print('Error in logger listener:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
    file_handler.close()

def worker_logger_config(queue):
    """配置子进程的日志处理器"""
    queue_handler = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    root.handlers.clear()  # 清除子进程继承的处理器
    root.addHandler(queue_handler)
    root.setLevel(logging.DEBUG)

def main():
    start_time = time.time()
    
    # 配置主进程日志
    configure_main_logger()
    
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
    
    if not args.output:
        raise ValueError("Output directory must be specified")
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
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

    # 根据`prefix`分类    
    tasks = {}
    for jf in json_files:
        dn = DataName(Path(jf).name)
        if dn.prefix not in tasks:
            tasks[dn.prefix] = []
        tasks[dn.prefix].append(jf)

    # 启动日志监听进程
    listener = mp.Process(target=logger_listener_process, args=(log_queue,))
    listener.start()
    
    num_processes = min(mp.cpu_count(), len(tasks))
    
    # 为每个前缀创建一个任务
    with ProcessPoolExecutor(max_workers=num_processes,
                             initializer=worker_logger_config,
                             initargs=(log_queue,)) as executor:
        futures = []
        for prefix, files in tasks.items():
            futures.append(
                executor.submit(
                    aggregator,
                    json_files=files,
                    output_dir=str(output_dir),
                    overlapped_threshold=args.overlapped,
                    chunk=args.chunk,
                )
            )
        
        completed_tasks = 0
        failed_tasks = 0
        for future in futures:
            try:
                result = future.result()
                if result:
                    completed_tasks += 1
            except Exception as e:
                failed_tasks += 1
                logger.error(f"任务执行失败: {str(e)}")
                import traceback
                logger.error(f"错误详情: {traceback.format_exc()}")
        
        logger.info(f"成功完成 {completed_tasks} 个任务，失败 {failed_tasks} 个任务")
    
    # 停止日志监听进程
    log_queue.put(None)
    listener.join()
    
    # 汇总所有进程的结果
    total_files_processed = sum(len(files) for files in tasks.values())
    logger.info(f"Total processing time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Total JSON files processed: {total_files_processed}")

if __name__ == "__main__":
    main()