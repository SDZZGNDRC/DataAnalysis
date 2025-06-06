from pathlib import Path
from typing import List
import logging
import logging.handlers
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import traceback
import sys
import pyarrow as pa
import pyarrow.parquet as pq
import argparse

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from tqdm import tqdm

from DataFile.data_name import DataName

logger = logging.getLogger(__name__)

# 全局日志队列，避免重复创建
log_queue = mp.Queue()

def rechunk_parquet(parquet_files: List[str], output_dir: str, chunk: int):
    process_id = mp.current_process().name
    try:
        if not parquet_files:
            return
        if chunk <= 0:
            logger.error(f'[{process_id}] chunk({chunk}) should be bigger than 0')
            raise Exception(f'chunk({chunk}) should be bigger than 0')
        
        parquet_files.sort()

        dataNames = [DataName(Path(pf).name) for pf in parquet_files]
        if len(set(dn.prefix for dn in dataNames)) != 1:
            logger.error(f'[{process_id}] prefix not same')
            raise Exception('prefix not same')
        

        current_table = pq.read_table(parquet_files[0])
        schema = current_table.schema
        prefix = dataNames[0].prefix

        start = 0
        for pf in tqdm(parquet_files[1:], desc=f"[{process_id}] Processing {prefix}"):
            next_table = pq.read_table(pf)
            if next_table.schema != schema:
                logger.error(f'[{process_id}] schema not equal: {parquet_files[0]} and {pf}')
                raise Exception(f'schema not equal: {parquet_files[0]} and {pf}')
            
            # concat
            current_max_ts = pc.max(current_table['ts']).as_py()
            next_min_ts = pc.min(next_table['ts']).as_py()
            if not (current_max_ts < next_min_ts):
                current_table = pa.concat_tables([current_table, next_table]).sort_by('ts')
            else:
                current_table = pa.concat_tables([current_table, next_table])

            remained_rows = current_table.num_rows

            while remained_rows > chunk:
                start_ts = current_table['ts'][start].as_py()
                end_ts = current_table['ts'][start + chunk-1].as_py()
                output_file = Path(output_dir) / Path(f'{prefix}-{start_ts}-{end_ts}.parquet')
                pq.write_table(current_table.slice(start,chunk), output_file, compression='ZSTD', compression_level=3)
                start += chunk
                remained_rows -= chunk
            
            current_table = current_table.slice(start,remained_rows)
            start = 0
        
        # remained rows
        start_ts = current_table['ts'][start].as_py()
        end_ts = current_table['ts'][-1].as_py()
        output_file = Path(output_dir) / Path(f'{prefix}-{start_ts}-{end_ts}.parquet')
        pq.write_table(current_table, output_file, compression='ZSTD', compression_level=3)
    except Exception as e:
        logger.error(f"[{process_id}] str(e)")
    return len(parquet_files)

def configure_main_logger():
    """配置主进程的日志记录器"""
    log_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s')
    
    # 文件处理器
    file_handler = logging.FileHandler('rechunk.log', mode='w', encoding='utf-8')  # 使用追加模式
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
        description="Rechunk parquet files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--files', nargs='+', help="List of specific parqeut files to process.")
    group.add_argument('--dir', help="Directory containing parquet files to process recursively.")
    parser.add_argument('--output', help="Output path of the parquet files.")
    parser.add_argument('--chunk', type=int, default=1_000_000, help="The number of row in each generated parquet file.")
    parser.add_argument('--prefix', help="Optional prefix for the input parquet files when --dir is used.")

    args = parser.parse_args()
    
    if not args.output:
        raise ValueError("Output directory must be specified")
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    parquet_files = []
    if args.files:
        parquet_files = [str(Path(f).absolute()) for f in args.files]
    elif args.dir:
        all_parquet_files = Path(args.dir).rglob('*.parquet')
        if args.prefix:
            parquet_files = [str(f.absolute()) for f in all_parquet_files if f.name.startswith(args.prefix)]
        else:
            parquet_files = [str(f.absolute()) for f in all_parquet_files]
    
    if not parquet_files:
        logger.warning("No parquet files found to process")
        return
    
    logger.info(f'Found {len(parquet_files)} parquet files')

    # 根据`prefix`分类    
    tasks = {}
    for pf in parquet_files:
        dn = DataName(Path(pf).name)
        if dn.prefix not in tasks:
            tasks[dn.prefix] = []
        tasks[dn.prefix].append(pf)

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
                    rechunk_parquet,
                    parquet_files=files,
                    output_dir=str(output_dir),
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
    logger.info(f"Total parquet files processed: {total_files_processed}")

if __name__ == "__main__":
    main()