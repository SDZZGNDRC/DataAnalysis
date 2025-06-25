from bisect import bisect_left
from datetime import datetime
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
import os
from itertools import groupby, chain
from operator import itemgetter

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import argparse
from tqdm import tqdm

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
                    raise Exception(f'read_jf: {e}')
        except Exception as e:
            print(f"read_jf: 处理文件 {jf} 时出错: {str(e)}")
            raise Exception(f'read_jf: {e}')
    raise Exception(f'修复重试次数超过最大值{MAX_RETRY}')

def get_elapsedTime(jf: str):
    try:
        dps = read_jf(jf)
        dps = filter(lambda dp: 'data' in dp and dp['data'], dps)
        dps = list(map(lambda dp: dp['data'][0]['ts'], dps))
        dps.sort()
        return [dps[0], dps[-1]]
    except Exception as e:
        raise Exception(f'get_elapsedTime {jf}: {e}')


def _rename_worker(original_path: Path) -> Path:
    """
    (内部使用) 单个文件的重命名工作函数。
    这个函数会被多进程池中的每个进程调用。
    """
    if not original_path.exists():
        print(f"警告: 文件不存在，跳过: {original_path}")
        return original_path

    try:
        # 1. 调用 get_elapsedTime 获取新的时间戳
        #    注意：get_elapsedTime 需要字符串类型的路径
        new_start, new_end = get_elapsedTime(str(original_path))
        
        # 2. 使用 DataName 类解析旧的文件名以获取前缀
        #    DataName 会自动验证文件名格式
        data_name_obj = DataName(original_path.name)
        
        # 3. 构建新的文件名
        #    格式: {prefix}-{new_start_timestamp}-{new_end_timestamp}.{extension}
        #    例如: "OKX-Books-1INCH-USD-SWAP-400" + "-" + "1689298329268" + "-" + "1689300999939" + ".json"
        extension = original_path.suffix  # 获取文件扩展名，例如 ".json"
        new_filename = f"{data_name_obj.prefix}-{new_start}-{new_end}{extension}"
        
        # 4. 构建新的完整路径
        new_path = original_path.with_name(new_filename)
        
        # 5. 在文件系统上执行重命名操作
        original_path.rename(new_path)
        
        # 6. 返回新的 Path 对象
        return new_path
        
    except Exception as e:
        print(f"处理文件 {original_path.name} 时发生错误: {e}")
        # 如果发生错误，返回原始路径，表示未作更改
        return original_path


def rename_json_files_concurrently(jfs: List[Path]) -> List[Path]:
    """
    使用多进程并发地重命名一组JSON文件。

    对于列表中的每个文件路径，此函数会：
    1. 调用 `get_elapsedTime` 来获取新的开始和结束时间戳。
    2. 根据返回的时间戳更新文件名。
    3. 在文件系统上执行重命名。
    4. 返回所有被成功重命名后的新文件路径列表。

    Args:
        jfs: 一个包含 pathlib.Path 对象的列表，每个对象指向一个JSON文件。

    Returns:
        一个列表，包含所有重命名后的新 pathlib.Path 对象。
    """
    if not jfs:
        print("输入的文件列表为空，无需操作。")
        return []
        
    # 使用 with 语句可以确保进程池在使用后被正确关闭
    # os.cpu_count() 可以用来决定启动多少个进程，也可以留空让其自动决定
    with mp.Pool(processes=os.cpu_count()) as pool:
        # 使用 imap 替代 map 以便与 tqdm 集成
        print(f"启动 {pool._processes} 个进程来处理 {len(jfs)} 个文件...")
        new_paths = list(tqdm(pool.imap(_rename_worker, jfs),
                           total=len(jfs),
                           desc="重命名进度"))
        
    return sorted(new_paths)

def resolve_repeated_ts(dps):
    """used for trades data.
    """
    res = []
    for key, group in groupby(dps, key=itemgetter('ts')):
        group_list = list(group)
        merged = {
            'arg': group_list[0]['arg'],
            'data': list(map(lambda dp: dp['data'], group_list)),
            'ts': key
        }
        res.append(merged)
    return res

def trades_aggregator(json_files: List[str], output_dir: str, overlapped_threshold: int, chunk: int):
    # json_files = rename_json_files_concurrently(list(map(lambda jf: Path(jf), json_files)))
    
    
    if not json_files:
        logger.warning('json_files is empty')
        return
    
    if not(overlapped_threshold > 0 and chunk > 0):
        raise Exception(f"overlapped_threshold({overlapped_threshold}) or chunk({chunk}) should be bigger than 0")
    
    if len(set(DataName(Path(jf).name).prefix for jf in json_files)) != 1:
        raise Exception("all json file should have the same prefix")
    file_prefix = DataName(Path(json_files[0]).name).prefix
    
    dp_buffer = [] # always sorted!
    gen_file_ts = []
    generated_counter = 0
    
    map_func = agg_utils.category_map[DataName(Path(json_files[0]).name).category]
    process_id = mp.current_process().name

    start = -1
    for i, jf in enumerate(json_files):
        logger.debug(f'[{process_id}] --> {jf}')
        try:
            data_points = resolve_repeated_ts(map_func(read_jf(jf)))
            if not data_points:
                continue
            dp_buffer = data_points
            start = i + 1
            break
        except Exception as e:
            logger.error(f"[{process_id}] 处理文件 {jf} 时出错: {str(e)}")
            continue
    
    for jf in tqdm(json_files[start:], desc=f"[{process_id}] Processing {file_prefix}"):
        logger.debug(f'[{process_id}] --> {jf}')
        try:
            try:
                data_points = resolve_repeated_ts(map_func(read_jf(jf)))
            except Exception as e:
                logger.error(f"[{process_id}] 处理文件 {jf} 时出错: {str(e)}")
                continue
            if not data_points:
                logger.warning(f"[{process_id}] 文件 {jf} 没有数据点")
                continue
                
            if dp_buffer[-1]['ts'] < data_points[0]['ts']: # Not overlapped, extend directly
                dp_buffer.extend(data_points)
            elif data_points[0]['ts'] <= dp_buffer[-1]['ts']: # overlapped
                if data_points[-1]['ts'] <= dp_buffer[-1]['ts']: # may meet large time range json file
                    ts_index = list(map(lambda dp: dp['ts'], dp_buffer))
                    target_idx = bisect_left(ts_index, data_points[-1]['ts'])
                    rmv_start = datetime.fromtimestamp(ts_index[target_idx]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    rmv_end = datetime.fromtimestamp(ts_index[-1]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    logger.warning(f"Critical Warning: Remove {len(dp_buffer) - target_idx} useless datapoint, from {rmv_start} to {rmv_end}")
                    print(f"Critical Warning: Remove {len(dp_buffer) - target_idx} useless datapoint, from {rmv_start} to {rmv_end}")
                    dp_buffer = dp_buffer[:target_idx]
                start = 0
                while data_points[start]['ts'] <= dp_buffer[-1]['ts']:
                    for i in range(len(dp_buffer)-1, -1, -1):
                        if dp_buffer[i]['ts'] == data_points[start]['ts']:
                            dp_buffer[i]['data'].extend(data_points[start]['data'])
                            break
                        elif dp_buffer[i]['ts'] < data_points[start]['ts']:
                            dp_buffer.insert(i+1,data_points[start])
                            break
                    start += 1
                    if start >= len(data_points):
                        logger.warning(f'There is a file has long time range!!!')
                        logger.warning(f'data_point -> {data_points[0]['ts']} : {data_points[start-1]['ts']}')
                        logger.warning(f'dp_buffer -> {dp_buffer[0]['ts']} : {dp_buffer[-1]['ts']}')
                        break
                dp_buffer.extend(data_points[start:])
                logger.info(f'[{process_id}] overlapped: {jf}')
            else:
                logger.warning(f"[{process_id}] 文件 {jf} 出错，请检查！")
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
            gen_file_ts.extend([start_ts, end_ts])
    
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
        gen_file_ts.extend([start_ts, end_ts])
    
    if not all(a <= b for a, b in zip(gen_file_ts, gen_file_ts[1:])):
        logger.error(f'generated parquet files are not sorted!!!!')
        print(f'Error: generated parquet files are not sorted!!!!')
        print(f'{gen_file_ts}')
        
    
    # logger.info(f'[{process_id}] {len(json_files)} json files with prefix <{file_prefix}> processed; gen \t{generated_counter} parquet')
    return generated_counter

def aggregator(json_files: List[str], output_dir: str, overlapped_threshold: int, chunk: int):
    json_files.sort()
    
    if not json_files:
        logger.warning('json_files is empty')
        return
    
    if DataName(Path(json_files[0]).name).category == 'Trades':
        return trades_aggregator(json_files, output_dir, overlapped_threshold, chunk)
    
    if not(overlapped_threshold > 0 and chunk > 0):
        raise Exception(f"overlapped_threshold({overlapped_threshold}) or chunk({chunk}) should be bigger than 0")
    
    if len(set(DataName(Path(jf).name).prefix for jf in json_files)) != 1:
        raise Exception("all json file should have the same prefix")
    file_prefix = DataName(Path(json_files[0]).name).prefix
    
    dp_buffer = [] # always sorted!
    gen_file_ts = []
    generated_counter = 0
    
    map_func = agg_utils.category_map[DataName(Path(json_files[0]).name).category]
    process_id = mp.current_process().name

    start = -1
    for i, jf in enumerate(json_files):
        try:
            data_points = map_func(read_jf(jf))
            if not data_points:
                logger.warning(f'aggregator-316: {jf} is empty...')
                continue
            dp_buffer = data_points
            start = i + 1
            break
        except Exception as e:
            print(f"[{process_id}] aggregator-320: 处理文件 {jf} 时出错: {str(e)}")
            continue
    if start == -1:
        logger.error(f'[{process_id}] aggregator-326: start = {start}')
        print(f'[{process_id}] Error : aggregator-326: start = {start}')
        # exit(-1)
    for jf in tqdm(json_files[start:], desc=f"[{process_id}] Processing {file_prefix}"):
        try:
            try:
                data_points = map_func(read_jf(jf))
            except Exception as e:
                logger.error(f"[{process_id}] aggregator-328: 处理文件 {jf} 时出错: {str(e)}")
                continue
            if not data_points:
                logger.warning(f"[{process_id}] 文件 {jf} 没有数据点")
                continue
                
            if dp_buffer[-1]['ts'] < data_points[0]['ts']: # Not overlapped, extend directly
                dp_buffer.extend(data_points)
            elif data_points[0]['ts'] < dp_buffer[-1]['ts']: # overlapped
                if data_points[-1]['ts'] <= dp_buffer[-1]['ts']: # may meet large time range json file
                    ts_index = list(map(lambda dp: dp['ts'], dp_buffer))
                    target_idx = bisect_left(ts_index, data_points[-1]['ts'])
                    rmv_start = datetime.fromtimestamp(ts_index[target_idx]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    rmv_end = datetime.fromtimestamp(ts_index[-1]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    logger.warning(f"Critical Warning: Remove {len(dp_buffer) - target_idx} useless datapoint, from {rmv_start} to {rmv_end}")
                    print(f"Critical Warning: Remove {len(dp_buffer) - target_idx} useless datapoint, from {rmv_start} to {rmv_end}")
                    dp_buffer = dp_buffer[:target_idx]
                    dp_buffer.extend(data_points)
                    continue
                else:
                    # Use heapq.merge for efficient merging of sorted lists
                    dp_buffer = list(heapq.merge(dp_buffer, data_points, key=lambda dp: dp['ts']))
                logger.info(f'[{process_id}] overlapped: {jf}')
            else: # equal? this should not exist
                logger.warning(f"[{process_id}] 文件 {jf} 中的数据点 {data_points[0]} 与上一个文件中的数据点时间戳相同，跳过")
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
            gen_file_ts.extend([start_ts, end_ts])
    
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
        gen_file_ts.extend([start_ts, end_ts])
    
    if not all(a <= b for a, b in zip(gen_file_ts, gen_file_ts[1:])):
        logger.error(f'generated parquet files are not sorted!!!!')
        print(f'Error: generated parquet files are not sorted!!!!')
        print(f'{gen_file_ts}')
    
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
    
    # Check if directory is not empty
    if any(output_dir.iterdir()):
        print(f"Error: Output directory '{output_dir}' is not empty")
        sys.exit(1)
        
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