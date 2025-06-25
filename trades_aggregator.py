import argparse
import heapq
import logging
import logging.handlers
import multiprocessing as mp
import sys
import time
import traceback
import tempfile
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed, CancelledError
from functools import partial
from itertools import groupby, chain
from operator import itemgetter
from pathlib import Path
from typing import List, Dict, Any, Iterator

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from DataFile.data_name import DataName
from agg_utils import pattern3

# 全局变量
logger = logging.getLogger(__name__)
log_queue = mp.Queue()
stop_event = mp.Event()

def read_jf(jf: str):
    with open(jf, 'rb') as f:
        content = f.read()
    while True:
        try:
            try:
                root = orjson.loads(content)
                return root['data']
            except orjson.JSONDecodeError as e:
                if "unexpected character" in str(e) and "char" in str(e):
                    error_pos_str = str(e).split("char ")[-1].strip(")")
                    try:
                        error_pos = int(error_pos_str)
                        if chr(content[error_pos]) == ',':
                            fixed_content = content[:error_pos] + content[error_pos+1:]
                        else:
                            raise Exception(f'unknown error: {content[error_pos-10:error_pos+10]}')
                        content = fixed_content
                        continue
                    except (ValueError, orjson.JSONDecodeError) as fix_error:
                        logger.warning(f"无法修复文件 {jf} 中的JSON: {str(fix_error)}")
                        raise fix_error
                else:
                    raise
        except Exception as e:
            logger.warning(f"处理文件 {jf} 时出错: {str(e)}")
            raise

def resolve_repeated_ts_in_file(dps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    res = []
    sorted_dps = sorted(dps, key=itemgetter('ts'))
    for key, group in groupby(sorted_dps, key=itemgetter('ts')):
        group_list = list(group)
        merged = {
            'arg': group_list[0]['arg'],
            'data': list(map(lambda dp: dp['data'], group_list)),
            'ts': key
        }
        res.append(merged)
    return res

def process_and_save_temp_parquet(file_path: str, temp_dir: Path) -> str:
    """
    处理单个JSON文件并将其保存为临时的、已排序的Parquet文件。
    返回临时文件的路径。
    """
    if stop_event.is_set():
        return ""
    try:
        raw_data_points = read_jf(file_path)
        if not raw_data_points:
            return ""

        mapped_dps = pattern3.map(raw_data_points)
        if not mapped_dps:
            return ""

        processed_dps = resolve_repeated_ts_in_file(mapped_dps)
        if not processed_dps:
            return ""

        # 直接写入临时的Parquet文件
        table = pa.Table.from_pylist(processed_dps)
        temp_file_path = temp_dir / f"{Path(file_path).stem}.parquet"
        pq.write_table(table, temp_file_path, compression='ZSTD')
        return str(temp_file_path)

    except Exception as e:
        logger.error(f"处理文件 {file_path} 时发生严重错误: {e}\n{traceback.format_exc()}")
        stop_event.set()
        raise

def read_parquet_iter(file_path: str) -> Iterator[Dict[str, Any]]:
    """创建一个从Parquet文件读取数据的迭代器"""
    try:
        parquet_file = pq.ParquetFile(file_path)
        for batch in parquet_file.iter_batches():
            for row in batch.to_pylist():
                yield row
    except Exception as e:
        logger.error(f"读取临时文件 {file_path} 失败: {e}")
        # 返回一个空迭代器
        return

def _merge_chunk_to_temp_file(input_files: List[str], output_path: str, row_group_size: int):
    """将一个Parquet文件块合并到一个新的临时Parquet文件中。"""
    logger.debug(f"正在将 {len(input_files)} 个文件合并到 {output_path}")
    
    iterators = [read_parquet_iter(f) for f in input_files]
    if not iterators:
        return

    merged_iterator = heapq.merge(*iterators, key=itemgetter('ts'))
    
    final_data_iterator = (
        {
            'arg': (group_list := list(group))[0]['arg'],
            'data': list(chain.from_iterable(item['data'] for item in group_list)),
            'ts': key
        }
        for key, group in groupby(merged_iterator, key=itemgetter('ts'))
    )

    writer = None
    try:
        buffer = []
        for record in final_data_iterator:
            if stop_event.is_set():
                logger.warning(f"检测到停止信号，已中断向 {output_path} 的写入操作。")
                return

            buffer.append(record)
            if len(buffer) >= row_group_size:
                table = pa.Table.from_pylist(buffer)
                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema, compression='ZSTD', compression_level=2)
                writer.write_table(table)
                buffer = []

        if buffer:
            table = pa.Table.from_pylist(buffer)
            if writer is None:
                pq.write_table(table, output_path, row_group_size=row_group_size, compression='ZSTD', compression_level=2)
            else:
                writer.write_table(table)
    finally:
        if writer:
            writer.close()

def _final_merge(
    files_to_merge: List[str],
    output_dir: Path,
    file_prefix: str,
    chunk_size: int,
    row_group_size: int
):
    """对最后一批文件进行最终合并，并按chunk_size写入到多个输出文件中。"""
    logger.info(f"开始从 {len(files_to_merge)} 个文件中进行最终合并...")

    iterators = [read_parquet_iter(f) for f in files_to_merge]
    merged_iterator = heapq.merge(*iterators, key=itemgetter('ts'))

    final_data_iterator = (
        {
            'arg': (group_list := list(group))[0]['arg'],
            'data': list(chain.from_iterable(item['data'] for item in group_list)),
            'ts': key
        }
        for key, group in groupby(merged_iterator, key=itemgetter('ts'))
    )

    buffer = []
    generated_counter = 0
    total_rows = 0

    pbar = tqdm(desc="写入最终Parquet文件", unit=" rows")
    for record in final_data_iterator:
        if stop_event.is_set():
            logger.warning("检测到停止信号，终止写入过程。")
            return

        buffer.append(record)
        if len(buffer) >= chunk_size:
            _write_chunk_to_parquet(buffer, output_dir, file_prefix, row_group_size)
            total_rows += len(buffer)
            pbar.update(len(buffer))
            generated_counter += 1
            buffer = []

    if buffer and not stop_event.is_set():
        _write_chunk_to_parquet(buffer, output_dir, file_prefix, row_group_size)
        total_rows += len(buffer)
        pbar.update(len(buffer))
        generated_counter += 1
    
    pbar.close()
    logger.info(f"数据合并写入完成。共生成 {generated_counter} 个Parquet文件，总计 {total_rows} 行数据。")

def merge_parquet_files(
    temp_files: List[str],
    output_dir: Path,
    file_prefix: str,
    chunk_size: int,
    row_group_size: int,
    temp_dir: Path
):
    """
    高效地合并多个Parquet文件，采用分层方法避免一次性打开过多文件。
    """
    MAX_OPEN_FILES = 512

    files_to_merge = temp_files
    level = 0
    
    while len(files_to_merge) > MAX_OPEN_FILES:
        logger.info(f"合并层级 {level}: 有 {len(files_to_merge)} 个文件待合并。将以 {MAX_OPEN_FILES} 个文件为一批进行合并。")
        
        intermediate_dir = temp_dir / f"intermediate_{level}"
        intermediate_dir.mkdir(exist_ok=True)
        logger.info(f"已创建中间目录: {intermediate_dir}")
        
        next_level_files = []
        
        file_chunks = [files_to_merge[i:i + MAX_OPEN_FILES] for i in range(0, len(files_to_merge), MAX_OPEN_FILES)]
        
        for i, chunk in enumerate(tqdm(file_chunks, desc=f"正在合并层级 {level} 的文件块")):
            if stop_event.is_set():
                logger.warning("检测到停止信号，正在中止合并。")
                return

            intermediate_file_path = str(intermediate_dir / f"merged_chunk_{i}.parquet")
            try:
                _merge_chunk_to_temp_file(chunk, intermediate_file_path, row_group_size)
                if Path(intermediate_file_path).exists():
                    next_level_files.append(intermediate_file_path)
            except Exception as e:
                logger.error(f"合并文件块到 {intermediate_file_path} 失败: {e}\n{traceback.format_exc()}")
                stop_event.set()
                raise

        logger.info(f"正在清理合并层级 {level} 的 {len(files_to_merge)} 个文件。")
        for f_path in files_to_merge:
            try:
                p = Path(f_path)
                if p.is_file():
                    p.unlink()
                elif p.is_dir(): # Just in case
                    shutil.rmtree(p)
            except OSError as e:
                logger.warning(f"无法删除中间文件 {f_path}: {e}")

        files_to_merge = next_level_files
        level += 1
        # If a level produces no files, something went wrong.
        if not files_to_merge:
            logger.error("中间合并步骤未产生任何文件，聚合终止。")
            return

    logger.info(f"开始对 {len(files_to_merge)} 个文件进行最终合并。")
    _final_merge(files_to_merge, output_dir, file_prefix, chunk_size, row_group_size)


def _write_chunk_to_parquet(
    chunk: List[Dict[str, Any]],
    output_dir: Path,
    file_prefix: str,
    row_group_size: int
):
    if not chunk:
        return
    start_ts = chunk[0]['ts']
    end_ts = chunk[-1]['ts']
    
    output_path = None # Initialize output_path
    try:
        table = pa.Table.from_pylist(chunk)
        parquet_file_name = f'{file_prefix}-{start_ts}-{end_ts}.parquet'
        output_path = output_dir / parquet_file_name
        pq.write_table(
            table,
            output_path,
            row_group_size=row_group_size,
            compression='ZSTD',
            compression_level=2
        )
    except Exception as e:
        log_path_str = f" {output_path}" if output_path else ""
        logger.error(f"写入Parquet文件{log_path_str}时失败: {e}")
        stop_event.set()
        raise

# --- 日志配置 ---
def configure_main_logger():
    log_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    
    file_handler = logging.FileHandler('trades_aggregator.log', mode='w', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(logging.handlers.QueueHandler(log_queue))

def logger_listener_process(queue: mp.Queue):
    log_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler('trades_aggregator.log', mode='a', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    
    listener_logger = logging.getLogger('listener')
    listener_logger.addHandler(file_handler)
    listener_logger.setLevel(logging.DEBUG)

    while True:
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            print('日志监听器错误:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

def worker_logger_config(queue: mp.Queue):
    queue_handler = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(queue_handler)
    root.setLevel(logging.DEBUG)

def main():
    start_time = time.time()
    
    parser = argparse.ArgumentParser(
        description="使用多进程高效聚合Trades JSON数据到Parquet文件。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--dir', required=True, help="包含JSON文件的输入目录。")
    parser.add_argument('--output', required=True, help="用于存储Parquet文件的输出目录。")
    parser.add_argument('--chunk-size', type=int, default=10_000_000, help="每个Parquet文件的最大行数。")
    parser.add_argument('--row-group-size', type=int, default=1_000_000, help="Parquet文件中每个row_group的最大行数。")
    parser.add_argument('--max-workers', type=int, default=None, help="使用的最大进程数，默认为CPU核心数。")
    
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    configure_main_logger()
    listener = mp.Process(target=logger_listener_process, args=(log_queue,))
    listener.start()
    
    logger.info("开始聚合Trades数据...")
    
    json_files = sorted([str(f) for f in Path(args.dir).rglob('*.json') if 'Trades' in f.name])
    if not json_files:
        logger.warning(f"在目录 {args.dir} 中没有找到 'Trades' 相关的JSON文件。")
        log_queue.put(None)
        listener.join()
        return

    logger.info(f"找到 {len(json_files)} 个Trades JSON文件进行处理。")
    
    try:
        file_prefix = DataName(Path(json_files[0]).name).prefix
    except ValueError as e:
        logger.error(f"第一个文件名 {Path(json_files[0]).name} 无效: {e}")
        log_queue.put(None)
        listener.join()
        sys.exit(1)

    num_workers = args.max_workers or mp.cpu_count()
    logger.info(f"使用 {num_workers} 个工作进程。")

    temp_dir = Path(tempfile.mkdtemp(prefix="trades_agg_"))
    logger.info(f"创建临时目录: {temp_dir}")
    
    temp_parquet_files = []
    
    try:
        with ProcessPoolExecutor(
            max_workers=num_workers,
            initializer=worker_logger_config,
            initargs=(log_queue,)
        ) as executor:
            
            futures = {executor.submit(process_and_save_temp_parquet, jf, temp_dir): jf for jf in json_files}
            
            with tqdm(total=len(json_files), desc="处理JSON并生成临时文件") as pbar:
                for future in as_completed(futures):
                    if stop_event.is_set():
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        logger.error("检测到停止信号，正在取消所有任务...")
                        break

                    try:
                        result_path = future.result()
                        if result_path:
                            temp_parquet_files.append(result_path)
                    except CancelledError:
                        # This can happen if stop_event was set and tasks were cancelled
                        logger.warning(f"任务 {futures[future]} 已被取消。")
                    except Exception as e:
                        file_name = futures[future]
                        logger.critical(f"处理文件 {file_name} 的子进程失败: {e}. 正在终止所有操作。")
                        stop_event.set()
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
                    finally:
                        pbar.update(1)

        if stop_event.is_set():
            raise Exception("由于一个或多个子进程发生错误，聚合操作已终止。")

        if not temp_parquet_files:
            logger.warning("没有生成任何临时文件，程序终止。")
        else:
            merge_parquet_files(
                temp_parquet_files,
                output_dir,
                file_prefix,
                args.chunk_size,
                args.row_group_size,
                temp_dir
            )

    except Exception as e:
        logger.critical(f"主进程捕获到严重错误: {e}")
        logger.critical("程序将异常终止，不会生成任何（或部分）输出文件。")
        stop_event.set()
    finally:
        if temp_dir.exists():
            logger.info(f"清理临时目录: {temp_dir}")
            shutil.rmtree(temp_dir)
            
        log_queue.put(None)
        listener.join()
        
        end_time = time.time()
        logger.info(f"总耗时: {end_time - start_time:.2f} 秒。")
        if stop_event.is_set():
            logger.error("程序因错误而终止。")
            sys.exit(1)
        else:
            logger.info("程序成功完成。")


if __name__ == "__main__":
    if sys.platform.startswith('win'):
        mp.set_start_method('spawn', force=True)
    main()