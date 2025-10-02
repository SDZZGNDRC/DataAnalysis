#!/usr/bin/env python3
"""
OKX数据转换器 - 将OKX原始市场数据转换为HftBacktest格式

支持以下功能：
- 处理Books（深度数据）和Trades（交易数据）
- 支持7z压缩文件自动解压
- 支持通配符文件匹配
- 支持目录递归读取所有文件
- 自动合并多个文件并按时间排序
- 处理snapshot和update的深度数据
- 自动生成或修正local timestamp
"""

import argparse
import glob
import os
import re
import shutil
import sys
import tempfile
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import List, Optional, Tuple

try:
    import orjson
    def json_loads(data):
        return orjson.loads(data)
except ImportError:
    import json
    def json_loads(data):
        return json.loads(data)
    print("警告: 未安装orjson，使用标准json库。建议运行: pip install orjson")

import numpy as np
from numba import njit
from numpy.typing import NDArray

from hftbacktest.types import (
    BUY_EVENT,
    SELL_EVENT,
    DEPTH_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    TRADE_EVENT,
    event_dtype,
    EVENT_ARRAY
)
from hftbacktest.data.validation import correct_event_order, validate_event_order
from hftbacktest.data.validation import correct_local_timestamp


@njit
def generate_monotonic_local_timestamp(
    data: EVENT_ARRAY,
    simulated_latency: float,
    min_timestamp_increment: int = 1
) -> None:
    """
    在原地生成单调递增的local_ts。

    用于缺少本地时间戳但按本地接收时间排序的数据。

    Args:
        data: 事件数据数组，必须按本地接收时间排序。
        simulated_latency: 要添加到交易所时间戳的基础延迟。
                          这有助于确保 local_ts >= exch_ts。单位必须与时间戳匹配。
        min_timestamp_increment: 两个连续本地时间戳之间的最小增量，
                                以确保严格单调性。对于纳秒，1就可以。
    """
    last_local_ts = 0
    for i in range(len(data)):
        exch_ts = data[i].exch_ts

        # 合理的 local_ts 是交易所时间戳加上一些延迟
        candidate_local_ts = exch_ts + simulated_latency

        # 新的 local_ts 必须：
        # 1. 至少是 candidate_local_ts
        # 2. 严格大于前一个事件的 local_ts
        new_local_ts = max(candidate_local_ts, last_local_ts + min_timestamp_increment)

        data[i].local_ts = new_local_ts
        last_local_ts = new_local_ts


def parse_filename(filename: str) -> Tuple[str, str, str, str, int, int]:
    """
    解析OKX数据文件名，提取元信息。

    文件名格式: OKX-{Type}-{Base}-{Quote}-{Depth}-{StartTime}-{EndTime}.7z

    Args:
        filename: OKX数据文件名

    Returns:
        Tuple of (type, base, quote, depth, start_time, end_time)
    """
    basename = os.path.basename(filename)
    # 移除扩展名
    if basename.endswith('.7z'):
        basename = basename[:-3]
    elif basename.endswith('.json'):
        basename = basename[:-5]

    # 解析文件名: OKX-Books-BTC-USDT-400-1741219337154-1741219833931 或 OKX-Trades-BTC-USDT-1741219337154-1741219833931
    pattern = r'OKX-(Books|Trades)-([A-Z0-9]+)-([A-Z0-9]+)(?:-(\d+))?-(\d+)-(\d+)'
    match = re.match(pattern, basename)

    if not match:
        raise ValueError(f"Invalid OKX filename format: {filename}")

    data_type = match.group(1)
    base = match.group(2)
    quote = match.group(3)
    depth = match.group(4) if match.group(4) else "0"  # 对于Trades文件，depth为可选，默认为"0"
    start_time = int(match.group(5))
    end_time = int(match.group(6))

    return data_type, base, quote, depth, start_time, end_time


def extract_7z(filename: str, temp_dir: str) -> str:
    """
    解压7z文件到临时目录。

    Args:
        filename: 7z文件路径
        temp_dir: 临时目录路径

    Returns:
        解压后的JSON文件路径
    """
    try:
        import py7zr
    except ImportError:
        raise ImportError(
            "需要安装 py7zr 来处理7z文件。请运行: pip install py7zr"
        )

    with py7zr.SevenZipFile(filename, mode='r') as archive:
        archive.extractall(path=temp_dir)

    # 查找解压后的JSON文件
    json_files = list(Path(temp_dir).glob('*.json'))
    if not json_files:
        raise FileNotFoundError(f"在 {filename} 中未找到JSON文件")

    if len(json_files) > 1:
        raise ValueError(f"在 {filename} 中找到多个JSON文件")

    return str(json_files[0])


def process_books_file(
    json_file: str,
    feed_latency: float,
    found_snapshot: bool
) -> Tuple[np.ndarray, bool]:
    """
    处理Books（深度数据）文件。

    Args:
        json_file: JSON文件路径
        feed_latency: 喂送延迟（纳秒）
        found_snapshot: 是否已找到snapshot

    Returns:
        Tuple of (events_array, found_snapshot)
    """
    with open(json_file, 'rb') as f:
        data = json_loads(f.read())

    # 预估事件数量
    estimated_events = 0
    for item in data['data']:
        book_data = item['data'][0]
        estimated_events += len(book_data['bids']) + len(book_data['asks'])
        if item['action'] == 'snapshot':
            estimated_events += 2  # 为clear事件预留空间
    
    # 创建临时缓冲区
    buffer = np.empty(estimated_events, event_dtype)
    row_num = 0

    for item in data['data']:
        action = item['action']

        # 在找到第一个snapshot之前，跳过所有update
        if not found_snapshot and action == 'update':
            continue

        if action == 'snapshot':
            found_snapshot = True

        book_data = item['data'][0]  # data数组长度总是1
        ts_ms = int(book_data['ts'])
        exch_ts = ts_ms * 1_000_000  # 转换为纳秒

        # 检查是否有localTs
        if 'localTs' in item:
            local_ts = int(item['localTs']) * 1_000_000  # 转换为纳秒
        else:
            local_ts = exch_ts + int(feed_latency)

        # 处理bids
        if action == 'snapshot':
            # 对于snapshot，先插入DEPTH_CLEAR_EVENT
            if len(book_data['bids']) > 0:
                # 使用最低的bid价格作为clear价格
                bid_clear_px = float(book_data['bids'][-1][0])
                buffer[row_num] = (
                    DEPTH_CLEAR_EVENT | BUY_EVENT,
                    exch_ts,
                    local_ts,
                    bid_clear_px,
                    0,
                    0,
                    0,
                    0
                )
                row_num += 1

            # 插入snapshot数据
            for price, qty, _, _ in book_data['bids']:
                buffer[row_num] = (
                    DEPTH_SNAPSHOT_EVENT | BUY_EVENT,
                    exch_ts,
                    local_ts,
                    float(price),
                    float(qty),
                    0,
                    0,
                    0
                )
                row_num += 1
        else:  # update
            for price, qty, _, _ in book_data['bids']:
                buffer[row_num] = (
                    DEPTH_EVENT | BUY_EVENT,
                    exch_ts,
                    local_ts,
                    float(price),
                    float(qty),
                    0,
                    0,
                    0
                )
                row_num += 1

        # 处理asks
        if action == 'snapshot':
            # 对于snapshot，先插入DEPTH_CLEAR_EVENT
            if len(book_data['asks']) > 0:
                # 使用最高的ask价格作为clear价格
                ask_clear_px = float(book_data['asks'][-1][0])
                buffer[row_num] = (
                    DEPTH_CLEAR_EVENT | SELL_EVENT,
                    exch_ts,
                    local_ts,
                    ask_clear_px,
                    0,
                    0,
                    0,
                    0
                )
                row_num += 1

            # 插入snapshot数据
            for price, qty, _, _ in book_data['asks']:
                buffer[row_num] = (
                    DEPTH_SNAPSHOT_EVENT | SELL_EVENT,
                    exch_ts,
                    local_ts,
                    float(price),
                    float(qty),
                    0,
                    0,
                    0
                )
                row_num += 1
        else:  # update
            for price, qty, _, _ in book_data['asks']:
                buffer[row_num] = (
                    DEPTH_EVENT | SELL_EVENT,
                    exch_ts,
                    local_ts,
                    float(price),
                    float(qty),
                    0,
                    0,
                    0
                )
                row_num += 1

    return buffer[:row_num], found_snapshot


def process_trades_file(
    json_file: str,
    feed_latency: float
) -> np.ndarray:
    """
    处理Trades（交易数据）文件。

    Args:
        json_file: JSON文件路径
        feed_latency: 喂送延迟（纳秒）

    Returns:
        events_array
    """
    with open(json_file, 'rb') as f:
        data = json_loads(f.read())

    # 创建缓冲区
    buffer = np.empty(len(data['data']), event_dtype)
    row_num = 0

    for item in data['data']:
        trade_data = item['data'][0]  # data数组长度总是1

        ts_ms = int(trade_data['ts'])
        exch_ts = ts_ms * 1_000_000  # 转换为纳秒

        # 检查是否有localTs
        if 'localTs' in item:
            local_ts = int(item['localTs']) * 1_000_000  # 转换为纳秒
        else:
            local_ts = exch_ts + int(feed_latency)

        price = float(trade_data['px'])
        qty = float(trade_data['sz'])
        side = trade_data['side']

        # 交易发起方的side
        buffer[row_num] = (
            TRADE_EVENT | (BUY_EVENT if side == 'buy' else SELL_EVENT),
            exch_ts,
            local_ts,
            price,
            qty,
            0,
            0,
            0
        )
        row_num += 1

    return buffer[:row_num]


def collect_files(patterns: List[str]) -> List[str]:
    """
    收集文件，支持通配符和目录递归。

    Args:
        patterns: 文件路径模式列表，支持通配符或目录

    Returns:
        排序后的文件路径列表
    """
    files = []
    for pattern in patterns:
        if os.path.isdir(pattern):
            # 递归收集 .7z 和 .json 文件
            path = Path(pattern)
            files.extend(str(p) for p in path.rglob('*.7z'))
            files.extend(str(p) for p in path.rglob('*.json'))
        else:
            files.extend(glob.glob(pattern))
    return sorted(list(set(files)))  # 去重并排序


def scan_snapshot_worker(files_chunk: List[str]) -> bool:
    """
    并行扫描worker函数，检查文件批次中是否包含snapshot。
    
    Args:
        files_chunk: 要扫描的文件列表
    
    Returns:
        是否找到snapshot
    """
    temp_dir = tempfile.mkdtemp(prefix='okx_scan_')
    
    try:
        for file_path in files_chunk:
            try:
                if file_path.endswith('.7z'):
                    json_file = extract_7z(file_path, temp_dir)
                else:
                    json_file = file_path
                
                # 快速检查是否包含snapshot
                with open(json_file, 'rb') as f:
                    data = json_loads(f.read())
                
                for item in data['data']:
                    if item['action'] == 'snapshot':
                        return True
                
                # 清理临时文件
                if file_path.endswith('.7z') and os.path.exists(json_file):
                    os.remove(json_file)
                    
            except Exception as e:
                print(f"扫描文件 {file_path} 时出错: {e}")
                # 清理临时文件
                if file_path.endswith('.7z'):
                    json_files = list(Path(temp_dir).glob('*.json'))
                    for jf in json_files:
                        if jf.exists():
                            jf.unlink()
                continue
    finally:
        # 清理临时目录
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    
    return False


def process_file_worker(file_info_chunk: Tuple[List[str], str, float, bool]) -> Tuple[np.ndarray, bool]:
    """
    多进程worker函数，处理一批文件。
    
    Args:
        file_info_chunk: 包含(文件列表, 数据类型, feed_latency, found_snapshot)的元组
    
    Returns:
        Tuple of (events_array, found_snapshot)
    """
    files, data_type, feed_latency, found_snapshot = file_info_chunk
    
    if not files:
        return np.empty(0, event_dtype), found_snapshot
    
    all_events = []
    temp_dir = tempfile.mkdtemp(prefix='okx_worker_')
    
    try:
        for file_path in files:
            try:
                # print(f"Worker处理: {file_path}")
                
                if file_path.endswith('.7z'):
                    json_file = extract_7z(file_path, temp_dir)
                else:
                    json_file = file_path
                
                if data_type == 'Books':
                    events, found_snapshot = process_books_file(
                        json_file, feed_latency, found_snapshot
                    )
                elif data_type == 'Trades':
                    events = process_trades_file(json_file, feed_latency)
                else:
                    continue
                
                if len(events) > 0:
                    all_events.append(events)
                
                # 清理临时文件
                if file_path.endswith('.7z') and os.path.exists(json_file):
                    os.remove(json_file)
                    
            except Exception as e:
                print(f"处理文件 {file_path} 时出错: {e}")
                continue
    
    finally:
        # 清理临时目录
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    
    if not all_events:
        return np.empty(0, event_dtype), found_snapshot
    
    # 合并所有事件
    if len(all_events) == 1:
        combined_events = all_events[0]
    else:
        total_length = sum(len(events) for events in all_events)
        combined_events = np.empty(total_length, event_dtype)
        offset = 0
        for events in all_events:
            combined_events[offset:offset + len(events)] = events
            offset += len(events)
    
    return combined_events, found_snapshot


def merge_event_arrays(event_arrays: List[np.ndarray]) -> np.ndarray:
    """
    合并多个事件数组并按时间排序。
    
    Args:
        event_arrays: 事件数组列表
        
    Returns:
        合并并排序后的事件数组
    """
    if not event_arrays:
        return np.empty(0, event_dtype)
    
    # 过滤空数组
    non_empty_arrays = [arr for arr in event_arrays if len(arr) > 0]
    
    if not non_empty_arrays:
        return np.empty(0, event_dtype)
    
    if len(non_empty_arrays) == 1:
        return non_empty_arrays[0]
    
    # 计算总长度
    total_length = sum(len(arr) for arr in non_empty_arrays)
    
    # 合并数组
    merged = np.empty(total_length, event_dtype)
    offset = 0
    for arr in non_empty_arrays:
        merged[offset:offset + len(arr)] = arr
        offset += len(arr)
    
    # 按交易所时间戳排序以保持时间顺序
    sort_indices = np.argsort(merged['exch_ts'], kind='mergesort')
    return merged[sort_indices]


def convert(
    file_patterns: List[str],
    output_filename: Optional[str] = None,
    buffer_size: int = 500_000_000,
    feed_latency: float = 0,
    base_latency: float = 0,
    simulated_latency: Optional[float] = None,
    num_processes: Optional[int] = None
) -> NDArray:
    """
    转换OKX市场数据文件为HftBacktest兼容格式（支持多进程）。

    Args:
        file_patterns: 文件路径模式列表，支持通配符或目录（如 ['data/OKX-*.7z'] 或 ['data/']）
        output_filename: 如果提供，转换后的数据将保存为npz格式
        buffer_size: 预分配缓冲区大小（已弃用，保留兼容性）
        feed_latency: 人工喂送延迟值（纳秒），用于创建local timestamp
        base_latency: 要添加到喂送延迟的值。参见 correct_local_timestamp
        simulated_latency: 如果数据缺少localTs，使用此值模拟延迟（纳秒）。
                          如果为None且需要，将使用feed_latency
        num_processes: 使用的进程数，None表示使用CPU核心数

    Returns:
        与HftBacktest兼容的转换后数据
    """
    # 收集文件，支持目录递归
    files = collect_files(file_patterns)

    if not files:
        raise FileNotFoundError(f"没有找到匹配的文件: {file_patterns}")

    print(f"找到 {len(files)} 个文件")

    # 按文件名中的起始时间戳排序
    file_info = []
    for f in files:
        try:
            data_type, base, quote, depth, start_time, end_time = parse_filename(f)
            file_info.append((f, data_type, start_time))
        except ValueError as e:
            print(f"警告: 跳过文件 {f}: {e}")
            continue

    # 按起始时间排序
    file_info.sort(key=lambda x: x[2])

    # 分离Books和Trades文件
    books_files = [f for f, t, _ in file_info if t == 'Books']
    trades_files = [f for f, t, _ in file_info if t == 'Trades']

    print(f"Books文件: {len(books_files)}, Trades文件: {len(trades_files)}")

    # 如果未指定simulated_latency，使用feed_latency
    if simulated_latency is None:
        simulated_latency = feed_latency

    # 确定进程数
    if num_processes is None:
        num_processes = min(cpu_count(), max(1, len(files) // 4))  # 每个进程至少处理4个文件
    
    print(f"使用 {num_processes} 个进程进行并行处理")

    # 分批处理文件
    def create_batches(file_list, data_type, num_procs):
        """将文件列表分成批次"""
        if not file_list:
            return []
        
        batch_size = max(1, len(file_list) // num_procs)
        batches = []
        for i in range(0, len(file_list), batch_size):
            batch = file_list[i:i + batch_size]
            batches.append((batch, data_type, feed_latency, False))
        return batches

    # 创建批次
    books_batches = create_batches(books_files, 'Books', num_processes)
    trades_batches = create_batches(trades_files, 'Trades', num_processes)

    all_event_arrays = []
    found_snapshot = False

    # 处理Books文件（需要保持顺序，先处理以确保找到snapshot）
    if books_batches:
        print("使用多进程处理Books文件...")
        
        # 并行扫描所有Books文件以查找snapshot
        print("并行扫描文件以查找snapshot...")
        
        # 创建扫描批次
        scan_batch_size = max(1, len(books_files) // num_processes)
        scan_batches = []
        for i in range(0, len(books_files), scan_batch_size):
            batch = books_files[i:i + scan_batch_size]
            scan_batches.append(batch)
        
        # 并行扫描
        first_snapshot_found = False
        if num_processes > 1:
            with Pool(num_processes) as pool:
                scan_results = pool.map(scan_snapshot_worker, scan_batches)
                first_snapshot_found = any(scan_results)
        else:
            # 单进程扫描
            for batch in scan_batches:
                if scan_snapshot_worker(batch):
                    first_snapshot_found = True
                    break
        
        if not first_snapshot_found:
            raise ValueError("未找到任何snapshot数据！无法处理深度更新。")
        
        print("找到snapshot，开始并行处理...")
        
        # 现在我们知道有snapshot，可以安全地并行处理所有批次
        # 更新所有批次的snapshot状态为True
        updated_batches = []
        for batch_files, data_type, feed_lat, _ in books_batches:
            updated_batches.append((batch_files, data_type, feed_lat, True))
        
        if num_processes > 1:
            with Pool(num_processes) as pool:
                results = pool.map(process_file_worker, updated_batches)
                for events, _ in results:
                    if len(events) > 0:
                        all_event_arrays.append(events)
        else:
            # 单进程处理
            for batch in updated_batches:
                events, _ = process_file_worker(batch)
                if len(events) > 0:
                    all_event_arrays.append(events)
        
        found_snapshot = True

    # 处理Trades文件
    if trades_batches:
        print("使用多进程处理Trades文件...")
        if num_processes > 1:
            with Pool(num_processes) as pool:
                results = pool.map(process_file_worker, trades_batches)
                for events, _ in results:
                    if len(events) > 0:
                        all_event_arrays.append(events)
        else:
            # 单进程处理
            for batch in trades_batches:
                events, _ = process_file_worker(batch)
                if len(events) > 0:
                    all_event_arrays.append(events)

    if not all_event_arrays:
        raise ValueError("没有处理任何数据！")

    print("合并所有事件数据...")
    tmp = merge_event_arrays(all_event_arrays)
    
    print(f"总共处理了 {len(tmp)} 条事件")

    # 检查是否需要生成单调local timestamp
    needs_monotonic_ts = False
    if len(tmp) > 0:
        # 简单检查：如果第一批事件的local_ts都是exch_ts + feed_latency
        sample_size = min(4000, len(tmp))
        matches = 0
        for i in range(sample_size):
            if tmp[i]['local_ts'] == tmp[i]['exch_ts'] + int(feed_latency):
                matches += 1
        if matches > sample_size * 0.9:  # 90%以上匹配
            needs_monotonic_ts = True
            print("检测到合成的local timestamp，将生成单调递增序列")

    if needs_monotonic_ts:
        print("生成单调递增的local timestamp")
        generate_monotonic_local_timestamp(tmp, int(simulated_latency))

    print("修正延迟")
    tmp = correct_local_timestamp(tmp, base_latency)

    print("修正事件顺序")
    data = correct_event_order(
        tmp,
        np.argsort(tmp['exch_ts'], kind='mergesort'),
        np.argsort(tmp['local_ts'], kind='mergesort')
    )

    validate_event_order(data)

    if output_filename is not None:
        print(f"保存到 {output_filename}")
        np.savez_compressed(output_filename, data=data)

    return data


def main():
    """CLI入口点"""
    parser = argparse.ArgumentParser(
        description='将OKX市场数据转换为HftBacktest格式',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
 示例:
   %(prog)s "data/OKX-*.7z" -o output.npz
   %(prog)s "data/OKX-Books-BTC-USDT-*.7z" -o btcusdt_books.npz --feed-latency 1000000
   %(prog)s "data/OKX-Trades-*.7z" "data/OKX-Books-*.7z" -o combined.npz
   %(prog)s "data/" -o output.npz  # 递归处理目录中的所有文件

注意:
  - 支持通配符（*）匹配多个文件
  - 文件将按时间戳自动排序
  - 需要至少一个snapshot才能处理深度数据
  - 延迟单位为纳秒
        """
    )

    parser.add_argument(
        'files',
        nargs='+',
        help='输入文件路径（支持通配符）'
    )
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='输出文件路径（.npz格式）'
    )
    parser.add_argument(
        '--buffer-size',
        type=int,
        default=500_000_000,
        help='缓冲区大小（默认: 500000000）'
    )
    parser.add_argument(
        '--feed-latency',
        type=float,
        default=0,
        help='人工喂送延迟（纳秒，默认: 0）'
    )
    parser.add_argument(
        '--base-latency',
        type=float,
        default=0,
        help='基础延迟修正值（纳秒，默认: 0）'
    )
    parser.add_argument(
        '--simulated-latency',
        type=float,
        default=None,
        help='模拟延迟，用于生成单调local timestamp（纳秒，默认: 使用feed-latency）'
    )
    parser.add_argument(
        '--num-processes',
        type=int,
        default=None,
        help='使用的进程数（默认: 自动确定基于CPU核心数）'
    )

    args = parser.parse_args()

    # 合并所有文件模式
    all_files = []
    for pattern in args.files:
        matched = glob.glob(pattern)
        if not matched:
            print(f"警告: 未找到匹配的文件: {pattern}", file=sys.stderr)
        all_files.extend(matched)

    if not all_files:
        print("错误: 未找到任何文件", file=sys.stderr)
        sys.exit(1)

    # 为所有文件创建一个临时文件列表
    temp_pattern_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
    try:
        for f in all_files:
            temp_pattern_file.write(f + '\n')
        temp_pattern_file.close()

        # 使用第一个文件的目录作为基础，创建通配符模式
        if len(all_files) == 1:
            file_pattern = all_files[0]
        else:
            # 对于多个文件，我们需要逐个处理
            # 这里我们创建一个临时的通配符
            base_dir = os.path.dirname(all_files[0])
            file_pattern = os.path.join(base_dir, '*')

        try:
            # 直接处理文件列表而不是使用通配符
            # 我们需要修改convert函数来接受文件列表
            # 转换数据
            convert(
                all_files,
                output_filename=args.output,
                buffer_size=args.buffer_size,
                feed_latency=args.feed_latency,
                base_latency=args.base_latency,
                simulated_latency=args.simulated_latency,
                num_processes=args.num_processes
            )
            print("转换完成！")
        except Exception as e:
            print(f"错误: {e}", file=sys.stderr)
            sys.exit(1)
    finally:
        os.unlink(temp_pattern_file.name)


if __name__ == '__main__':
    main()