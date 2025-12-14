#!/usr/bin/env python3
"""
OKX数据转换器 (Memmap版) - 将OKX原始市场数据转换为HftBacktest格式
解决内存不足问题，使用 np.memmap 处理大数据。
"""

import argparse
import glob
import os
import re
import shutil
import sys
import tempfile
import psutil
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
    EXCH_EVENT,
    LOCAL_EVENT,
    event_dtype,
    EVENT_ARRAY
)
from hftbacktest.data.validation import correct_local_timestamp, validate_event_order

INT64_MAX = np.int64(np.iinfo(np.int64).max)

# ---------------- Copied from hftbacktest_okx.py ----------------

@njit
def find_first_snapshot_ts(data: EVENT_ARRAY) -> int:
    for i in range(len(data)):
        if data[i].ev & DEPTH_SNAPSHOT_EVENT:
            return data[i].exch_ts
    return -1

@njit
def filter_events_by_ts(data: EVENT_ARRAY, min_ts: int) -> EVENT_ARRAY:
    count = 0
    for i in range(len(data)):
        if data[i].exch_ts > min_ts:
            count += 1
    
    result = np.empty(count, data.dtype)
    idx = 0
    for i in range(len(data)):
        if data[i].exch_ts > min_ts:
            result[idx] = data[i]
            idx += 1
    
    return result

@njit
def generate_lognormal_latencies(
    n: int,
    mu: float = 0.0,
    sigma: float = 1.0,
    seed: int = 42
) -> np.ndarray:
    rng = np.random.default_rng(seed)
    lognormal_samples = rng.lognormal(mean=float(mu), sigma=float(sigma), size=n)
    latencies_ns = lognormal_samples * 1_000_000.0

    int64_max = float(INT64_MAX)
    overflow_mask = ~np.isfinite(latencies_ns) | (latencies_ns > int64_max)
    overflow_count = int(np.count_nonzero(overflow_mask))
    if overflow_count > 0:
        latencies_ns[overflow_mask] = int64_max
        print(f"警告: 有 {overflow_count} 个随机延迟值超过 int64 范围，已自动截断。")

    latencies_ns = np.clip(latencies_ns, 0.0, int64_max)
    return latencies_ns.astype(np.int64, copy=False)

@njit
def generate_monotonic_local_timestamp_with_random_latency(
    data: EVENT_ARRAY,
    latencies: np.ndarray,
    min_timestamp_increment: int = 1
) -> None:
    last_local_ts = np.int64(0)
    min_increment_i64 = np.int64(min_timestamp_increment)
    if min_increment_i64 < 0:
        min_increment_i64 = np.int64(0)
    max_last_without_overflow = INT64_MAX - min_increment_i64
    if max_last_without_overflow < 0:
        max_last_without_overflow = np.int64(0)

    for i in range(len(data)):
        exch_ts = data[i].exch_ts
        latency_i = latencies[i]
        if latency_i < 0:
            latency_i = np.int64(0)

        max_latency_without_overflow = INT64_MAX - exch_ts
        if max_latency_without_overflow < 0:
            max_latency_without_overflow = np.int64(0)

        if latency_i > max_latency_without_overflow:
            candidate_local_ts = INT64_MAX
        else:
            candidate_local_ts = exch_ts + latency_i

        if last_local_ts > max_last_without_overflow:
            monotonic_threshold = INT64_MAX
        else:
            monotonic_threshold = last_local_ts + min_increment_i64

        if candidate_local_ts < monotonic_threshold:
            final_local_ts = monotonic_threshold
        else:
            final_local_ts = candidate_local_ts

        if final_local_ts > INT64_MAX:
            final_local_ts = INT64_MAX

        data[i].local_ts = final_local_ts
        last_local_ts = final_local_ts

@njit
def generate_monotonic_local_timestamp(
    data: EVENT_ARRAY,
    simulated_latency: float,
    min_timestamp_increment: int = 1
) -> None:
    last_local_ts = np.int64(0)
    min_increment_i64 = np.int64(min_timestamp_increment)
    if min_increment_i64 < 0:
        min_increment_i64 = np.int64(0)
    max_last_without_overflow = INT64_MAX - min_increment_i64
    if max_last_without_overflow < 0:
        max_last_without_overflow = np.int64(0)
 
    latency_i64 = np.int64(simulated_latency)
    if latency_i64 < 0:
        latency_i64 = np.int64(0)
 
    for i in range(len(data)):
        exch_ts = data[i].exch_ts
 
        max_latency_without_overflow = INT64_MAX - exch_ts
        if max_latency_without_overflow < 0:
            max_latency_without_overflow = np.int64(0)
 
        if latency_i64 > max_latency_without_overflow:
            candidate_local_ts = INT64_MAX
        else:
            candidate_local_ts = exch_ts + latency_i64
 
        if last_local_ts > max_last_without_overflow:
            new_local_ts = INT64_MAX
        else:
            monotonic_threshold = last_local_ts + min_increment_i64
            if candidate_local_ts < monotonic_threshold:
                new_local_ts = monotonic_threshold
            else:
                new_local_ts = candidate_local_ts
 
        if new_local_ts > INT64_MAX:
            new_local_ts = INT64_MAX
 
        data[i].local_ts = new_local_ts
        last_local_ts = new_local_ts

def parse_filename(filename: str) -> Tuple[str, str, str, str, int, int]:
    basename = os.path.basename(filename)
    if basename.endswith('.7z'):
        basename = basename[:-3]
    elif basename.endswith('.json'):
        basename = basename[:-5]

    pattern = r'OKX-(Books|Trades)-([A-Z0-9]+)-([A-Z0-9]+)(?:-(\d+))?-(\d+)-(\d+)'
    match = re.match(pattern, basename)

    if not match:
        raise ValueError(f"Invalid OKX filename format: {filename}")

    data_type = match.group(1)
    base = match.group(2)
    quote = match.group(3)
    depth = match.group(4) if match.group(4) else "0"
    start_time = int(match.group(5))
    end_time = int(match.group(6))

    return data_type, base, quote, depth, start_time, end_time

def extract_7z(filename: str, temp_dir: str) -> str:
    try:
        import py7zr
    except ImportError:
        raise ImportError("需要安装 py7zr 来处理7z文件。请运行: pip install py7zr")

    with py7zr.SevenZipFile(filename, mode='r') as archive:
        archive.extractall(path=temp_dir)

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
    with open(json_file, 'rb') as f:
        data = json_loads(f.read())

    estimated_events = 0
    for item in data['data']:
        book_data = item['data'][0]
        estimated_events += len(book_data['bids']) + len(book_data['asks'])
        if item['action'] == 'snapshot':
            estimated_events += 2
    
    buffer = np.empty(estimated_events, event_dtype)
    row_num = 0

    for item in data['data']:
        action = item['action']

        if not found_snapshot and action == 'update':
            continue

        if action == 'snapshot':
            found_snapshot = True

        book_data = item['data'][0]
        ts_ms = int(book_data['ts'])
        exch_ts = ts_ms * 1_000_000

        if 'localTs' in item:
            local_ts = int(item['localTs']) * 1_000_000
        else:
            local_ts = exch_ts + int(feed_latency)

        if action == 'snapshot':
            if len(book_data['bids']) > 0:
                bid_clear_px = float(book_data['bids'][-1][0])
                buffer[row_num] = (DEPTH_CLEAR_EVENT | BUY_EVENT, exch_ts, local_ts, bid_clear_px, 0, 0, 0, 0)
                row_num += 1

            for price, qty, _, _ in book_data['bids']:
                buffer[row_num] = (DEPTH_SNAPSHOT_EVENT | BUY_EVENT, exch_ts, local_ts, float(price), float(qty), 0, 0, 0)
                row_num += 1
        else:
            for price, qty, _, _ in book_data['bids']:
                buffer[row_num] = (DEPTH_EVENT | BUY_EVENT, exch_ts, local_ts, float(price), float(qty), 0, 0, 0)
                row_num += 1

        if action == 'snapshot':
            if len(book_data['asks']) > 0:
                ask_clear_px = float(book_data['asks'][-1][0])
                buffer[row_num] = (DEPTH_CLEAR_EVENT | SELL_EVENT, exch_ts, local_ts, ask_clear_px, 0, 0, 0, 0)
                row_num += 1

            for price, qty, _, _ in book_data['asks']:
                buffer[row_num] = (DEPTH_SNAPSHOT_EVENT | SELL_EVENT, exch_ts, local_ts, float(price), float(qty), 0, 0, 0)
                row_num += 1
        else:
            for price, qty, _, _ in book_data['asks']:
                buffer[row_num] = (DEPTH_EVENT | SELL_EVENT, exch_ts, local_ts, float(price), float(qty), 0, 0, 0)
                row_num += 1

    return buffer[:row_num], found_snapshot

def process_trades_file(
    json_file: str,
    feed_latency: float
) -> np.ndarray:
    with open(json_file, 'rb') as f:
        data = json_loads(f.read())

    buffer = np.empty(len(data['data']), event_dtype)
    row_num = 0

    for item in data['data']:
        trade_data = item['data'][0]
        ts_ms = int(trade_data['ts'])
        exch_ts = ts_ms * 1_000_000

        if 'localTs' in item:
            local_ts = int(item['localTs']) * 1_000_000
        else:
            local_ts = exch_ts + int(feed_latency)

        price = float(trade_data['px'])
        qty = float(trade_data['sz'])
        side = trade_data['side']

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
    files = []
    for pattern in patterns:
        if os.path.isdir(pattern):
            path = Path(pattern)
            files.extend(str(p) for p in path.rglob('*.7z'))
            files.extend(str(p) for p in path.rglob('*.json'))
        else:
            files.extend(glob.glob(pattern))
    return sorted(list(set(files)))

def scan_snapshot_worker(files_chunk: List[str]) -> bool:
    temp_dir = tempfile.mkdtemp(prefix='okx_scan_')
    try:
        for file_path in files_chunk:
            try:
                if file_path.endswith('.7z'):
                    json_file = extract_7z(file_path, temp_dir)
                else:
                    json_file = file_path
                
                with open(json_file, 'rb') as f:
                    data = json_loads(f.read())
                
                for item in data['data']:
                    if item['action'] == 'snapshot':
                        return True
                
                if file_path.endswith('.7z') and os.path.exists(json_file):
                    os.remove(json_file)
                    
            except Exception as e:
                print(f"扫描文件 {file_path} 时出错: {e}")
                if file_path.endswith('.7z'):
                    json_files = list(Path(temp_dir).glob('*.json'))
                    for jf in json_files:
                        if jf.exists():
                            jf.unlink()
                continue
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    return False

def process_file_worker(file_info_chunk: Tuple[List[str], str, float, bool]) -> Tuple[np.ndarray, bool]:
    files, data_type, feed_latency, found_snapshot = file_info_chunk
    
    if not files:
        return np.empty(0, event_dtype), found_snapshot
    
    all_events = []
    temp_dir = tempfile.mkdtemp(prefix='okx_worker_')
    
    try:
        for file_path in files:
            try:
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
                
                if file_path.endswith('.7z') and os.path.exists(json_file):
                    os.remove(json_file)
                    
            except Exception as e:
                print(f"处理文件 {file_path} 时出错: {e}")
                raise e
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    
    if not all_events:
        return np.empty(0, event_dtype), found_snapshot
    
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

# ---------------- New Memmap Functions ----------------

@njit
def merge_arrays_by_exch_ts_mmap(depth_data: EVENT_ARRAY, trade_data: EVENT_ARRAY, out: EVENT_ARRAY) -> None:
    len_d = len(depth_data)
    len_t = len(trade_data)
    
    ptr_d = 0
    ptr_t = 0
    idx = 0
    
    while ptr_d < len_d and ptr_t < len_t:
        if depth_data[ptr_d].exch_ts <= trade_data[ptr_t].exch_ts:
            out[idx] = depth_data[ptr_d]
            ptr_d += 1
        else:
            out[idx] = trade_data[ptr_t]
            ptr_t += 1
        idx += 1
    
    while ptr_d < len_d:
        out[idx] = depth_data[ptr_d]
        ptr_d += 1
        idx += 1
    
    while ptr_t < len_t:
        out[idx] = trade_data[ptr_t]
        ptr_t += 1
        idx += 1

@njit
def correct_event_order_mmap(
        data: EVENT_ARRAY,
        sorted_exch_index: NDArray,
        sorted_local_index: NDArray,
        out: EVENT_ARRAY
) -> int:
    out_rn = 0
    exch_rn = 0
    local_rn = 0
    while True:
        sorted_exch = data[sorted_exch_index[exch_rn]]
        sorted_local = data[sorted_local_index[local_rn]]
        if (
                exch_rn < len(data)
                and local_rn < len(data)
                and sorted_exch.exch_ts == sorted_local.exch_ts
                and sorted_exch.local_ts == sorted_local.local_ts
        ):
            # assert sorted_exch.ev == sorted_local.ev
            # assert (sorted_exch.px == sorted_local.px) or (np.isnan(sorted_exch.px) and np.isnan(sorted_local.px))
            # assert sorted_exch.qty == sorted_local.qty

            out[out_rn] = sorted_exch
            out[out_rn].ev = out[out_rn].ev | EXCH_EVENT | LOCAL_EVENT

            out_rn += 1
            exch_rn += 1
            local_rn += 1
        elif ((
                exch_rn < len(data)
                and local_rn < len(data)
                and sorted_exch.exch_ts == sorted_local.exch_ts
                and sorted_exch.local_ts < sorted_local.local_ts
        ) or (
                exch_rn < len(data)
                and sorted_exch.exch_ts < sorted_local.exch_ts
        )):
            # exchange
            out[out_rn] = sorted_exch
            out[out_rn].ev = out[out_rn].ev | EXCH_EVENT

            out_rn += 1
            exch_rn += 1
        elif ((
                exch_rn < len(data)
                and local_rn < len(data)
                and sorted_exch.exch_ts == sorted_local.exch_ts
                and sorted_exch.local_ts > sorted_local.local_ts
        ) or (
                local_rn < len(data)
        )):
            # local
            out[out_rn] = sorted_local
            out[out_rn].ev = out[out_rn].ev | LOCAL_EVENT

            out_rn += 1
            local_rn += 1
        elif exch_rn < len(data):
            # exchange
            out[out_rn] = sorted_exch
            out[out_rn].ev = out[out_rn].ev | EXCH_EVENT

            out_rn += 1
            exch_rn += 1
        else:
            break
    return out_rn

def convert(
    file_patterns: List[str],
    output_filename: Optional[str] = None,
    buffer_size: int = 500_000_000,
    feed_latency: float = 0,
    base_latency: float = 0,
    simulated_latency: Optional[float] = None,
    num_processes: Optional[int] = None,
    latency_mu: float = 0.0,
    latency_sigma: float = 1.0,
    use_random_latency: bool = False,
    random_seed: int = 42
) -> NDArray:
    files = collect_files(file_patterns)
    if not files:
        raise FileNotFoundError(f"没有找到匹配的文件: {file_patterns}")
    print(f"找到 {len(files)} 个文件")

    file_info = []
    for f in files:
        try:
            data_type, base, quote, depth, start_time, end_time = parse_filename(f)
            file_info.append((f, data_type, start_time))
        except ValueError as e:
            print(f"警告: 跳过文件 {f}: {e}")
            continue
    file_info.sort(key=lambda x: x[2])

    books_files = [f for f, t, _ in file_info if t == 'Books']
    trades_files = [f for f, t, _ in file_info if t == 'Trades']
    print(f"Books文件: {len(books_files)}, Trades文件: {len(trades_files)}")

    if simulated_latency is None:
        simulated_latency = feed_latency

    if num_processes is None:
        # 根据可用内存动态调整进程数
        try:
            mem = psutil.virtual_memory()
            # 假设每个进程处理大文件时可能需要高达 1.5GB 内存 (解压+JSON解析+Numpy数组)
            # 保守估计，留出 2GB 系统缓冲
            available_mem = mem.available - 2 * 1024**3
            if available_mem < 0:
                available_mem = 1024**3 # 至少给1G
            
            estimated_per_process = 1.5 * 1024**3
            safe_procs = max(1, int(available_mem / estimated_per_process))
            
            num_processes = min(cpu_count(), safe_procs)
            print(f"根据内存限制 ({mem.available / 1024**3:.2f} GB 可用), 调整进程数为: {num_processes}")
        except Exception as e:
            print(f"无法获取内存信息: {e}, 使用默认进程数计算")
            num_processes = min(cpu_count(), max(1, len(files) // 4))
            
    print(f"使用 {num_processes} 个进程进行并行处理")

    def create_batches(file_list, data_type, num_procs):
        if not file_list:
            return []
        # 使用小批次 (1个文件) 以减少每个工作进程的内存占用
        batch_size = 1
        batches = []
        for i in range(0, len(file_list), batch_size):
            batch = file_list[i:i + batch_size]
            batches.append((batch, data_type, feed_latency, False))
        return batches

    books_batches = create_batches(books_files, 'Books', num_processes)
    trades_batches = create_batches(trades_files, 'Trades', num_processes)

    # 使用临时目录存储中间文件
    work_dir = tempfile.mkdtemp(prefix='okx_mmap_')
    print(f"使用临时目录: {work_dir}")

    try:
        # 1. 处理 Books
        books_temp_files = []
        total_books_len = 0
        
        if books_batches:
            print("使用多进程处理Books文件...")
            # 扫描 snapshot (同原版)
            print("并行扫描文件以查找snapshot...")
            scan_batch_size = max(1, len(books_files) // num_processes)
            scan_batches = []
            for i in range(0, len(books_files), scan_batch_size):
                batch = books_files[i:i + scan_batch_size]
                scan_batches.append(batch)
            
            first_snapshot_found = False
            if num_processes > 1:
                with Pool(num_processes) as pool:
                    scan_results = pool.map(scan_snapshot_worker, scan_batches)
                    first_snapshot_found = any(scan_results)
            else:
                for batch in scan_batches:
                    if scan_snapshot_worker(batch):
                        first_snapshot_found = True
                        break
            
            if not first_snapshot_found:
                raise ValueError("未找到任何snapshot数据！无法处理深度更新。")
            
            print("找到snapshot，开始并行处理...")
            updated_batches = []
            for batch_files, data_type, feed_lat, _ in books_batches:
                updated_batches.append((batch_files, data_type, feed_lat, True))
            
            # 使用 imap 处理并立即保存到磁盘
            with Pool(num_processes) as pool:
                for i, (events, _) in enumerate(pool.imap(process_file_worker, updated_batches)):
                    if len(events) > 0:
                        tmp_path = os.path.join(work_dir, f"books_{i}.npy")
                        np.save(tmp_path, events)
                        books_temp_files.append(tmp_path)
                        total_books_len += len(events)
                        del events # 释放内存

        # 创建 Books memmap
        print(f"合并深度数据 (总大小: {total_books_len})...")
        if total_books_len > 0:
            depth_mmap_path = os.path.join(work_dir, "depth.mmap")
            depth_events = np.memmap(depth_mmap_path, dtype=event_dtype, mode='w+', shape=(total_books_len,))
            offset = 0
            for tmp_path in books_temp_files:
                chunk = np.load(tmp_path)
                depth_events[offset:offset + len(chunk)] = chunk
                offset += len(chunk)
                os.remove(tmp_path) # 删除临时文件
            
            # Flush to ensure data is written
            depth_events.flush()
            
            first_snapshot_ts = find_first_snapshot_ts(depth_events)
            if first_snapshot_ts >= 0:
                print(f"第一个snapshot时间戳: {first_snapshot_ts}")
        else:
            depth_events = np.empty(0, event_dtype)
            first_snapshot_ts = -1

        # 2. 处理 Trades
        trades_temp_files = []
        total_trades_len = 0
        
        if trades_batches:
            print("使用多进程处理Trades文件...")
            with Pool(num_processes) as pool:
                for i, (events, _) in enumerate(pool.imap(process_file_worker, trades_batches)):
                    if len(events) > 0:
                        tmp_path = os.path.join(work_dir, f"trades_{i}.npy")
                        np.save(tmp_path, events)
                        trades_temp_files.append(tmp_path)
                        total_trades_len += len(events)
                        del events

        # 创建 Trades memmap 并过滤
        print(f"合并交易数据 (原始大小: {total_trades_len})...")
        if total_trades_len > 0:
            trade_mmap_path = os.path.join(work_dir, "trades.mmap")
            # 先分配最大可能空间
            trade_events_raw = np.memmap(trade_mmap_path, dtype=event_dtype, mode='w+', shape=(total_trades_len,))
            
            current_idx = 0
            for tmp_path in trades_temp_files:
                chunk = np.load(tmp_path)
                if first_snapshot_ts >= 0:
                    # 过滤
                    filtered_chunk = filter_events_by_ts(chunk, first_snapshot_ts)
                    if len(filtered_chunk) > 0:
                        trade_events_raw[current_idx:current_idx + len(filtered_chunk)] = filtered_chunk
                        current_idx += len(filtered_chunk)
                else:
                    trade_events_raw[current_idx:current_idx + len(chunk)] = chunk
                    current_idx += len(chunk)
                os.remove(tmp_path)
            
            trade_events_raw.flush()
            
            # 截断 memmap (通过切片，实际文件大小不变，但我们只用有效部分)
            # 或者创建一个新的 memmap 指向同一文件但不同 shape? 
            # np.memmap 不支持 resize。
            # 我们可以只使用切片。
            trade_events = trade_events_raw[:current_idx]
            print(f"过滤后交易事件: {len(trade_events)}")
        else:
            trade_events = np.empty(0, event_dtype)

        print(f"深度事件: {len(depth_events)}, 交易事件: {len(trade_events)}")

        # 3. 归并合并
        print("归并合并深度和交易数据...")
        total_merged_len = len(depth_events) + len(trade_events)
        merged_mmap_path = os.path.join(work_dir, "merged.mmap")
        merged_events = np.memmap(merged_mmap_path, dtype=event_dtype, mode='w+', shape=(total_merged_len,))
        
        merge_arrays_by_exch_ts_mmap(depth_events, trade_events, merged_events)
        merged_events.flush()
        
        # 释放之前的 memmap
        del depth_events
        del trade_events
        # (Optional: remove depth.mmap and trades.mmap files to save disk space)
        if os.path.exists(os.path.join(work_dir, "depth.mmap")):
            os.remove(os.path.join(work_dir, "depth.mmap"))
        if os.path.exists(os.path.join(work_dir, "trades.mmap")):
            os.remove(os.path.join(work_dir, "trades.mmap"))

        tmp = merged_events
        print(f"归并后总共 {len(tmp)} 条事件")

        # 4. 生成单调本地时间戳
        needs_monotonic_ts = False
        if len(tmp) > 0:
            sample_size = min(4000, len(tmp))
            matches = sum(1 for i in range(sample_size) if tmp[i]['local_ts'] == tmp[i]['exch_ts'] + int(feed_latency))
            if matches > sample_size * 0.9:
                needs_monotonic_ts = True
                print("检测到合成的local timestamp，将生成单调递增序列")

        if needs_monotonic_ts:
            if use_random_latency:
                print(f"使用对数正态分布随机延迟 (mu={latency_mu}, sigma={latency_sigma}, seed={random_seed})")
                latencies = generate_lognormal_latencies(len(tmp), latency_mu, latency_sigma, random_seed)
                generate_monotonic_local_timestamp_with_random_latency(tmp, latencies)
            else:
                print("使用固定延迟生成单调local timestamp")
                generate_monotonic_local_timestamp(tmp, int(simulated_latency))
            tmp.flush()

        # 5. 最终处理
        print("修正延迟")
        # correct_local_timestamp modifies in-place
        correct_local_timestamp(tmp, base_latency)
        tmp.flush()

        print("修正事件顺序")
        # argsort returns indices, which can be large but usually fit in memory (8 bytes * 100M = 800MB)
        # If memory is very tight, argsort might fail. But usually it's the data copy that kills it.
        print("Sorting indices...")
        sorted_exch_idx = np.argsort(tmp['exch_ts'], kind='mergesort')
        sorted_local_idx = np.argsort(tmp['local_ts'], kind='mergesort')
        
        # Create final memmap (2x size)
        final_mmap_path = os.path.join(work_dir, "final.mmap")
        final_events = np.memmap(final_mmap_path, dtype=event_dtype, mode='w+', shape=(len(tmp) * 2,))
        
        print("Correcting event order...")
        out_rn = correct_event_order_mmap(tmp, sorted_exch_idx, sorted_local_idx, final_events)
        final_events.flush()
        
        data = final_events[:out_rn]
        
        print("验证事件顺序")
        validate_event_order(data)

        if output_filename is not None:
            print(f"保存到 {output_filename}")
            np.savez_compressed(output_filename, data=data)

        return data

    finally:
        # 清理临时目录
        try:
            # Close memmaps before deleting
            # In Python, memmaps are closed when garbage collected.
            # We can try to force it.
            pass 
        except:
            pass
        # shutil.rmtree(work_dir) # Don't delete immediately if we return data which is a view into memmap?
        # But we return data. If we delete the file, the memmap might become invalid on some OS (Windows allows deletion but keeps handle? No, Windows locks it).
        # On Windows, we cannot delete open files.
        # So we should probably copy the result to output_filename and then close/delete.
        # The function returns 'data'. If 'data' is a memmap, the file must exist.
        # But the caller expects an array.
        # If we return a memmap backed by a temp file, and then delete the temp dir, it will fail on Windows.
        # However, the main use case here is saving to output_filename.
        # If output_filename is provided, we saved it.
        # If we return 'data', it's a memmap.
        # We should probably warn or handle this.
        # Given the script is mostly used as CLI or via process_segments_to_npz which saves to file, 
        # we can probably rely on the save.
        # But to be safe, we should not delete work_dir if we are returning the memmap.
        # But we want to clean up.
        # Let's just leave the cleanup to the OS or user if we return memmap?
        # Or, since we saved to output_filename, we can return a small placeholder or load the saved file?
        # The original convert returns the array.
        # If we want to be clean, we should probably not return the memmap if we want to delete the file.
        # But the signature says returns NDArray.
        
        # For this task, process_segments_to_npz calls convert and expects it to finish. 
        # It doesn't use the return value except for checking success (it wraps in try-except).
        # So we can close everything and delete.
        # But we can't close 'data' explicitly easily.
        # We can 'del data', 'del final_events', etc.
        pass

    # Cleanup logic:
    # On Windows, we can't delete the directory if files are open.
    # We need to ensure all memmaps are closed.
    # This is tricky in Python.
    # We will try to delete, if fails, print warning.
    try:
        del depth_events
    except: pass
    try:
        del trade_events
    except: pass
    try:
        del merged_events
    except: pass
    try:
        del final_events
    except: pass
    try:
        del data
    except: pass
    
    # Force GC
    import gc
    gc.collect()
    
    try:
        shutil.rmtree(work_dir)
    except Exception as e:
        print(f"警告: 无法清理临时目录 {work_dir}: {e}")

    return np.empty(0, event_dtype) # Return empty to avoid accessing closed memmap

def main():
    parser = argparse.ArgumentParser(
        description='将OKX市场数据转换为HftBacktest格式 (Memmap版)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('files', nargs='+', help='输入文件路径（支持通配符）')
    parser.add_argument('-o', '--output', required=True, help='输出文件路径（.npz格式）')
    parser.add_argument('--buffer-size', type=int, default=500_000_000, help='缓冲区大小（已忽略）')
    parser.add_argument('--feed-latency', type=float, default=0, help='人工喂送延迟（纳秒）')
    parser.add_argument('--base-latency', type=float, default=0, help='基础延迟修正值（纳秒）')
    parser.add_argument('--simulated-latency', type=float, default=None, help='模拟延迟')
    parser.add_argument('--num-processes', type=int, default=None, help='使用的进程数')
    parser.add_argument('--use-random-latency', action='store_true', help='使用随机延迟')
    parser.add_argument('--latency-mu', type=float, default=0.0, help='对数正态分布 mu')
    parser.add_argument('--latency-sigma', type=float, default=1.0, help='对数正态分布 sigma')
    parser.add_argument('--random-seed', type=int, default=42, help='随机种子')

    args = parser.parse_args()

    all_files = []
    for pattern in args.files:
        matched = glob.glob(pattern)
        if not matched:
            print(f"警告: 未找到匹配的文件: {pattern}", file=sys.stderr)
        all_files.extend(matched)

    if not all_files:
        print("错误: 未找到任何文件", file=sys.stderr)
        sys.exit(1)

    try:
        convert(
            all_files,
            output_filename=args.output,
            buffer_size=args.buffer_size,
            feed_latency=args.feed_latency,
            base_latency=args.base_latency,
            simulated_latency=args.simulated_latency,
            num_processes=args.num_processes,
            latency_mu=args.latency_mu,
            latency_sigma=args.latency_sigma,
            use_random_latency=args.use_random_latency,
            random_seed=args.random_seed
        )
        print("转换完成！")
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
