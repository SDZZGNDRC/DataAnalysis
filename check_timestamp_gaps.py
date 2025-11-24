#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
时间戳断层检查工具

检查HftBacktest数据文件中的时间戳断层现象，即前一个时间戳和后一个时间戳相差太大。
支持检查交易所时间戳(exch_ts)和本地时间戳(local_ts)的断层。
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Tuple, Dict, Any

import numpy as np
try:
    from hftbacktest.types import EXCH_EVENT, LOCAL_EVENT
except ImportError:
    # Fallback if hftbacktest is not installed
    EXCH_EVENT = 1 << 31
    LOCAL_EVENT = 1 << 30


def load_data_file(file_path: str) -> np.ndarray:
    """
    加载HftBacktest数据文件
    
    Args:
        file_path: 数据文件路径（.npz格式）
        
    Returns:
        事件数据数组
    """
    try:
        # 尝试使用 mmap_mode='r' 以减少内存使用
        data = np.load(file_path, mmap_mode='r')
        if 'data' not in data:
            raise ValueError(f"文件 {file_path} 中未找到 'data' 字段")
        return data['data']
    except Exception as e:
        # 如果 mmap 失败（例如压缩文件），尝试普通加载
        try:
            data = np.load(file_path)
            if 'data' not in data:
                raise ValueError(f"文件 {file_path} 中未找到 'data' 字段")
            return data['data']
        except Exception as e2:
            raise ValueError(f"无法加载文件 {file_path}: {e} | {e2}")


def check_timestamp_gaps(
    data: np.ndarray,
    threshold_ns: int,
    timestamp_field: str = 'exch_ts',
    chunk_size: int = 10_000_000
) -> Tuple[List[Tuple[int, int, int]], Dict[str, Any]]:
    """
    检查时间戳断层（分块处理以优化内存）
    
    Args:
        data: 事件数据数组
        threshold_ns: 断层阈值（纳秒）
        timestamp_field: 要检查的时间戳字段（'exch_ts' 或 'local_ts'）
        chunk_size: 分块大小
        
    Returns:
        Tuple of (断层列表, 统计信息)
        断层列表格式: [(断层开始索引, 断层结束索引, 断层时长)]
    """
    total_events = len(data)
    if total_events < 2:
        return [], {
            'total_events': total_events,
            'gaps_found': 0,
            'max_gap': 0,
            'min_gap': 0,
            'avg_gap': 0
        }
    
    if timestamp_field not in data.dtype.names:
        raise ValueError(f"数据中不存在字段 '{timestamp_field}'")
    
    gap_details = []
    
    # 统计变量
    max_gap = 0
    min_gap = float('inf')
    sum_gap = 0
    count_gap = 0
    checked_events = 0
    
    # 上一个块的最后一个有效时间戳和索引
    last_ts = None
    last_idx = None
    
    # 分块处理
    for i in range(0, total_events, chunk_size):
        chunk_end = min(i + chunk_size, total_events)
        # 使用切片创建视图，避免复制大量数据
        chunk_data = data[i:chunk_end]
        
        # 创建掩码
        if timestamp_field == 'exch_ts':
            mask = (chunk_data['ev'] & EXCH_EVENT) == EXCH_EVENT
        elif timestamp_field == 'local_ts':
            mask = (chunk_data['ev'] & LOCAL_EVENT) == LOCAL_EVENT
        else:
            mask = np.ones(len(chunk_data), dtype=bool)
            
        # 获取当前块的有效时间戳（这里会产生复制，但仅限于块大小）
        chunk_ts = chunk_data[timestamp_field][mask]
        
        if len(chunk_ts) == 0:
            continue
            
        # 获取当前块的有效索引（全局索引）
        chunk_indices = np.where(mask)[0] + i
        
        checked_events += len(chunk_ts)
        
        # 1. 检查与上一个块的连接处
        if last_ts is not None:
            gap = chunk_ts[0] - last_ts
            if gap > 0: # 确保gap为正
                if gap > threshold_ns:
                    gap_details.append((last_idx, chunk_indices[0], gap))
                
                max_gap = max(max_gap, gap)
                min_gap = min(min_gap, gap)
                sum_gap += gap
                count_gap += 1
        
        # 2. 检查块内部
        if len(chunk_ts) > 1:
            current_gaps = chunk_ts[1:] - chunk_ts[:-1]
            
            if len(current_gaps) > 0:
                # 更新统计
                chunk_max = np.max(current_gaps)
                chunk_min = np.min(current_gaps)
                chunk_sum = np.sum(current_gaps)
                
                max_gap = max(max_gap, chunk_max)
                min_gap = min(min_gap, chunk_min)
                sum_gap += chunk_sum
                count_gap += len(current_gaps)
                
                # 找出大断层
                large_gap_indices = np.where(current_gaps > threshold_ns)[0]
                for idx in large_gap_indices:
                    # idx 是 chunk_ts 中的索引
                    # 断层在 chunk_ts[idx] 和 chunk_ts[idx+1] 之间
                    # 对应的全局索引是 chunk_indices[idx] 和 chunk_indices[idx+1]
                    gap_details.append((chunk_indices[idx], chunk_indices[idx+1], current_gaps[idx]))
        
        # 更新 last_ts
        last_ts = chunk_ts[-1]
        last_idx = chunk_indices[-1]
    
    # 整理统计结果
    if count_gap == 0:
        min_gap = 0
    
    stats = {
        'total_events': total_events,
        'checked_events': checked_events,
        'gaps_found': len(gap_details),
        'max_gap': max_gap,
        'min_gap': min_gap if min_gap != float('inf') else 0,
        'avg_gap': (sum_gap / count_gap) if count_gap > 0 else 0,
        # 移除 median 和 std 以节省内存
        'median_gap': 0,
        'std_gap': 0
    }
    
    return gap_details, stats


def format_timestamp_ns(timestamp_ns: int) -> str:
    """
    格式化时间戳为可读格式
    
    Args:
        timestamp_ns: 纳秒时间戳
        
    Returns:
        格式化后的时间字符串
    """
    # 转换为毫秒
    timestamp_ms = timestamp_ns / 1_000_000
    
    # 转换为秒
    seconds = timestamp_ms / 1000
    
    # 转换为可读格式
    if seconds < 60:
        return f"{seconds:.3f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f}min"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"


def format_gap_duration(gap_ns: int) -> str:
    """
    格式化断层时长
    
    Args:
        gap_ns: 断层时长（纳秒）
        
    Returns:
        格式化后的时长字符串
    """
    if gap_ns < 1_000_000:  # < 1ms
        return f"{gap_ns}ns"
    elif gap_ns < 1_000_000_000:  # < 1s
        ms = gap_ns / 1_000_000
        return f"{ms:.3f}ms"
    else:  # >= 1s
        seconds = gap_ns / 1_000_000_000
        return f"{seconds:.3f}s"


def print_gap_report(
    file_path: str,
    gap_details: List[Tuple[int, int, int]],
    stats: Dict[str, Any],
    threshold_ns: int,
    timestamp_field: str,
    verbose: bool = False
) -> None:
    """
    打印断层报告
    
    Args:
        file_path: 文件路径
        gap_details: 断层详细信息
        stats: 统计信息
        threshold_ns: 断层阈值
        timestamp_field: 检查的时间戳字段
        verbose: 是否显示详细信息
    """
    print(f"\n{'='*80}")
    print(f"文件: {file_path}")
    print(f"检查字段: {timestamp_field}")
    print(f"断层阈值: {format_gap_duration(threshold_ns)}")
    print(f"{'='*80}")
    
    print(f"总事件数: {stats['total_events']:,}")
    if 'checked_events' in stats:
        print(f"检查事件数: {stats['checked_events']:,} (过滤后)")
    print(f"断层数量: {stats['gaps_found']:,}")
    print(f"最大间隔: {format_gap_duration(int(stats['max_gap']))}")
    print(f"最小间隔: {format_gap_duration(int(stats['min_gap']))}")
    print(f"平均间隔: {format_gap_duration(int(stats['avg_gap']))}")
    if stats.get('median_gap', 0) > 0:
        print(f"中位数间隔: {format_gap_duration(int(stats['median_gap']))}")
    if stats.get('std_gap', 0) > 0:
        print(f"间隔标准差: {format_gap_duration(int(stats['std_gap']))}")
    
    if gap_details and verbose:
        print(f"\n详细断层信息:")
        print(f"{'索引':<8} {'开始时间':<20} {'结束时间':<20} {'断层时长':<15}")
        print(f"{'-'*65}")
        
        for start_idx, end_idx, gap_duration in gap_details[:20]:  # 只显示前20个
            start_ts = format_timestamp_ns(stats['total_events'] if start_idx >= len(gap_details) else gap_details[0][2])
            end_ts = format_timestamp_ns(stats['total_events'] if end_idx >= len(gap_details) else gap_details[0][2])
            print(f"{start_idx:<8} {start_ts:<20} {end_ts:<20} {format_gap_duration(gap_duration):<15}")
        
        if len(gap_details) > 20:
            print(f"... 还有 {len(gap_details) - 20} 个断层未显示")
    
    if gap_details:
        print(f"\n[WARN] 发现 {len(gap_details)} 个超过阈值的断层!")
    else:
        print(f"\n[OK] 未发现超过阈值的断层")


def find_data_files(directory: str, recursive: bool = False) -> List[str]:
    """
    查找数据文件
    
    Args:
        directory: 目录路径
        recursive: 是否递归查找
        
    Returns:
        数据文件路径列表
    """
    path = Path(directory)
    
    if recursive:
        files = list(path.rglob('*.npz'))
    else:
        files = list(path.glob('*.npz'))
    
    return [str(f) for f in files if f.is_file()]


def main():
    """命令行入口点"""
    parser = argparse.ArgumentParser(
        description='检查HftBacktest数据文件中的时间戳断层现象',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s data.npz --threshold 1000000
  %(prog)s data.npz --threshold 5000000 --timestamp-field local_ts
  %(prog)s ./data_directory --recursive --threshold 1000000 --verbose
  %(prog)s data1.npz data2.npz --threshold 2000000 --output report.txt

时间戳单位:
  阈值单位为纳秒，常用值参考:
    - 1毫秒 = 1,000,000 纳秒
    - 1秒 = 1,000,000,000 纳秒
    - 1分钟 = 60,000,000,000 纳秒
        """
    )
    
    parser.add_argument(
        'files',
        nargs='+',
        help='数据文件路径或目录（支持通配符）'
    )
    
    parser.add_argument(
        '-t', '--threshold',
        type=int,
        required=True,
        help='断层阈值（纳秒）'
    )
    
    parser.add_argument(
        '--timestamp-field',
        choices=['exch_ts', 'local_ts'],
        default='exch_ts',
        help='要检查的时间戳字段（默认: exch_ts）'
    )
    
    parser.add_argument(
        '--recursive',
        action='store_true',
        help='递归查找目录中的文件'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='显示详细断层信息'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='输出报告到文件'
    )
    
    args = parser.parse_args()
    
    # 收集所有数据文件
    data_files = []
    for file_pattern in args.files:
        if os.path.isdir(file_pattern):
            # 如果是目录，查找其中的.npz文件
            files = find_data_files(file_pattern, args.recursive)
            data_files.extend(files)
        else:
            # 如果是文件模式，使用glob匹配
            import glob
            matched_files = glob.glob(file_pattern)
            data_files.extend([f for f in matched_files if f.endswith('.npz')])
    
    # 去重
    data_files = list(set(data_files))
    
    if not data_files:
        print("错误: 未找到任何.npz数据文件", file=sys.stderr)
        sys.exit(1)
    
    print(f"找到 {len(data_files)} 个数据文件")
    
    # 检查每个文件
    all_results = []
    for file_path in sorted(data_files):
        try:
            print(f"\n处理文件: {file_path}")
            data = load_data_file(file_path)
            gap_details, stats = check_timestamp_gaps(data, args.threshold, args.timestamp_field)
            print_gap_report(file_path, gap_details, stats, args.threshold, args.timestamp_field, args.verbose)
            all_results.append((file_path, gap_details, stats))
        except Exception as e:
            print(f"错误: 处理文件 {file_path} 时出错: {e}", file=sys.stderr)
            continue
    
    # 输出汇总报告
    if len(all_results) > 1:
        print(f"\n{'='*80}")
        print("汇总报告")
        print(f"{'='*80}")
        
        total_gaps = sum(len(gaps) for _, gaps, _ in all_results)
        total_events = sum(stats['total_events'] for _, _, stats in all_results)
        
        print(f"总文件数: {len(all_results)}")
        print(f"总事件数: {total_events:,}")
        print(f"总断层数: {total_gaps:,}")
        
        if total_gaps > 0:
            print(f"\n[WARN] 在所有文件中发现 {total_gaps} 个超过阈值的断层!")
        else:
            print(f"\n[OK] 所有文件均未发现超过阈值的断层")
    
    # 输出到文件
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write("时间戳断层检查报告\n")
                f.write("=" * 50 + "\n\n")
                
                for file_path, gap_details, stats in all_results:
                    f.write(f"文件: {file_path}\n")
                    f.write(f"总事件数: {stats['total_events']:,}\n")
                    f.write(f"断层数量: {len(gap_details):,}\n")
                    f.write(f"最大间隔: {format_gap_duration(int(stats['max_gap']))}\n")
                    f.write(f"平均间隔: {format_gap_duration(int(stats['avg_gap']))}\n")
                    
                    if gap_details:
                        f.write("断层详情:\n")
                        for start_idx, end_idx, gap_duration in gap_details:
                            f.write(f"  索引 {start_idx}-{end_idx}: {format_gap_duration(gap_duration)}\n")
                    
                    f.write("\n" + "-" * 50 + "\n\n")
                
                print(f"\n报告已保存到: {args.output}")
        except Exception as e:
            print(f"警告: 无法保存报告到文件 {args.output}: {e}", file=sys.stderr)


if __name__ == '__main__':
    main()