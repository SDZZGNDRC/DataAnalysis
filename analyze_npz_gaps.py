#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NPZ文件时间间隔分析工具

用于分析多个HftBacktest格式的npz文件之间的时间连续性。
会自动根据文件内部的时间戳对文件进行排序，并计算相邻文件之间的时间间隔。
"""

import argparse
import numpy as np
import os
import sys
import glob
from typing import List, Dict, Tuple, Optional

def format_duration(ns: int) -> str:
    """格式化纳秒时间为可读字符串"""
    if ns < 0:
        return "-" + format_duration(-ns)
    
    if ns < 1000:
        return f"{ns}ns"
    elif ns < 1_000_000:
        return f"{ns/1000:.2f}us"
    elif ns < 1_000_000_000:
        return f"{ns/1_000_000:.2f}ms"
    elif ns < 60_000_000_000:
        return f"{ns/1_000_000_000:.2f}s"
    elif ns < 3600_000_000_000:
        return f"{ns/60_000_000_000:.2f}min"
    else:
        return f"{ns/3600_000_000_000:.2f}h"

def load_file_metadata(file_path: str) -> Optional[Dict]:
    """读取npz文件的首尾时间戳信息"""
    try:
        # 使用 mmap_mode='r' 避免加载整个文件到内存
        # 只需要读取头部和尾部的数据
        data = np.load(file_path, mmap_mode='r')
        
        if 'data' not in data:
            print(f"[WARN] 文件 {file_path} 中未找到 'data' 字段，跳过。")
            return None
        
        events = data['data']
        if len(events) == 0:
            print(f"[WARN] 文件 {file_path} 为空，跳过。")
            return None
            
        # 获取第一个和最后一个事件
        # 注意：这里假设文件内部已经是按时间排序的
        first_event = events[0]
        last_event = events[-1]
        
        # 检查字段是否存在
        has_exch_ts = 'exch_ts' in events.dtype.names
        has_local_ts = 'local_ts' in events.dtype.names
        
        result = {
            'file_path': file_path,
            'count': len(events),
            'size_bytes': os.path.getsize(file_path)
        }
        
        if has_exch_ts:
            result['start_exch_ts'] = int(first_event['exch_ts'])
            result['end_exch_ts'] = int(last_event['exch_ts'])
        
        if has_local_ts:
            result['start_local_ts'] = int(first_event['local_ts'])
            result['end_local_ts'] = int(last_event['local_ts'])
            
        return result
        
    except Exception as e:
        print(f"[ERROR] 读取文件 {file_path} 失败: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(
        description="分析多个NPZ文件之间的时间间隔连续性",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('files', nargs='+', help='NPZ文件列表（支持通配符）')
    parser.add_argument('--sort-by', choices=['exch_ts', 'local_ts', 'name'], default='exch_ts', help='排序依据 (默认: exch_ts)')
    
    args = parser.parse_args()
    
    # 1. 收集文件
    file_list = []
    for pattern in args.files:
        # 处理通配符 (Windows下shell可能不自动展开)
        matched = glob.glob(pattern)
        if not matched:
            # 如果glob没找到，可能是直接的文件路径（或者真的不存在）
            if os.path.exists(pattern):
                matched = [pattern]
            else:
                print(f"[WARN] 未找到匹配的文件: {pattern}")
        file_list.extend(matched)
    
    # 去重并排序路径
    file_list = sorted(list(set(file_list)))
    
    if not file_list:
        print("错误: 未指定有效的文件。")
        sys.exit(1)
        
    print(f"正在扫描 {len(file_list)} 个文件...")
    
    # 2. 读取元数据
    metadata_list = []
    for f in file_list:
        meta = load_file_metadata(f)
        if meta:
            metadata_list.append(meta)
            
    if not metadata_list:
        print("没有可分析的数据。")
        sys.exit(1)

    # 3. 排序
    if args.sort_by == 'name':
        metadata_list.sort(key=lambda x: x['file_path'])
    elif args.sort_by == 'local_ts':
        # 如果没有local_ts字段，回退到exch_ts
        if 'start_local_ts' in metadata_list[0]:
            metadata_list.sort(key=lambda x: x['start_local_ts'])
        else:
            print("[WARN] 数据中没有 local_ts 字段，回退到按 exch_ts 排序")
            metadata_list.sort(key=lambda x: x['start_exch_ts'])
    else: # exch_ts
        if 'start_exch_ts' in metadata_list[0]:
            metadata_list.sort(key=lambda x: x['start_exch_ts'])
        else:
            print("[WARN] 数据中没有 exch_ts 字段，无法排序")
            sys.exit(1)

    # 4. 分析间隔
    print("\n" + "="*120)
    print(f"{'文件名':<40} | {'开始时间 (Exch)':<20} | {'结束时间 (Exch)':<20} | {'数据点数':<10}")
    print("-" * 120)
    
    for meta in metadata_list:
        fname = os.path.basename(meta['file_path'])
        if len(fname) > 38:
            fname = fname[:35] + "..."
        
        start_ts = meta.get('start_exch_ts', 'N/A')
        end_ts = meta.get('end_exch_ts', 'N/A')
        count = meta.get('count', 0)
        
        print(f"{fname:<40} | {start_ts:<20} | {end_ts:<20} | {count:<10}")
        
    print("\n" + "="*120)
    print("文件间隙分析 (基于 exch_ts)")
    print("-" * 120)
    
    gaps = []
    overlap_count = 0
    
    if 'start_exch_ts' not in metadata_list[0]:
        print("无法计算 exch_ts 间隔。")
    else:
        for i in range(1, len(metadata_list)):
            prev = metadata_list[i-1]
            curr = metadata_list[i]
            
            prev_end = prev['end_exch_ts']
            curr_start = curr['start_exch_ts']
            
            gap = curr_start - prev_end
            gaps.append(gap)
            
            prev_name = os.path.basename(prev['file_path'])
            curr_name = os.path.basename(curr['file_path'])
            
            # 缩短文件名以便显示
            if len(prev_name) > 20: prev_name = prev_name[:17] + "..."
            if len(curr_name) > 20: curr_name = curr_name[:17] + "..."
            
            gap_str = format_duration(gap)
            status = ""
            if gap < 0:
                status = "[重叠]"
                overlap_count += 1
            elif gap > 1_000_000_000: # > 1s
                status = "[大间隙]"
            
            print(f"{prev_name:<20} -> {curr_name:<20} : 间隔 = {gap:>15} ns ({gap_str}) {status}")

    # 5. 统计信息
    if gaps:
        gaps_np = np.array(gaps)
        positive_gaps = gaps_np[gaps_np > 0]
        
        print("\n" + "="*120)
        print("统计信息")
        print("-" * 120)
        print(f"文件数量: {len(metadata_list)}")
        print(f"间隔数量: {len(gaps)}")
        print(f"重叠数量: {overlap_count}")
        
        if len(positive_gaps) > 0:
            print(f"\n正向间隔统计 (排除重叠):")
            print(f"最小间隔: {np.min(positive_gaps)} ns ({format_duration(np.min(positive_gaps))})")
            print(f"最大间隔: {np.max(positive_gaps)} ns ({format_duration(np.max(positive_gaps))})")
            print(f"平均间隔: {np.mean(positive_gaps):.2f} ns ({format_duration(int(np.mean(positive_gaps)))})")
            print(f"中位间隔: {np.median(positive_gaps):.2f} ns ({format_duration(int(np.median(positive_gaps)))})")
        
        if overlap_count > 0:
            overlaps = gaps_np[gaps_np < 0]
            print(f"\n重叠统计:")
            print(f"最大重叠: {np.min(overlaps)} ns ({format_duration(np.min(overlaps))}) (绝对值最大)")
            print(f"平均重叠: {np.mean(overlaps):.2f} ns ({format_duration(int(np.mean(overlaps)))})")

        # 总跨度
        total_start = metadata_list[0]['start_exch_ts']
        total_end = metadata_list[-1]['end_exch_ts']
        total_duration = total_end - total_start
        print(f"\n数据总跨度: {total_duration} ns ({format_duration(total_duration)})")
        print(f"开始时间: {total_start}")
        print(f"结束时间: {total_end}")

if __name__ == "__main__":
    main()