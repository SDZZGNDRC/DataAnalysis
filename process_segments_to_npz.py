#!/usr/bin/env python3
"""
根据 merge_segments.py 生成的 CSV 文件，为每个 segment 生成 npz 文件。
对于每个 segment，使用 find_trades_by_timestamp.py 查找对应的 trades 文件，
然后使用 hftbacktest_okx.py 将 books 和 trades 文件合并为 npz。
"""

import argparse
import csv
import subprocess
import sys
import os
import tempfile
import shutil
import py7zr
import psutil
from pathlib import Path
from typing import List, Optional

# 尝试导入 hftbacktest_okx 的 convert 函数
try:
    from hftbacktest_okx import convert
    HAS_CONVERT = True
except ImportError:
    HAS_CONVERT = False
    print("警告: 无法导入 hftbacktest_okx.convert，将使用子进程调用。")

def run_find_trades(start_ts: int, end_ts: int, trades_dir: str, num_processes: int = None) -> List[str]:
    """
    调用 find_trades_by_timestamp.py 获取覆盖时间范围的 trades 文件列表。
    返回文件路径列表。
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
        tmp_csv = tmp.name
    try:
        cmd = [
            sys.executable, 'find_trades_by_timestamp.py',
            str(start_ts), str(end_ts), trades_dir,
            '--save-csv',
            '--output', tmp_csv,
        ]
        if num_processes is not None:
            cmd.extend(['--num-processes', str(num_processes)])
        print(f"运行命令: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"find_trades_by_timestamp.py 失败: {result.stderr}")
            return []
        # 读取 CSV
        files = []
        # 增加字段大小限制，避免 field larger than field limit 错误
        csv.field_size_limit(10000000)  # 10 MB
        with open(tmp_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                files.append(row['file_path'])
        print(f"找到 {len(files)} 个 trades 文件")
        return files
    finally:
        if os.path.exists(tmp_csv):
            os.unlink(tmp_csv)

def parse_covered_files(covered_files_str: str) -> List[str]:
    """解析分号分隔的文件路径字符串，返回列表"""
    if not covered_files_str:
        return []
    return [f.strip() for f in covered_files_str.split(';') if f.strip()]

def generate_npz_via_subprocess(books_files: List[str], trades_files: List[str], output_npz: str,
                                feed_latency: float, base_latency: float, simulated_latency: Optional[float],
                                num_processes: int, use_random_latency: bool, latency_mu: float,
                                latency_sigma: float, random_seed: int, tmp_dir: str = None) -> bool:
    """
    通过子进程调用 hftbacktest_okx.py 生成 npz 文件。
    """
    # 构建文件列表参数
    all_files = books_files + trades_files
    # 检查文件是否存在
    for f in all_files:
        if not os.path.exists(f):
            print(f"警告: 文件不存在 {f}")
            return False
    cmd = [
        sys.executable, 'hftbacktest_okx.py',
        '-o', output_npz,
        '--feed-latency', str(feed_latency),
        '--base-latency', str(base_latency),
    ]
    if simulated_latency is not None:
        cmd.extend(['--simulated-latency', str(simulated_latency)])
    if num_processes is not None:
        cmd.extend(['--num-processes', str(num_processes)])
    if use_random_latency:
        cmd.append('--use-random-latency')
        cmd.extend(['--latency-mu', str(latency_mu)])
        cmd.extend(['--latency-sigma', str(latency_sigma)])
        cmd.extend(['--random-seed', str(random_seed)])
    if tmp_dir:
        cmd.extend(['--tmp-dir', tmp_dir])
    cmd.extend(all_files)
    print(f"运行 hftbacktest_okx.py: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"hftbacktest_okx.py 失败: {result.stderr}")
        return False
    return True

def generate_npz_via_import(books_files: List[str], trades_files: List[str], output_npz: str,
                             feed_latency: float, base_latency: float, simulated_latency: Optional[float],
                             num_processes: int, use_random_latency: bool, latency_mu: float,
                             latency_sigma: float, random_seed: int, tmp_dir: str = None,
                             start_ts: int = None, end_ts: int = None) -> bool:
    """
    直接导入 convert 函数生成 npz 文件。
    """
    try:
        from hftbacktest_okx_mmap import convert
    except ImportError:
        return False
    all_files = books_files + trades_files
    
    # 统计所有文件解压后的总大小
    total_uncompressed_size = 0
    for f in all_files:
        if not os.path.exists(f):
            print(f"警告: 文件不存在 {f}")
            return False
        try:
            with py7zr.SevenZipFile(f, mode='r') as z:
                for info in z.list():
                    total_uncompressed_size += info.uncompressed
        except Exception as e:
            print(f"无法读取 7z 文件信息 {f}: {e}")
            return False

    # 检查内存限制
    try:
        mem = psutil.virtual_memory()
        available_mem = mem.available
        # 预留一些缓冲，例如使用 90% 的可用内存
        limit = available_mem * 0.9
        
        print(f"预估解压后总大小: {total_uncompressed_size / (1024**3):.2f} GB, 可用内存: {available_mem / (1024**3):.2f} GB")
        
        if total_uncompressed_size > limit:
            print(f"错误: 解压后总大小超过内存限制！")
            return False
    except Exception as e:
        print(f"无法获取内存信息，跳过内存检查: {e}")

    try:
        convert(
            all_files,
            output_filename=output_npz,
            feed_latency=feed_latency,
            base_latency=base_latency,
            simulated_latency=simulated_latency,
            num_processes=num_processes,
            latency_mu=latency_mu,
            latency_sigma=latency_sigma,
            use_random_latency=use_random_latency,
            random_seed=random_seed,
            tmp_dir=tmp_dir,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        return True
    except Exception as e:
        print(f"转换失败: {e}")
        return False

def process_segment(row: dict, index: int, args):
    """处理单个 segment"""
    start_ts = int(row['start_timestamp'])
    end_ts = int(row['end_timestamp'])
    covered_files_str = row['covered_files']
    books_files = parse_covered_files(covered_files_str)
    if not books_files:
        print(f"Segment {index}: 没有 books 文件，跳过")
        return False
    
    # 确定输出文件名
    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)
        output_filename = f"segment_{index}_{start_ts}_{end_ts}.npz"
        output_path = os.path.join(args.output_dir, output_filename)
    else:
        output_path = f"segment_{index}_{start_ts}_{end_ts}.npz"
    
    if os.path.exists(output_path) and args.skip_existing:
        print(f"Segment {index}: 输出文件已存在，跳过")
        return True
    
    # 查找 trades 文件
    trades_files = run_find_trades(start_ts, end_ts, args.trades_dir, args.num_processes)
    if not trades_files:
        print(f"Segment {index}: 未找到 trades 文件，可能跳过或仅使用 books")
        # 根据需求决定是否继续，这里我们继续但警告
        # 如果要求必须有 trades，可以返回 False
        if args.require_trades:
            print("需要 trades 文件但未找到，跳过")
            return False
    
    # 生成 npz
    print(f"Segment {index}: 生成 {output_path}")
    success = False
    if HAS_CONVERT and not args.force_subprocess:
        success = generate_npz_via_import(
            books_files, trades_files, output_path,
            args.feed_latency, args.base_latency, args.simulated_latency,
            args.num_processes, args.use_random_latency, args.latency_mu,
            args.latency_sigma, args.random_seed, args.tmp_dir,
            start_ts, end_ts
        )
    # if not success:
    #     success = generate_npz_via_subprocess(
    #         books_files, trades_files, output_path,
    #         args.feed_latency, args.base_latency, args.simulated_latency,
    #         args.num_processes, args.use_random_latency, args.latency_mu,
    #         args.latency_sigma, args.random_seed, args.tmp_dir
    #     )
    if success:
        print(f"Segment {index}: 成功生成 {output_path}")
    else:
        print(f"Segment {index}: 生成失败")
    return success

def main():
    parser = argparse.ArgumentParser(description="从 merged_segments.csv 生成 npz 文件")
    parser.add_argument('--csv', default='merged_segments.csv', help='输入的 CSV 文件路径')
    parser.add_argument('--trades-dir', required=True, help='交易数据目录（包含 .7z 文件）')
    parser.add_argument('--output-dir', default='.', help='输出目录')
    parser.add_argument('--feed-latency', type=float, default=200000000, help='喂送延迟（纳秒）')
    parser.add_argument('--base-latency', type=float, default=100000000, help='基础延迟（纳秒）')
    parser.add_argument('--simulated-latency', type=float, help='模拟延迟（纳秒）')
    parser.add_argument('--num-processes', type=int, help='进程数')
    parser.add_argument('--use-random-latency', action='store_true', help='使用随机延迟')
    parser.add_argument('--latency-mu', type=float, default=-9.71, help='对数正态分布 mu')
    parser.add_argument('--latency-sigma', type=float, default=1.0, help='对数正态分布 sigma')
    parser.add_argument('--random-seed', type=int, default=42, help='随机种子')
    parser.add_argument('--skip-existing', action='store_true', help='跳过已存在的输出文件')
    parser.add_argument('--require-trades', action='store_true', help='必须要有 trades 文件')
    parser.add_argument('--force-subprocess', action='store_true', help='强制使用子进程')
    parser.add_argument('--tmp-dir', type=str, default=None, help='临时文件存储目录')
    args = parser.parse_args()
    
    if not os.path.exists(args.csv):
        print(f"错误: CSV 文件不存在 {args.csv}")
        sys.exit(1)
    
    if not os.path.isdir(args.trades_dir):
        print(f"错误: 交易目录不存在 {args.trades_dir}")
        sys.exit(1)
    
    # 读取 CSV
    rows = []
    # 增加字段大小限制，避免 field larger than field limit 错误
    csv.field_size_limit(10000000)  # 10 MB
    with open(args.csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    
    print(f"共读取 {len(rows)} 个 segments")
    
    success_count = 0
    for i, row in enumerate(rows):
        print(f"处理 segment {i+1}/{len(rows)}: start={row['start_timestamp']}, end={row['end_timestamp']}")
        if process_segment(row, i, args):
            success_count += 1
    
    print(f"完成: 成功 {success_count}/{len(rows)}")

if __name__ == '__main__':
    main()