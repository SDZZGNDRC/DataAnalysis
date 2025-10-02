#!/usr/bin/env python3
"""
查找包含 snapshot 的 OKX 数据文件

支持以下功能：
- 处理7z压缩文件自动解压
- 支持通配符文件匹配
- 支持目录递归读取所有文件
- 使用多进程并行处理
- 处理完成后删除解压的JSON文件
"""

import argparse
import glob
import os
import shutil
import sys
import tempfile
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import List

try:
    import orjson
    def json_loads(data):
        return orjson.loads(data)
except ImportError:
    import json
    def json_loads(data):
        return json.loads(data)
    print("警告: 未安装orjson，使用标准json库。建议运行: pip install orjson")

try:
    from tqdm import tqdm
except ImportError:
    print("警告: 未安装tqdm，无法显示进度条。建议运行: pip install tqdm")
    tqdm = None


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


def has_snapshot(json_file: str) -> bool:
    """
    检查JSON文件是否包含snapshot。

    Args:
        json_file: JSON文件路径

    Returns:
        是否包含snapshot
    """
    with open(json_file, 'rb') as f:
        data = json_loads(f.read())

    for item in data['data']:
        if item['action'] == 'snapshot':
            return True
    return False


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


def process_files_worker(file_chunk: List[str]) -> List[str]:
    """
    多进程worker函数，处理一批文件，找出包含snapshot的文件。

    Args:
        file_chunk: 文件路径列表

    Returns:
        包含snapshot的文件路径列表
    """
    snapshot_files = []
    temp_dir = tempfile.mkdtemp(prefix='snapshot_worker_')

    try:
        for file_path in file_chunk:
            try:
                if file_path.endswith('.7z'):
                    json_file = extract_7z(file_path, temp_dir)
                else:
                    json_file = file_path

                if has_snapshot(json_file):
                    snapshot_files.append(file_path)

                # 删除解压的JSON文件
                if file_path.endswith('.7z') and os.path.exists(json_file):
                    os.remove(json_file)

            except Exception as e:
                print(f"处理文件 {file_path} 时出错: {e}")
                continue

    finally:
        # 清理临时目录
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    return snapshot_files


def create_batches(file_list: List[str], num_procs: int) -> List[List[str]]:
    """
    将文件列表分成批次。

    Args:
        file_list: 文件路径列表
        num_procs: 进程数

    Returns:
        文件批次列表
    """
    if not file_list:
        return []

    batch_size = max(1, len(file_list) // num_procs)
    batches = []
    for i in range(0, len(file_list), batch_size):
        batches.append(file_list[i:i + batch_size])
    return batches


def main():
    parser = argparse.ArgumentParser(description='查找包含 snapshot 的 OKX 数据文件')
    parser.add_argument('path', help='文件路径模式，支持通配符或目录')
    parser.add_argument('--processes', type=int, default=cpu_count(),
                       help=f'使用的进程数（默认：{cpu_count()}）')

    args = parser.parse_args()

    # 收集文件
    patterns = [args.path]
    all_files = collect_files(patterns)

    if not all_files:
        print("未找到任何 .7z 或 .json 文件")
        return

    print(f"找到 {len(all_files)} 个文件，开始处理...")

    # 分批处理
    batches = create_batches(all_files, args.processes)

    # 使用多进程处理
    with Pool(args.processes) as pool:
        if tqdm is not None:
            pbar = tqdm(total=len(all_files), desc="处理文件")
            results = []
            for result in pool.imap_unordered(process_files_worker, batches):
                results.append(result)
                # 估算已处理的文件数（每个批次平均文件数）
                batch_size = len(all_files) // len(batches)
                pbar.update(batch_size)
            pbar.close()
        else:
            results = pool.map(process_files_worker, batches)

    # 合并结果
    snapshot_files = []
    for result in results:
        snapshot_files.extend(result)

    # 排序并输出
    snapshot_files.sort()
    print(f"\n找到 {len(snapshot_files)} 个包含 snapshot 的文件：")
    for file_path in snapshot_files:
        print(file_path)


if __name__ == '__main__':
    main()