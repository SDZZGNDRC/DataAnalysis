import sys
import os
import json
import tempfile
import time
from pathlib import Path
from typing import List, Optional
from multiprocessing import Pool, cpu_count
import py7zr
from tqdm import tqdm
from DataFile.data_name import DataName

def unzip_7z(args: tuple[Path, Path]) -> Path:
    """解压单个7z文件到临时目录，返回解压后的json文件路径"""
    zip_path, temp_dir = args
    with py7zr.SevenZipFile(zip_path, mode='r') as z:
        z.extractall(temp_dir)
    # 返回解压后的json文件路径（与7z同名，后缀不同）
    return temp_dir / zip_path.name.replace('.7z', '.json')

def find_files_by_prefix(directory: Path, prefix: str) -> List[Path]:
    """查找指定目录下符合prefix的所有7z文件"""
    files = []
    for file in directory.glob('*.7z'):
        try:
            data_name = DataName(file.name)
            if data_name.prefix == prefix:
                files.append(file)
        except ValueError:
            continue
    return files

def process_files(input_dir: Path, output_path: Path, prefix: str, temp_dir: Path):
    """主处理函数"""
    # 1. 查找文件
    files = find_files_by_prefix(input_dir, prefix)
    if not files:
        print(f"Error: No files found with prefix '{prefix}' in {input_dir}")
        sys.exit(1)

    # 2. 确保临时目录存在
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # 3. 解压文件（带进度显示）
    print(f"Unzipping {len(files)} files...")
    json_files = []
    start_time = time.time()
    
    with Pool(cpu_count()) as pool:
        args = [(f, temp_dir) for f in files]
        for json_file in tqdm(pool.imap(unzip_7z, args), total=len(files)):
            json_files.append(json_file)
    
    # 4. 计算解压耗时
    unzip_time = time.time() - start_time
    avg_time_per_file = unzip_time / len(files) if files else 0
    
    # 5. 调用aggregate_books处理
    print(f"\nProcessing {len(json_files)} JSON files...")
    from DataFile.aggregate_books import aggregate_books
    aggregate_books(json_files, output_path)
    
    # 6. 输出统计信息
    print(f"\nCompleted!")
    print(f"Total files processed: {len(files)}")
    print(f"Total time: {time.time() - start_time:.2f}s")
    print(f"Average time per file: {avg_time_per_file:.2f}s")

def main():
    if len(sys.argv) != 5:
        print("Usage: python process_books.py <input_directory> <output_file> <prefix> <temp_dir>")
        print("Arguments:")
        print("  input_directory: Path to directory containing 7z files")
        print("  output_file: Path for the merged output JSON file")
        print("  prefix: File prefix to match (e.g. 'OKX-Books-BTC-USD-SWAP-')")
        print("  temp_dir: Path for temporary directory")
        sys.exit(1)
    
    input_dir = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    prefix = sys.argv[3]
    temp_dir = Path(sys.argv[4])
    
    if not input_dir.exists():
        print(f"Error: Input directory {input_dir} does not exist")
        sys.exit(1)
    
    process_files(input_dir, output_path, prefix, temp_dir)

if __name__ == "__main__":
    main()
