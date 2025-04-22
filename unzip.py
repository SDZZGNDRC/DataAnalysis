from typing import Tuple
import glob
import os
import sys
from multiprocessing import Pool
import py7zr
import time
from tqdm import tqdm
import argparse

def unzip_7z(args: Tuple[str, str]):
    zipPath, destPath = args
    with py7zr.SevenZipFile(zipPath, mode='r') as z:
        z.extractall(destPath)

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Extract 7z files with progress tracking',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'input_dir',
        help='Path to the directory containing the 7z files to be extracted'
    )
    parser.add_argument(
        'output_dir',
        help='Path to the directory where the extracted files will be saved'
    )
    parser.add_argument(
        '--prefix',
        default="*",
        help='Prefix of the zip files to process (default: match all prefix)'
    )
    return parser.parse_args()

if __name__ == "__main__":
    # 解析命令行参数
    args = parse_arguments()
    zipDir = args.input_dir
    destDir = args.output_dir
    file_prefix = args.prefix

    # 验证输入目录
    if not os.path.isdir(zipDir):
        print(f'Input directory does not exist: {zipDir}')
        exit(-1)

    # 创建输出目录（如果不存在）
    os.makedirs(destDir, exist_ok=True)
    
    # 查找所有符合前缀的 .7z 文件
    zip_pattern = os.path.join(zipDir, f'{file_prefix}-*-*.7z')
    zipfiles = glob.glob(zip_pattern)
    
    if not zipfiles:
        print(f'Cannot find any zip files with prefix "{file_prefix}" under: {zipDir}')
        exit(-1)

    destfiles = [destDir] * len(zipfiles)
    total_files = len(zipfiles)

    print(f'Found {total_files} files to process...')
    
    # 设置 CPU 核心数和进度条
    cpu_nums = os.cpu_count()
    print(f'Using {cpu_nums} CPU cores')
    
    # 准备参数列表
    args_list = list(zip(zipfiles, destfiles))
    
    # 记录开始时间
    start_time = time.time()
    with tqdm(total=total_files, 
              desc='Extracting',
              unit='file',
              dynamic_ncols=True) as pbar:
        with Pool(cpu_nums) as p:
            # 使用 imap_unordered 处理文件，并在主进程中更新进度条
            for _ in p.imap_unordered(unzip_7z, args_list):
                pbar.update(1)
    
    # 计算总时间和平均速度
    total_time = time.time() - start_time
    avg_speed = total_files / total_time if total_time > 0 else 0
    
    print(f'\nCompleted processing {total_files} files')
    print(f'Total time: {total_time:.2f} seconds')
    print(f'Average speed: {avg_speed:.2f} files/second')