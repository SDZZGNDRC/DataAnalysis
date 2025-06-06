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
        nargs='?',
        default=None,
        help='Path to the directory where the extracted files will be saved (ignored if --overwrite is set)'
    )
    parser.add_argument(
        '--prefix',
        default="*",
        help='Prefix of the zip files to process (default: match all prefix)'
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='If set, output_dir will be ignored, and extracted files will be placed in the same directory as the 7z file. The 7z file will be deleted after extraction.'
    )
    args = parser.parse_args()
    # 检查参数合法性
    if not args.overwrite and args.output_dir is None:
        parser.error('the following arguments are required: output_dir (unless --overwrite is set)')
    return args

if __name__ == "__main__":
    # 解析命令行参数
    args = parse_arguments()
    zipDir = args.input_dir
    destDir = args.output_dir
    file_prefix = args.prefix
    overwrite = args.overwrite

    # 验证输入目录
    if not os.path.isdir(zipDir):
        print(f'Input directory does not exist: {zipDir}')
        exit(-1)

    # 创建输出目录（如果不存在），仅在未指定 overwrite 时
    if not overwrite and destDir is not None:
        os.makedirs(destDir, exist_ok=True)
    
    # 递归查找所有符合前缀的 .7z 文件（支持多级子目录）
    zip_pattern = os.path.join(zipDir, '**', f'{file_prefix}*.7z')
    zipfiles = glob.glob(zip_pattern, recursive=True)
    
    if not zipfiles:
        exit(0)

    # 根据 overwrite 参数决定解压目标目录
    if overwrite:
        destfiles = [os.path.dirname(z) for z in zipfiles]
    else:
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
    
    # 如果 overwrite，删除所有已处理的 7z 文件
    if overwrite:
        for z in zipfiles:
            try:
                os.remove(z)
                # print(f"Deleted: {z}")
            except Exception as e:
                print(f"Failed to delete {z}: {e}")
        print(f'Deleted {total_files} .7z files after extraction.')

    # 计算总时间和平均速度
    total_time = time.time() - start_time
    avg_speed = total_files / total_time if total_time > 0 else 0
    
    print(f'\nCompleted processing {total_files} files')
    print(f'Total time: {total_time:.2f} seconds')
    print(f'Average speed: {avg_speed:.2f} files/second')