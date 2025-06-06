import os
import argparse
import re
from pathlib import Path

def count_files(root_dir):
    """统计指定目录及其子目录中的7z文件和非7z文件的数量"""
    seven_zip_count = 0
    non_seven_zip_count = 0
    
    try:
        # 遍历根目录及其子目录
        for root, _, files in os.walk(root_dir):
            for file in files:
                # 检查文件扩展名是否为.7z（不区分大小写）
                if file.lower().endswith('.7z'):
                    seven_zip_count += 1
                else:
                    non_seven_zip_count += 1
                    
        return seven_zip_count, non_seven_zip_count
    
    except Exception as e:
        print(f"错误: 遍历目录时发生错误 - {e}")
        return 0, 0

def main():
    # 设置命令行参数解析
    parser = argparse.ArgumentParser(description="统计目录中7z文件和非7z文件的数量（支持正则表达式匹配多个目录）")
    parser.add_argument('root_pattern', type=str, help="要统计的根目录路径正则表达式，如 'test/CASE.*' 或 'E:/datapool/2025-05-.*'")
    args = parser.parse_args()

    pattern = args.root_pattern
    # 判断是否为绝对路径正则
    if os.path.isabs(pattern):
        # 拆分为父目录和正则部分
        parent = os.path.dirname(pattern)
        regex_str = os.path.basename(pattern)
        parent_path = Path(parent)
        if not parent_path.exists() or not parent_path.is_dir():
            print(f"错误: 父目录 '{parent}' 不存在或不是目录")
            return
        regex = re.compile(regex_str)
        matched_dirs = [d for d in parent_path.iterdir() if d.is_dir() and regex.fullmatch(d.name)]
    else:
        cwd = Path('.')
        regex = re.compile(pattern)
        matched_dirs = [d for d in cwd.iterdir() if d.is_dir() and regex.fullmatch(str(d))]

    if not matched_dirs:
        print(f"错误: 未找到匹配正则 '{pattern}' 的目录")
        return

    total_seven_zip = 0
    total_non_seven_zip = 0
    for root_dir in matched_dirs:
        seven_zip_count, non_seven_zip_count = count_files(root_dir)
        total_seven_zip += seven_zip_count
        total_non_seven_zip += non_seven_zip_count
        print(f"统计结果 for {root_dir}:")
        print(f"7z 文件数量: {seven_zip_count}")
        print(f"非7z 文件数量: {non_seven_zip_count}")
        print(f"总文件数量: {seven_zip_count + non_seven_zip_count}\n")

    print("==== 总计 ====")
    print(f"7z 文件总数: {total_seven_zip}")
    print(f"非7z 文件总数: {total_non_seven_zip}")
    print(f"总文件数: {total_seven_zip + total_non_seven_zip}")

if __name__ == "__main__":
    main()