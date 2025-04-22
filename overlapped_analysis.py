import argparse
import glob
import json
import os
from bisect import bisect_left, bisect_right
from typing import List, Dict, Tuple
import multiprocessing
import csv

def parse_filename(filename: str) -> Tuple[int, int]:
    """
    从文件名中解析出 startTs 和 endTs。
    
    Args:
        filename (str): 文件名，格式为 <prefix>-[startTimestamp]-[endTimestamp].json
    
    Returns:
        Tuple[int, int]: 开始时间戳和结束时间戳
    """
    base_name = os.path.basename(filename)
    parts = base_name.split('-')
    if len(parts) >= 3:  # 有前缀
        start_ts = int(parts[-2])
        end_ts = int(parts[-1].split('.')[0])
    else:
        raise ValueError(f"文件名格式错误: {filename}")
    return start_ts, end_ts

def load_file_data(filepath: str) -> Dict:
    """
    加载 JSON 文件内容。
    
    Args:
        filepath (str): 文件路径
    
    Returns:
        Dict: JSON 文件的解析内容
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_ts_list(data: Dict) -> List[int]:
    """
    从 JSON 数据中提取所有数据项的 ts 列表。
    
    Args:
        data (Dict): JSON 数据
    
    Returns:
        List[int]: 数据项的时间戳列表
    """
    return [int(item['data'][0]['ts']) for item in data['data']]

def count_overlapping_items(ts_list1: List[int], ts_list2: List[int], is_sorted: bool) -> int:
    """
    计算两个时间戳列表在重叠时间段内的相同元素数量。
    
    Args:
        ts_list1 (List[int]): 第一个文件的时间戳列表
        ts_list2 (List[int]): 第二个文件的时间戳列表
    
    Returns:
        int: 重叠时间段内相同的时间戳数量
    """
    if not ts_list1 or not ts_list2:
        return 0
    
    if not is_sorted:
        ts_list1 = sorted(ts_list1)
        ts_list2 = sorted(ts_list2)

    def fix_one(ts_list1, ts_list2):
        start = bisect_left(ts_list1, ts_list2[0])
        end = bisect_right(ts_list1, ts_list2[-1])
        return end - start

    return max(fix_one(ts_list1, ts_list2), fix_one(ts_list2, ts_list1))

def analyze_single_file(file1: Dict, file_metadata: List[Dict], cache: Dict = None) -> Dict:
    """
    分析单个文件的时间重叠情况。
    
    Args:
        file1 (Dict): 第一个文件的元数据
        file_metadata (List[Dict]): 所有文件的元数据列表
        cache (Dict, optional): 文件数据缓存
    
    Returns:
        Dict: 该文件的重叠统计数据，包括重叠文件列表
    """
    if cache is None:
        cache = {}
    
    overlap_count = 0
    total_overlapping_items = 0
    overlapping_files = []
    
    # 加载第一个文件的数据
    if file1['filepath'] not in cache:
        cache[file1['filepath']] = load_file_data(file1['filepath'])
    data1 = cache[file1['filepath']]
    ts_list1 = get_ts_list(data1)
    is_sorted1 = data1.get('extend', {}).get('sorted', False)
    
    for file2 in file_metadata:
        if file1['filepath'] == file2['filepath']:
            continue
        
        # 判断时间范围是否重叠
        overlap_start = max(file1['start_ts'], file2['start_ts'])
        overlap_end = min(file1['end_ts'], file2['end_ts'])
        if overlap_start < overlap_end:
            overlap_count += 1
            overlapping_files.append(file2['filepath'])
            
            # 加载第二个文件的数据
            if file2['filepath'] not in cache:
                cache[file2['filepath']] = load_file_data(file2['filepath'])
            data2 = cache[file2['filepath']]
            ts_list2 = get_ts_list(data2)
            is_sorted2 = data2.get('extend', {}).get('sorted', False)
            
            # 计算重叠数据项数量
            overlapping_items = count_overlapping_items(ts_list1, ts_list2, is_sorted1 and is_sorted2)
            total_overlapping_items += overlapping_items
    
    if overlap_count > 0:
        average_overlapping_items = total_overlapping_items / overlap_count
    else:
        average_overlapping_items = 0
    
    return {
        'filepath': file1['filepath'],
        'overlap_count': overlap_count,
        'average_overlapping_items': average_overlapping_items,
        'overlapping_files': overlapping_files
    }

def analyze_files(path: str, prefix: str = None) -> Dict[str, Dict[str, float]]:
    """
    分析 JSON 文件，统计时间重叠情况，使用多进程并行处理。
    
    Args:
        path (str): 文件目录路径
        prefix (str, optional): 文件名前缀，默认为 None，表示匹配任何前缀
    
    Returns:
        Dict[str, Dict[str, float]]: 每个文件的统计数据
    """
    # 查找所有符合格式的文件
    if prefix is None:
        pattern = os.path.join(path, "*-[0-9]*-[0-9]*.json")
    else:
        pattern = os.path.join(path, f"{prefix}-[0-9]*-[0-9]*.json")
    
    files = glob.glob(pattern)
    if not files:
        print("未找到符合格式的 JSON 文件。")
        return {}

    # 提取文件元数据
    file_metadata = []
    for filepath in files:
        try:
            start_ts, end_ts = parse_filename(filepath)
            file_metadata.append({
                'filepath': filepath,
                'start_ts': start_ts,
                'end_ts': end_ts
            })
        except ValueError as e:
            print(f"跳过文件: {filepath}，原因: {e}")
            continue

    # 使用多进程池并行分析
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.starmap(analyze_single_file, [(file1, file_metadata) for file1 in file_metadata])

    # 收集结果
    overlap_stats = {result['filepath']: {
        'overlap_count': result['overlap_count'],
        'average_overlapping_items': result['average_overlapping_items'],
        'overlapping_files': result['overlapping_files']
    } for result in results}

    return overlap_stats

def main():
    """主函数，解析命令行参数并运行分析程序。"""
    parser = argparse.ArgumentParser(description='分析 JSON 文件的时间重叠情况。')
    parser.add_argument('path', help='包含 JSON 文件的目录路径')
    parser.add_argument('prefix', nargs='?', default=None, help='JSON 文件的前缀（可选）')
    parser.add_argument('--csv', help='输出结果到指定的CSV文件')
    parser.add_argument('--json', help='输出每个文件的重叠文件列表到指定的JSON文件')  # 新增参数
    args = parser.parse_args()

    # 运行分析
    stats = analyze_files(args.path, args.prefix)

    # 处理 JSON 输出
    if args.json:
        json_output = {filepath: stat['overlapping_files'] for filepath, stat in stats.items()}
        with open(args.json, 'w', encoding='utf-8') as jsonfile:
            json.dump(json_output, jsonfile, ensure_ascii=False, indent=2)
        print(f"重叠文件列表已写入到 {args.json}")

    # 根据是否指定 CSV 参数决定输出方式
    if args.csv:
        with open(args.csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Filepath', 'Overlap Count', 'Average Overlapping Items'])
            for filepath, stat in stats.items():
                writer.writerow([filepath, stat['overlap_count'], stat['average_overlapping_items']])
        print(f"结果已写入到 {args.csv}")
    else:
        if stats:
            print("\n统计结果：")
            for filepath, stat in stats.items():
                print(f"文件: {filepath}")
                print(f"  重叠文件数量: {stat['overlap_count']}")
                print(f"  平均重叠数据项数量: {stat['average_overlapping_items']:.2f}")
                print()
        else:
            print("没有找到符合格式的 JSON 文件。")

if __name__ == '__main__':
    main()