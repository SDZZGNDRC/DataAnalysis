import os
import json
import argparse
import random
from typing import List, Union, Optional
import jsonschema
from jsonschema import validate, ValidationError
import multiprocessing
from multiprocessing import Pool, Manager

def find_json_files(path: str, recursive: bool = False) -> List[str]:
    """查找指定路径下的JSON文件"""
    json_files = []
    
    if os.path.isfile(path):
        if path.endswith('.json'):
            return [path]
        return []
    
    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
        if not recursive:
            break
            
    return json_files

def validate_single_file(json_file: str, schema: Optional[dict] = None) -> tuple[bool, str]:
    """验证单个JSON文件
    
    Args:
        json_file: JSON文件路径
        schema: 可选，用于验证的JSON Schema
        
    Returns:
        tuple: (是否有效, 错误信息)
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if schema:
            validate(instance=data, schema=schema)
            print(f"✅ Valid JSON (schema): {json_file}")
        else:
            print(f"✅ Valid JSON: {json_file}")
        return True, ""
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"❌ Invalid JSON: {json_file}")
        print(f"   Error: {str(e)}")
        return False, str(e)
    except Exception as e:
        print(f"⚠️  Error reading {json_file}: {str(e)}")
        return False, str(e)

def validate_json_files(
    path: str = ".", 
    recursive: bool = False,
    schema_path: Optional[str] = None,
    processes: Optional[int] = None,
    percentage: float = 1.0
) -> tuple[bool, int, list]:
    """验证指定路径下的JSON文件
    
    Args:
        path: 要验证的文件或目录路径
        recursive: 是否递归检查子目录
        schema_path: 可选，用于验证JSON Schema的路径
        processes: 可选，进程池大小，默认使用CPU核心数
        percentage: 验证文件的百分比(0-1)，默认1.0(100%)
        
    Returns:
        tuple: (是否全部有效, 有效文件数, 无效文件列表)
    """
    json_files = find_json_files(path, recursive)
    
    # 随机抽样指定百分比的文件
    if percentage < 1.0:
        sample_size = max(1, int(len(json_files) * percentage))
        json_files = random.sample(json_files, sample_size)
        print(f"Randomly selected {sample_size} files ({percentage*100}%) for validation")
    
    schema = None
    if schema_path:
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
        except Exception as e:
            print(f"⚠️  Error loading schema {schema_path}: {str(e)}")
            return False, 0, json_files  # 视为所有文件无效

    if not json_files:
        return True, 0, []

    # 设置进程数，默认为CPU核心数
    num_processes = processes if processes else multiprocessing.cpu_count()
    
    # 使用进程池并行验证
    with Pool(processes=num_processes) as pool:
        # 为每个文件创建验证任务
        tasks = [(file, schema) for file in json_files]
        results = pool.starmap(validate_single_file, tasks)
    
    # 统计结果
    valid_count = sum(1 for is_valid, _ in results if is_valid)
    invalid_files = [json_files[i] for i, (is_valid, _) in enumerate(results) if not is_valid]
    
    print(f"\nValidation complete: {valid_count} valid, {len(invalid_files)} invalid")
    
    return len(invalid_files) == 0, valid_count, invalid_files

def main():
    parser = argparse.ArgumentParser(
        description="JSON文件验证工具",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("path", nargs="?", default=".", 
                      help="要验证的文件或目录路径")
    parser.add_argument("-r", "--recursive", action="store_true",
                      help="递归检查子目录")
    parser.add_argument("-j", "--json", 
                      help="将验证结果输出到指定JSON文件")
    parser.add_argument("-s", "--schema", 
                      help="用于验证的JSON Schema文件路径")
    parser.add_argument("-p", "--processes", type=int,
                      help="并行进程数，默认使用CPU核心数")
    parser.add_argument("--percentage", type=float, default=1.0,
                      help="验证文件的百分比(0-1)，默认1.0(100%%)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.path):
        print(f"错误：路径不存在 '{args.path}'")
        exit(2)
        
    all_valid, valid_count, invalid_files = validate_json_files(
        args.path, args.recursive, args.schema, args.processes, args.percentage
    )
    
    if args.json:
        result = {
            "valid": all_valid,
            "path": args.path,
            "recursive": args.recursive,
            "statistics": {
                "total": len(invalid_files) + valid_count,
                "valid": valid_count,
                "invalid": len(invalid_files)
            },
            "invalid_files": invalid_files
        }
        try:
            with open(args.json, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
            print(f"验证结果已保存到: {args.json}")
        except Exception as e:
            print(f"无法保存结果到文件 {args.json}: {str(e)}")
    
    exit(0 if all_valid else 1)

if __name__ == "__main__":
    main()
