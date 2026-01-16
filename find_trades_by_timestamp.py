import argparse
import multiprocessing
import os
import json
import csv
import sys
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import py7zr
from tqdm import tqdm
from DataFile.data_name import DataName

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

def get_timestamp(item: Dict[str, Any]) -> Optional[int]:
    """
    Extract timestamp from a trade data item.
    Schema: item -> data -> [0] -> ts
    """
    try:
        # Structure based on DataSchema/schema/jsonschema/Trades.json
        # item is one element of the root 'data' array
        # item['data'] is an array of size 1
        # item['data'][0] is the object containing 'ts'
        ts_str = item['data'][0]['ts']
        return int(ts_str)
    except (KeyError, IndexError, ValueError, TypeError):
        return None

def process_file(args: Tuple[Path, int]) -> Dict[str, Any]:
    """
    Worker function to process a single 7z file.
    Args:
        args: (file_path, file_index)
    Returns:
        Dict containing:
            - file_index: int
            - file_path: str
            - start_timestamp: int (min)
            - end_timestamp: int (max)
            - error: str (optional)
    """
    file_path, file_index = args
    min_ts = float('inf')
    max_ts = float('-inf')
    
    try:
        if not py7zr.is_7zfile(file_path):
            return {'file_index': file_index, 'file_path': str(file_path), 'error': f"Not a 7z file"}

        with py7zr.SevenZipFile(file_path, mode='r') as z:
            all_fnames = z.getnames()
            json_fname = None
            for fname in all_fnames:
                if fname.endswith('.json'):
                    json_fname = fname
                    break
            
            if json_fname is None:
                return {'file_index': file_index, 'file_path': str(file_path), 'error': "No JSON file found"}
                
            with tempfile.TemporaryDirectory() as tmpdirname:
                z.extract(path=tmpdirname, targets=[json_fname])
                extracted_path = os.path.join(tmpdirname, json_fname)
                
                if not os.path.exists(extracted_path):
                     return {'file_index': file_index, 'file_path': str(file_path), 'error': f"Failed to extract {json_fname}"}
                
                with open(extracted_path, 'rb') as f:
                    file_bytes = f.read()
            
            try:
                if HAS_ORJSON:
                    data_obj = orjson.loads(file_bytes)
                else:
                    data_obj = json.loads(file_bytes)
            except Exception as e:
                return {'file_index': file_index, 'file_path': str(file_path), 'error': f"JSON parse error: {e}"}
            
            # Extract data points
            items = data_obj.get('data', [])
            if not isinstance(items, list):
                return {'file_index': file_index, 'file_path': str(file_path), 'error': "Invalid data format"}
            
            for item in items:
                ts = get_timestamp(item)
                if ts is not None:
                    if ts < min_ts:
                        min_ts = ts
                    if ts > max_ts:
                        max_ts = ts

    except Exception as e:
        return {'file_index': file_index, 'file_path': str(file_path), 'error': str(e)}

    if min_ts == float('inf') or max_ts == float('-inf'):
         return {'file_index': file_index, 'file_path': str(file_path), 'error': "No valid timestamps found"}

    return {
        'file_index': file_index,
        'file_path': str(file_path),
        'start_timestamp': int(min_ts),
        'end_timestamp': int(max_ts)
    }

def worker_shim(args):
    return process_file(args)

def main():
    parser = argparse.ArgumentParser(description="Find 7z files containing Trades data within a timestamp range.")
    parser.add_argument("startTimestamp", type=int, help="Start timestamp (ms)")
    parser.add_argument("endTimestamp", type=int, help="End timestamp (ms)")
    parser.add_argument("source_dir", type=str, help="Directory containing 7z files")
    parser.add_argument("--output", type=str, default="found_trades_files.csv", help="Output CSV filename")
    parser.add_argument("--num-processes", type=int, default=os.cpu_count(), help="Number of worker processes")
    parser.add_argument("--save-csv", action="store_true", help="If specified, save result to CSV. Otherwise, merge files into one 7z file.")

    args = parser.parse_args()
    
    source_path = Path(args.source_dir)
    if not source_path.exists():
        print(f"Error: Source directory {args.source_dir} does not exist.")
        return

    # 1. Recursively find and sort 7z files
    print(f"Scanning {source_path} for 7z files...")
    files = sorted([f for f in source_path.rglob("*.7z")], key=lambda x: x.name)
    
    if not files:
        print("No 7z files found.")
        return

    print(f"Found {len(files)} files. Processing...")

    # 2. Multiprocessing
    worker_args = [(f, i) for i, f in enumerate(files)]
    results = []
    
    with multiprocessing.Pool(processes=args.num_processes) as pool:
        with tqdm(total=len(files), desc="Processing files") as pbar:
            for res in pool.imap_unordered(worker_shim, worker_args):
                results.append(res)
                pbar.update(1)

    # Filter out errors and sort by file_index (which corresponds to name sort)
    valid_results = []
    for res in results:
        if 'error' in res:
            print(f"Warning: File {res['file_path']} skipped: {res['error']}")
            pass
        else:
            valid_results.append(res)
    
    valid_results.sort(key=lambda x: x['file_index'])
    
    if not valid_results:
        print("No valid data found in files.")
        return

    
    start_idx = -1
    
    candidates_start = [i for i, r in enumerate(valid_results) if r['start_timestamp'] < args.startTimestamp]
    if not candidates_start:
        print(f"No file found with start_timestamp < {args.startTimestamp}")
        return
    
    best_start_idx = -1
    max_start_val = -1
    
    for idx in candidates_start:
        val = valid_results[idx]['start_timestamp']
        if val > max_start_val:
            max_start_val = val
            best_start_idx = idx
        elif val == max_start_val:
            best_start_idx = idx
            
    
    candidates_end = [i for i, r in enumerate(valid_results) if r['end_timestamp'] > args.endTimestamp]
    if not candidates_end:
        print(f"No file found with end_timestamp > {args.endTimestamp}")
        return

    best_end_idx = -1
    min_end_val = float('inf')
    
    for idx in candidates_end:
        val = valid_results[idx]['end_timestamp']
        if val < min_end_val:
            min_end_val = val
            best_end_idx = idx
        elif val == min_end_val:
            # If equal, prefer the one earlier in the list to minimize set size
            if best_end_idx == -1 or idx < best_end_idx:
                best_end_idx = idx

    
    intersection = set(candidates_start) & set(candidates_end)
    if intersection:
        # 提示：存在至少一个文件完全覆盖目标时间范围
        files = [valid_results[i]['file_path'] for i in intersection]
        print(f"提示：发现 {len(intersection)} 个文件完全覆盖目标时间范围，建议直接使用这些文件：")
        for f in files[:5]:  # 最多显示前5个
            print(f"  {f}")
        if len(files) > 5:
            print(f"  ... 以及另外 {len(files) - 5} 个文件")
    
    final_set = []
    if best_start_idx <= best_end_idx:
        final_set = valid_results[best_start_idx : best_end_idx + 1]
    else:
        # 错误：无法找到连续的时间范围，数据可能不连续
        print(f"错误：best_start_idx ({best_start_idx}) > best_end_idx ({best_end_idx})，时间范围不连续。")
        print("可能原因：文件时间戳重叠或顺序错误。")
        sys.exit(1)

    print(f"Selected {len(final_set)} files.")
    print(f"Set range: {final_set[0]['file_path']} ... {final_set[-1]['file_path']}")
    print(f"Set Time Range: {min(f['start_timestamp'] for f in final_set)} - {max(f['end_timestamp'] for f in final_set)}")

    if args.save_csv:
        # 4. Save to CSV
        keys = ['file_index', 'file_path', 'start_timestamp', 'end_timestamp']
        try:
            with open(args.output, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                for item in final_set:
                    # Filter keys to match CSV
                    row = {k: item[k] for k in keys}
                    writer.writerow(row)
            print(f"Saved result to {args.output}")
        except Exception as e:
            print(f"Error saving CSV: {e}")
    else:
        # Merge files
        print(f"Merging {len(final_set)} files...")
        
        merged_items = []
        # Calculate overall timestamps from the scanned results in final_set
        overall_min_ts = min(f['start_timestamp'] for f in final_set)
        overall_max_ts = max(f['end_timestamp'] for f in final_set)
        
        for item in tqdm(final_set, desc="Merging content"):
            fpath = item['file_path']
            try:
                with py7zr.SevenZipFile(fpath, mode='r') as z:
                    all_fnames = z.getnames()
                    json_fname = next((f for f in all_fnames if f.endswith('.json')), None)
                    if not json_fname:
                        print(f"Warning: No JSON found in {fpath}")
                        continue
                        
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        z.extract(path=tmpdirname, targets=[json_fname])
                        extracted_path = os.path.join(tmpdirname, json_fname)
                        
                        if not os.path.exists(extracted_path):
                            print(f"Warning: Failed to extract {json_fname} from {fpath}")
                            continue

                        with open(extracted_path, 'rb') as f:
                            file_bytes = f.read()
                    
                    if HAS_ORJSON:
                        data_obj = orjson.loads(file_bytes)
                    else:
                        data_obj = json.loads(file_bytes)
                        
                    # Merge data
                    current_items = data_obj.get('data', [])
                    merged_items.extend(current_items)
                        
            except Exception as e:
                print(f"Error processing {fpath} for merge: {e}")
        
        if not merged_items:
            print("Error: No data merged.")
            return

        # Construct merged object
        merged_obj = {
            "elapsedTime": [int(overall_min_ts), int(overall_max_ts)],
            "data": merged_items
        }
        
        # Generate filename
        first_file_path = final_set[0]['file_path']
        try:
            dn = DataName(Path(first_file_path).name)
            prefix = dn.prefix
            
            new_base_name = f"{prefix}-{int(overall_min_ts)}-{int(overall_max_ts)}"
            new_json_name = f"{new_base_name}.json"
            new_7z_name = f"{new_base_name}.7z"
            
            print(f"Writing merged JSON to {new_json_name}...")
            with open(new_json_name, 'wb') as f:
                if HAS_ORJSON:
                    f.write(orjson.dumps(merged_obj))
                else:
                    f.write(json.dumps(merged_obj).encode('utf-8'))
            
            print(f"Compressing to {new_7z_name}...")
            with py7zr.SevenZipFile(new_7z_name, 'w') as z:
                z.write(new_json_name, arcname=new_json_name)
                
            print(f"Removing temporary JSON {new_json_name}...")
            os.remove(new_json_name)
            
            print(f"Successfully created {new_7z_name}")
            
        except Exception as e:
            print(f"Error generating output file: {e}")

if __name__ == "__main__":
    main()