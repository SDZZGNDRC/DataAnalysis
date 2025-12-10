import argparse
import multiprocessing
import os
import json
import subprocess
import shutil
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import py7zr
from pytest import skip
from tqdm import tqdm
import sys

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

from DataFile.data_name import DataName

def get_timestamp(item: Dict[str, Any]) -> Optional[int]:
    try:
        # Structure: item -> data -> [0] -> ts
        # ts is string in schema, but we need int for comparison
        ts_str = item['data'][0]['ts']
        return int(ts_str)
    except (KeyError, IndexError, ValueError, TypeError):
        return None

def get_seq_ids(item: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    try:
        inner = item['data'][0]
        return inner.get('seqId'), inner.get('prevSeqId')
    except (KeyError, IndexError, TypeError):
        return None, None

def process_file(args: Tuple[Path, int]) -> Dict[str, Any]:
    """
    Worker function to process a single 7z file.
    Args:
        args: (file_path, file_index)
    Returns:
        Dict containing:
            - file_index: int
            - items: List[Dict] (the data points)
            - seq_map: Dict[int, int] (seqId -> index in items)
            - error: str (optional)
    """
    file_path, file_index = args
    items = []
    seq_map = {}
    
    try:
        if not py7zr.is_7zfile(file_path):
            return {'file_index': file_index, 'error': f"{file_path} is not a 7z file"}

        with py7zr.SevenZipFile(file_path, mode='r') as z:
            # Each 7z file contains only one json file, find and read the first json file
            all_fnames = sorted(z.getnames())
            
            json_fname = None
            for fname in all_fnames:
                if fname.endswith('.json'):
                    json_fname = fname
                    break
            
            if json_fname is None:
                return {'file_index': file_index, 'error': f"No JSON file found in {file_path}"}
                
            content_dict = z.read(targets=[json_fname])
            if json_fname not in content_dict:
                return {'file_index': file_index, 'error': f"Failed to read {json_fname} from {file_path}"}
            
            file_bytes = content_dict[json_fname].read()
            
            try:
                if HAS_ORJSON:
                    data_obj = orjson.loads(file_bytes)
                else:
                    data_obj = json.loads(file_bytes)
            except Exception as e:
                return {'file_index': file_index, 'error': f"Error parsing {json_fname} in {file_path}: {e}"}
            
            # Extract data points
            # Schema: root -> data (List)
            current_items = data_obj.get('data', [])
            if not isinstance(current_items, list):
                return {'file_index': file_index, 'error': f"Invalid data format in {json_fname}"}
            
            # Append to local list and build map
            start_idx = len(items)
            for i, item in enumerate(current_items):
                items.append(item)
                seq_id, _ = get_seq_ids(item)
                if seq_id is not None:
                    # Global index within this 7z file processing context
                    seq_map[seq_id] = start_idx + i

    except Exception as e:
        return {'file_index': file_index, 'error': str(e)}

    return {
        'file_index': file_index,
        'items': items,
        'seq_map': seq_map
    }

def worker_shim(args):
    return process_file(args)

def main():
    parser = argparse.ArgumentParser(description="Concat Books data from 7z files based on seqId chain.")
    parser.add_argument("files", nargs='+', type=Path, help="List of 7z files to process (can be semicolon-separated string)")
    parser.add_argument("--startSeqId", type=int, required=True, help="The sequence ID to start collecting from")
    parser.add_argument("--start_timestamp", type=int, required=True, help="Start timestamp (ms)")
    parser.add_argument("--end_timestamp", type=int, required=True, help="End timestamp (ms)")
    parser.add_argument("--output", type=str, default="concatenated_books.json", help="Output JSON filename")
    parser.add_argument("--num-processes", type=int, default=os.cpu_count(), help="Number of worker processes")

    args = parser.parse_args()

    # Handle semicolon-separated file list
    if len(args.files) == 1 and ';' in str(args.files[0]):
        # Split by semicolon and convert to Path objects
        file_paths = [Path(f.strip()) for f in str(args.files[0]).split(';') if f.strip()]
    else:
        file_paths = args.files

    files = sorted([f.resolve() for f in file_paths], key=lambda x: x.name)
    
    if not files:
        print("No files provided.")
        return

    print(f"Processing {len(files)} files...")

    # 2. Multiprocessing Step
    worker_args = [(f, i) for i, f in enumerate(files)]
    results = []
    
    with multiprocessing.Pool(processes=args.num_processes) as pool:
        with tqdm(total=len(files), desc="Reading files") as pbar:
            for res in pool.imap_unordered(worker_shim, worker_args):
                results.append(res)
                pbar.update(1)

    # 3. Aggregate Results
    # Sort results by file_index to reconstruct the global order
    results.sort(key=lambda x: x['file_index'])
    
    global_items = []
    # We need a way to quickly find the start index.
    # We can check the maps as we merge.
    
    start_global_index = -1
    current_global_offset = 0
    
    # To optimize lookup across all files, we can merge the maps with offset
    # But since we only need to find the startSeqId once, we can just search for it.
    # For subsequent lookups (prevSeqId), we might need a global map if the chain jumps across files (which it does).
    # However, the requirement says: "prevSeqId points to previous collected data point's seqId".
    # This implies we are traversing the chain.
    # If the data is mostly sequential, we can just iterate.
    # But if there are gaps or out-of-order packets, we need random access.
    # The prompt says: "establish seqId -> index mapping... return to main process... main process concatenates... traverse global list"
    
    # So we should build a global map.
    global_seq_map = {}
    
    print("Aggregating data...")
    
    for res in results:
        if 'error' in res:
            print(f"Error in file index {res['file_index']}: {res['error']}")
            continue
            
        items = res['items']
        seq_map = res['seq_map']
        
        # Merge map with offset
        for seq_id, local_idx in seq_map.items():
            global_seq_map[seq_id] = current_global_offset + local_idx
            
        global_items.extend(items)
        current_global_offset += len(items)

    if args.startSeqId not in global_seq_map:
        print(f"Error: startSeqId {args.startSeqId} not found in any file.")
        return

    start_global_index = global_seq_map[args.startSeqId]
    print(f"Found startSeqId at global index {start_global_index}. Total items: {len(global_items)}")

    # 4. Traverse and Collect
    print(f"Traversing {len(global_items)} items to reconstruct chain starting from {args.startSeqId}...")
    
    start_index = global_seq_map[args.startSeqId]
    print(f"Ignoring previous {start_index} items.")
    
    collected_seq_ids = {args.startSeqId}
    result_list = []
    start_item = global_items[start_index]
    result_list.append(start_item)
    skip_count = 0
    for i in tqdm(range(start_index + 1, len(global_items)), desc="Collecting chain"):
        item = global_items[i]
        seq_id, prev_seq_id = get_seq_ids(item)
        
        # If already collected, skip (avoid duplicates)
        if seq_id in collected_seq_ids:
            print(f"Duplicate seqId {seq_id} at index {i}.")
            raise RuntimeError("Duplicate seqId detected in data.")
            
        # Check timestamp limit
        ts = get_timestamp(item)
        if ts is not None and ts > args.end_timestamp:
            skip_count += 1
            continue

        # Check if directly linked to a collected item
        if prev_seq_id in collected_seq_ids:
            result_list.append(item)
            collected_seq_ids.add(seq_id)
            continue
            
        # Backtracking check
        # We need to see if we can reach a collected item OR startSeqId by following prevSeqId
        cursor = prev_seq_id
        is_connected = False
        
        # We use a set for visited in this backtrack to avoid cycles
        visited_in_path = set()
        
        while True:
            if cursor in collected_seq_ids:
                is_connected = True
                break
            
            # Also check if we hit startSeqId (even if not collected yet)
            if cursor == args.startSeqId:
                is_connected = True
                break
                
            if cursor == -1: # End of chain
                print(f'Should not reach here: prevSeqId -1 without connecting to collected or startSeqId')
                raise RuntimeError("Unexpected prevSeqId -1 without connection")
                
            if cursor in visited_in_path: # Cycle detected
                raise RuntimeError("Cycle detected in prevSeqId chain")
            visited_in_path.add(cursor)
            
            if cursor not in global_seq_map:
                # Cannot backtrack further
                break
                
            # Move to previous
            prev_idx = global_seq_map[cursor]
            prev_item_obj = global_items[prev_idx]
            _, cursor = get_seq_ids(prev_item_obj)
            
        if is_connected:
            result_list.append(item)
            collected_seq_ids.add(seq_id)
        else:
            skip_count += 1

    print(f"Collected {len(result_list)} items.")
    print(f"Skipped {skip_count} items beyond end_timestamp or unconnected.")

    # 5. Output
    if not result_list:
        print("No items collected.")
        return

    # Calculate min/max ts for elapsedTime
    min_ts = float('inf')
    max_ts = float('-inf')
    
    for item in result_list:
        ts = get_timestamp(item)
        if ts is not None:
            if ts < min_ts: min_ts = ts
            if ts > max_ts: max_ts = ts
            
    if min_ts == float('inf'):
        min_ts = args.start_timestamp
        max_ts = args.end_timestamp

    output_obj = {
        "elapsedTime": [int(min_ts), int(max_ts)],
        "data": result_list,
        "extend": {
            "sorted": False,
            "true_elapsedTime": True
        }
    }
    
    # Determine output filename based on prefix and timestamps
    first_file = files[0].name
    try:
        dn = DataName(first_file)
        prefix = dn.prefix
    except Exception as e:
        print(f"Warning: Could not parse prefix from {first_file}: {e}")
        # Fallback: use default prefix "OKX-Books-unknown"
        prefix = "OKX-Books-unknown"

    # Use actual timestamps (min_ts, max_ts) for filename
    output_filename = f"{prefix}-{int(min_ts)}-{int(max_ts)}.json"
    output_path = Path(output_filename)
    
    print(f"Writing to {output_path}...")
    with open(output_path, 'wb') as f:
        if HAS_ORJSON:
            f.write(orjson.dumps(output_obj))
        else:
            f.write(json.dumps(output_obj).encode('utf-8'))
            
    # Compress
    archive_path = output_path.with_suffix('.7z')
    print(f"Compressing to {archive_path}...")
    
    # Try using subprocess 7z command first (faster)
    if shutil.which('7z'):
        try:
            # Use 7z command line tool for better performance
            cmd = ['7z', 'a', '-t7z', '-mx=3', '-mmt=on', str(archive_path), str(output_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print("7z compression completed successfully using subprocess")
        except subprocess.CalledProcessError as e:
            print(f"7z command failed: {e}")
            print(f"stderr: {e.stderr}")
            print("Falling back to py7zr...")
            # Fallback to py7zr
            with py7zr.SevenZipFile(archive_path, 'w') as z:
                z.write(output_path, arcname=output_path.name)
        except Exception as e:
            print(f"Unexpected error with 7z command: {e}")
            print("Falling back to py7zr...")
            with py7zr.SevenZipFile(archive_path, 'w') as z:
                z.write(output_path, arcname=output_path.name)
    else:
        print("7z command not found in PATH, using py7zr...")
        with py7zr.SevenZipFile(archive_path, 'w') as z:
            z.write(output_path, arcname=output_path.name)
        
    # Delete the JSON file after successful compression
    try:
        os.remove(output_path)
        print(f"Deleted JSON file: {output_path}")
    except OSError as e:
        print(f"Warning: Could not delete JSON file {output_path}: {e}")

    print("Done.")

if __name__ == "__main__":
    main()