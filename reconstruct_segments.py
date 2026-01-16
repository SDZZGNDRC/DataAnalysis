import argparse
import json
from pathlib import Path
import multiprocessing
import os
import tempfile
from typing import List, Tuple, Dict
from tqdm import tqdm
import py7zr
import csv
from datetime import datetime

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

def timestamp_to_datetime(timestamp: int) -> str:
    """Convert millisecond timestamp to 'yyyy-mm-dd hh:mm' format."""
    if timestamp is None:
        return ""
    dt = datetime.fromtimestamp(timestamp / 1000)
    return dt.strftime('%Y-%m-%d %H:%M')

def merge_pairs(pairs: List[Tuple]) -> List[Tuple]:
    """
    Merge pairs using bitmap for file coverage.
    Input Tuple structure: (prevSeqId, seqId, file_bitmap, start_ts, end_ts, startSeqId)
    Output Tuple structure: (prevSeqId, seqId, file_bitmap, start_ts, end_ts, startSeqId)
    """
    # Sort by prevSeqId
    # x[0] is prevSeqId
    pairs.sort(key=lambda x: x[0])
    segments = []

    for pair in pairs:
        # Unpack standard structure - now with 6 elements
        p, s, bitmap, start_ts, end_ts, startSeqId = pair
            
        if p == -1:
            # New chain starts - use current startSeqId
            segments.append((p, s, bitmap, start_ts, end_ts, startSeqId))
        else:
            # Try to merge with existing segments
            merged = False
            # Optimization: Only look at the last few segments for performance in mostly sequential data
            # But strictly speaking we should iterate. For reverse searching, iterating backwards is better.
            for i in range(len(segments) - 1, -1, -1):
                seg = segments[i]
                seg_p, seg_s, seg_bitmap, seg_start_ts, seg_end_ts, seg_startSeqId = seg
                
                if p == seg_s:
                    # Case: Existing Segment ends where New Pair starts -> Append to end
                    # Merge Bitmaps
                    new_bitmap = seg_bitmap | bitmap
                    
                    # Update timestamps: take min of starts, max of ends (ignoring Nones if possible)
                    new_start_ts = seg_start_ts if seg_start_ts is not None else start_ts
                    new_end_ts = end_ts if end_ts is not None else seg_end_ts
                    
                    # Keep the original startSeqId from the segment
                    segments[i] = (seg_p, s, new_bitmap, new_start_ts, new_end_ts, seg_startSeqId)
                    merged = True
                    break
                elif s == seg_p:
                    # Case: New Pair ends where Existing Segment starts -> Prepend to start
                    # (Less common in sequential processing but possible)
                    new_bitmap = bitmap | seg_bitmap
                    
                    new_start_ts = start_ts if start_ts is not None else seg_start_ts
                    new_end_ts = seg_end_ts if seg_end_ts is not None else end_ts
                    
                    # Use the current pair's startSeqId as the new startSeqId (since we're prepending)
                    segments[i] = (p, seg_s, new_bitmap, new_start_ts, new_end_ts, startSeqId)
                    merged = True
                    break
            
            if not merged:
                # New segment - use current startSeqId
                segments.append((p, s, bitmap, start_ts, end_ts, startSeqId))
    return segments

def validate_segments_no_overlap(segments: List[Tuple]) -> bool:
    """
    验证segments中的时间区间没有重叠
    每个segment的结构: (prevSeqId, seqId, bitmap, start_ts, end_ts, startSeqId)
    返回True表示没有重叠，False表示有重叠
    """
    if not segments:
        return True
    
    # 过滤掉时间戳为None的segments
    valid_segments = []
    for seg in segments:
        p, s, bitmap, start_ts, end_ts, startSeqId = seg
        if start_ts is not None and end_ts is not None:
            valid_segments.append((start_ts, end_ts, seg))
    
    # 按开始时间排序
    valid_segments.sort(key=lambda x: x[0])
    
    # 检查重叠
    for i in range(1, len(valid_segments)):
        prev_start, prev_end, prev_seg = valid_segments[i-1]
        curr_start, curr_end, curr_seg = valid_segments[i]
        
        # 如果前一个区间的结束时间大于等于当前区间的开始时间，说明有重叠
        if prev_end >= curr_start:
            print(f"Overlap detected:")
            print(f"  Segment 1: start_ts={prev_start}, end_ts={prev_end}")
            print(f"  Segment 2: start_ts={curr_start}, end_ts={curr_end}")
            print(f"  Overlap range: {curr_start} to {min(prev_end, curr_end)}")
            return False
    
    return True

def process_file(args: Tuple[Path, int]) -> dict:
    """
    args: (file_path, global_file_index)
    Returns a dict with metadata, segments (with bitmap), and errors.
    """
    file_path, global_file_index = args
    errors = []
    # Standardized Structure: (prevSeqId, seqId, file_bitmap, start_ts, end_ts)
    pairs = []
    
    # Pre-calculate bitmap for this file (it's just a single bit set)
    current_bitmap = 1 << global_file_index
    
    try:
        if not py7zr.is_7zfile(file_path):
             return {'file': file_path.name, 'status': 'error', 'msg': 'Not a 7z file'}

        with py7zr.SevenZipFile(file_path, mode='r') as z:
            all_fnames = z.getnames()
            # Sort internal files just in case
            sorted_fnames = sorted(all_fnames)
            
            with tempfile.TemporaryDirectory() as tmpdirname:
                for fname in sorted_fnames:
                    z.extract(targets=[fname], path=tmpdirname)
                    extracted_path = Path(tmpdirname) / fname
                    
                    if not extracted_path.exists():
                        continue
                        
                    try:
                        with open(extracted_path, 'rb') as f:
                            file_bytes = f.read()
                    except Exception as e:
                        errors.append(f"Read Error in {fname}: {str(e)}")
                        continue
                    finally:
                        # Clean up the file immediately to save space
                        if extracted_path.exists():
                            os.remove(extracted_path)
                    
                    try:
                        if HAS_ORJSON:
                            data = orjson.loads(file_bytes)
                        else:
                            data = json.loads(file_bytes)
                    except Exception as e:
                        errors.append(f"JSON Decode Error in {fname}: {str(e)}")
                        continue
                    
                    items = data.get('data', [])
                    
                    if not isinstance(items, list):
                        continue
                        
                    for item in items:
                        inner_data_list = item.get('data')
                        if not inner_data_list or not isinstance(inner_data_list, list):
                            continue
                        
                        inner_obj = inner_data_list[0]
                        curr_seqId = inner_obj.get('seqId')
                        curr_prevSeqId = inner_obj.get('prevSeqId')
                        curr_ts = inner_obj.get('ts')
                        
                        if curr_seqId is None or curr_prevSeqId is None:
                            continue
                        
                        ts_val = None
                        if curr_ts is not None:
                            try:
                                ts_val = int(curr_ts)
                            except ValueError:
                                pass
                        
                        pairs.append((curr_prevSeqId, curr_seqId, current_bitmap, ts_val, ts_val, curr_seqId))

    except Exception as e:
        return {'file': file_path.name, 'status': 'error', 'msg': str(e)}

    # Local merge to reduce data transfer overhead
    # Inside a single file, the bitmap is identical, so logic still holds
    segments = merge_pairs(pairs)
    
    return {
        'file': file_path.name,
        'status': 'ok',
        'segments': segments,
        'errors': errors
    }

def worker_shim(args):
    return process_file(args)

def decode_bitmap(bitmap: int, all_filenames: List[str]) -> str:
    """Convert integer bitmap back to list of filenames"""
    covered = []
    total_files = len(all_filenames)
    
    # Optimization: iterate only length of bitstring
    # But since we need to map to index, iterating range is straightforward
    # Or iterate while bitmap > 0
    idx = 0
    temp_bmp = bitmap
    while temp_bmp > 0:
        if temp_bmp & 1:
            if idx < total_files:
                covered.append(all_filenames[idx])
            else:
                covered.append(f"Unknown_Index_{idx}")
        temp_bmp >>= 1
        idx += 1
        
    return ";".join(covered)

def main():
    parser = argparse.ArgumentParser(description="Reconstruct segments from 7z files using Bitmap coverage.")
    parser.add_argument("--dir", type=str, required=True, nargs='+',
                        help="Directory or directories containing the 7z files.")
    parser.add_argument("--num-processes", type=int, default=os.cpu_count(),
                        help=f"Number of processes to use. Defaults to {os.cpu_count()}.")
    parser.add_argument("--output", type=str, default="reconstructed_segments.csv", help="Output CSV file.")
    args = parser.parse_args()

    # 1. Collect all files
    data_dirs = [Path(d) for d in args.dir]
    files = []
    for data_dir in data_dirs:
        if not data_dir.is_dir():
            print(f"Error: {data_dir} is not a valid directory.")
            exit(1)
        # Use rglob and ensure sorted order for consistent indexing
        dir_files = sorted(list(data_dir.rglob("*.7z")), key=lambda x: x.name)
        files.extend(dir_files)
        print(f"Found {len(dir_files)} 7z files in {data_dir}.")
    
    if not files:
        print(f"No 7z files found.")
        exit(1)
    
    # Global sort to ensure index 0 is the earliest file (by name)
    files.sort(key=lambda x: x.name)
    all_filenames = [str(f.resolve()) for f in files]
    
    print(f"Total: Found {len(files)} 7z files. Starting processing...")
    
    # Prepare arguments: List of (Path, index)
    worker_args = [(f, i) for i, f in enumerate(files)]
    
    results = []
    with multiprocessing.Pool(processes=args.num_processes) as pool:
        with tqdm(total=len(files), desc="Processing 7z files") as pbar:
            # Note: passing worker_args now
            for res in pool.imap_unordered(worker_shim, worker_args):
                results.append(res)
                pbar.update(1)
    
    all_segments = []
    
    print("Collecting segments from workers...")
    for res in results:
        if res['status'] == 'error':
            print(f"Error processing {res['file']}: {res['msg']}")
            continue
        
        if 'segments' in res:
            all_segments.extend(res['segments'])

    if all_segments:
        print(f"Merging {len(all_segments)} initial segments...")
        # Sort by start_prevSeqId before merging
        all_segments.sort(key=lambda x: x[0])
        
        # Global Merge
        final_segments = merge_pairs(all_segments)
        
        # 检验final_segments中每一个segments的start_ts和end_ts组成的区间集合没有任何重叠
        if not validate_segments_no_overlap(final_segments):
            print("ERROR: Found overlapping segments in final_segments!")
            return
        
        print(f"Writing {len(final_segments)} final segments to {args.output}...")
        
        # 按照start_ts进行升序排序
        final_segments.sort(key=lambda x: x[3] if x[3] is not None else 0)
        
        with open(args.output, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'prevSeqId', 'seqId', 'startSeqId',
                'start_timestamp', 'start_datetime',
                'end_timestamp', 'end_datetime',
                'duration_hours',
                'covered_files'
            ])
            
            for seg in tqdm(final_segments, desc="Writing CSV"):
                # seg: (prev, seq, bitmap, start_ts, end_ts, startSeqId)
                p, s, bitmap, start_ts, end_ts, startSeqId = seg
                
                # Decode Bitmap to Filenames
                covered_str = decode_bitmap(bitmap, all_filenames)
                
                duration_str = ""
                if start_ts is not None and end_ts is not None:
                    duration = (end_ts - start_ts) / (1000 * 3600)
                    duration_str = f"{duration:.4f}"

                writer.writerow([
                    p,
                    s,
                    startSeqId,
                    start_ts,
                    timestamp_to_datetime(start_ts),
                    end_ts,
                    timestamp_to_datetime(end_ts),
                    duration_str,
                    covered_str
                ])
        print("Done.")
    else:
        print("No segments found.")

if __name__ == "__main__":
    main()