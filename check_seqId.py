import argparse
import json
from pathlib import Path
import pyarrow.parquet as pq
import multiprocessing
import os
from typing import List, Tuple
from tqdm import tqdm
import py7zr
import csv
from collections import defaultdict
import re
from datetime import datetime
try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

def extract_timestamps_from_filename(file_name: str) -> Tuple[int, int]:
    """
    Extract start and end timestamps from filename.
    Expected format: OKX-Books-BTC-USDT-400-{start_timestamp}-{end_timestamp}.7z
    Returns: (start_timestamp, end_timestamp)
    """
    # Match the timestamp pattern in filename
    pattern = r'(\d{13})-(\d{13})\.7z$'
    match = re.search(pattern, file_name)
    if match:
        start_ts = int(match.group(1))
        end_ts = int(match.group(2))
        return start_ts, end_ts
    return None, None

def timestamp_to_datetime(timestamp: int) -> str:
    """
    Convert millisecond timestamp to 'yyyy-mm-dd hh:mm' format.
    """
    if timestamp is None:
        return ""
    # Convert from milliseconds to seconds
    dt = datetime.fromtimestamp(timestamp / 1000)
    return dt.strftime('%Y-%m-%d %H:%M')

def merge_pairs(pairs: List[Tuple]) -> List[Tuple]:
    """Merge pairs while preserving file and index information"""
    sorted_pairs = sorted(list(set(pairs)), key=lambda x: x[0])
    segments = []
    for pair in sorted_pairs:
        # Normalize inputs
        start_ts, end_ts = None, None
        end_file = None
        
        if len(pair) == 4:
            p, s, file_name, file_index = pair
            end_file = file_name
        elif len(pair) == 5: # (p, s, f, i, end_f)
            p, s, file_name, file_index, end_file = pair
        elif len(pair) == 6:
            p, s, file_name, file_index, start_ts, end_ts = pair
            end_file = file_name
        elif len(pair) == 7:
            p, s, file_name, file_index, start_ts, end_ts, end_file = pair
        else:
            continue
            
        if p == -1: # if prevSeqId is -1, that means a new chain starts
            if start_ts is not None and end_ts is not None:
                segments.append((p, s, file_name, file_index, start_ts, end_ts, end_file))
            else:
                segments.append((p, s, file_name, file_index, end_file))
        else:
            # try to merge with existing segments
            merged = False
            for i in range(len(segments)):
                seg = segments[i]
                # Normalize segment
                seg_start_ts, seg_end_ts = None, None
                seg_end_file = None
                
                if len(seg) == 5:
                    seg_start, seg_end, seg_file, seg_index, seg_end_file = seg
                elif len(seg) == 7:
                    seg_start, seg_end, seg_file, seg_index, seg_start_ts, seg_end_ts, seg_end_file = seg
                else:
                    continue
                    
                if p == seg_end:
                    # extend the segment forward
                    if seg_start_ts is not None and start_ts is not None:
                        segments[i] = (seg_start, s, seg_file, seg_index, seg_start_ts, end_ts, end_file)
                    else:
                        segments[i] = (seg_start, s, seg_file, seg_index, end_file)
                    merged = True
                    break
                elif s == seg_start:
                    # extend the segment backwards
                    if seg_start_ts is not None and start_ts is not None:
                        segments[i] = (p, seg_end, file_name, file_index, start_ts, seg_end_ts, seg_end_file)
                    else:
                        segments[i] = (p, seg_end, file_name, file_index, seg_end_file)
                    merged = True
                    break
            if not merged:
                if start_ts is not None and end_ts is not None:
                    segments.append((p, s, file_name, file_index, start_ts, end_ts, end_file))
                else:
                    segments.append((p, s, file_name, file_index, end_file))
    return segments

def check_seqId_worker_raw(file_path: Path) -> dict:
    """
    Checks seqId and prevSeqId for a 7z file containing JSONs.
    Returns a dict with metadata, segments, and errors.
    Segments are list of (start_prevSeqId, end_seqId, file_name, file_index, start_timestamp, end_timestamp) representing continuous chains.
    """
    errors = []
    pairs = [] # List of (prevSeqId, seqId, file_name, file_index)
    
    # Extract timestamps from filename
    # start_ts, end_ts = extract_timestamps_from_filename(file_path.name)
    
    try:
        if not py7zr.is_7zfile(file_path):
             return {'file': file_path.name, 'status': 'error', 'msg': 'Not a 7z file'}

        with py7zr.SevenZipFile(file_path, mode='r') as z:
            all_fnames = z.getnames()
            sorted_fnames = sorted(all_fnames)
            
            for file_index, fname in enumerate(sorted_fnames):
                # Extract specific file
                content_dict = z.read(targets=[fname])
                if fname not in content_dict:
                    continue
                    
                file_bytes = content_dict[fname].read()
                
                try:
                    if HAS_ORJSON:
                        data = orjson.loads(file_bytes)
                    else:
                        data = json.loads(file_bytes)
                except (json.JSONDecodeError, orjson.JSONDecodeError) as e:
                    errors.append([fname, -1, f"JSON Decode Error: {str(e)}", -1, -1])
                    continue
                
                # Validate structure based on Books.json
                # root -> data (list)
                items = data.get('data', [])
                if not isinstance(items, list):
                    continue
                    
                for i, item in enumerate(items):
                    # item -> data (list of 1) -> {seqId, prevSeqId}
                    inner_data_list = item.get('data')
                    if not inner_data_list or not isinstance(inner_data_list, list):
                        continue
                    
                    inner_obj = inner_data_list[0]
                    curr_seqId = inner_obj.get('seqId')
                    curr_prevSeqId = inner_obj.get('prevSeqId')
                    curr_ts = inner_obj.get('ts')
                    
                    # Skip if seqId/prevSeqId are missing
                    if curr_seqId is None or curr_prevSeqId is None:
                        continue
                    
                    if curr_ts is not None:
                        try:
                            curr_ts = int(curr_ts)
                            pairs.append((curr_prevSeqId, curr_seqId, file_path.name, file_index, curr_ts, curr_ts))
                        except ValueError:
                            pairs.append((curr_prevSeqId, curr_seqId, file_path.name, file_index))
                    else:
                        pairs.append((curr_prevSeqId, curr_seqId, file_path.name, file_index))

    except Exception as e:
        return {'file': file_path.name, 'status': 'error', 'msg': str(e)}

    # Process pairs to form segments
    segments = merge_pairs(pairs)
    
    return {
        'file': file_path.name,
        'status': 'ok',
        'segments': segments,
        'errors': errors
    }

def check_seqId_worker(pf_path: Path, group_index: int, pfs: List[Path]) -> List:
    """
    Checks seqId and prevSeqId for a single row group against the previous one.
    Returns a list of error messages.
    """
    errors = []
    err_candi = []
    try:
        pf = pq.ParquetFile(pf_path)
        # Read current row group
        row_group = pf.read_row_group(group_index, columns=['data'])
        df = row_group.to_pandas()
    except Exception as e:
        return [f"Error reading row group {group_index} from {pf_path.name}: {e}"]

    if df.empty:
        return []

    # Extract seqId and prevSeqId
    try:
        df['prevSeqId'] = df['data'].apply(lambda x: x.get('prevSeqId'))
        df['seqId'] = df['data'].apply(lambda x: x.get('seqId'))
    except Exception as e:
        return [f"Error parsing 'data' column in {pf_path.name}, row_group {group_index}: {e}"]

    # Get the seqId from the last row of the previous row group
    last_seqId = None
    
    prev_df = None
    try:
        if group_index > 0:
            # Previous row group is in the same file
            prev_row_group = pf.read_row_group(group_index - 1, columns=['data'])
            prev_df = prev_row_group.to_pandas()
        else:  # group_index == 0, try previous file
            try:
                current_file_index = pfs.index(pf_path)
                if current_file_index > 0:
                    prev_pf_path = pfs[current_file_index - 1]
                    try:
                        prev_pf = pq.ParquetFile(prev_pf_path)
                        if prev_pf.num_row_groups > 0:
                            # Read last row group of previous file
                            prev_row_group = prev_pf.read_row_group(prev_pf.num_row_groups - 1, columns=['data'])
                            prev_df = prev_row_group.to_pandas()
                    except Exception as e:
                        print(f"Warning: Could not read from previous file {prev_pf_path.name}: {e}")
                        exit(-1)
            except ValueError:
                print(f"Warning: {pf_path.name} not found in the list of files.")
                exit(-1)

    except Exception as e:
        print(f"Warning: Error getting previous row group for {pf_path.name} group {group_index}: {e}")
        exit(-1)

    if prev_df is not None and not prev_df.empty:
        # The data in the last row of the previous dataframe
        last_seqId = prev_df.iloc[-1]['data'].get('seqId')
        del prev_df
        del prev_row_group

    # --- Start Checking ---
    
    # 1. Check first row of current group against last row of previous group
    first_row_prev_seq_id = df.iloc[0]['prevSeqId']
    if first_row_prev_seq_id is not None and first_row_prev_seq_id != -1:
        if last_seqId is not None:
            if first_row_prev_seq_id != last_seqId:
                if not err_candi:
                    err_candi.append([first_row_prev_seq_id,last_seqId,pf_path.name,group_index,0])
                else:
                    if err_candi[-1][1] == first_row_prev_seq_id:
                        err_candi.append([first_row_prev_seq_id,last_seqId,pf_path.name,group_index,0])
                        if err_candi[-1][1] == err_candi[0][0]:
                            err_candi = []
                    else:
                        errors.append(err_candi)
                        err_candi = []
                        err_candi.append([first_row_prev_seq_id,last_seqId,pf_path.name,group_index,0])
                # errors.append(
                #     f"Mismatch at boundary: {pf_path.name} group {group_index} row 0, "
                #     f"prevSeqId ({first_row_prev_seq_id}) != previous file/group's last seqId ({last_seqId})"
                # )
                
                # errors.append((first_row_prev_seq_id,last_seqId))
    
    # 2. Check within the current row group
    for i in range(1, len(df)):
        prev_seq_id = df.iloc[i]['prevSeqId']
        if prev_seq_id is not None and prev_seq_id != -1:
            seq_id_prev_row = df.iloc[i-1]['seqId']
            if prev_seq_id != seq_id_prev_row:
                if not err_candi:
                    err_candi.append([prev_seq_id,seq_id_prev_row,pf_path.name,group_index,i])
                else:
                    if err_candi[-1][1] == prev_seq_id:
                        err_candi.append([prev_seq_id,seq_id_prev_row,pf_path.name,group_index,i])
                        if err_candi[-1][1] == err_candi[0][0]:
                            err_candi = []
                    else:
                        errors.append(err_candi)
                        err_candi = []
                        err_candi.append([prev_seq_id,seq_id_prev_row,pf_path.name,group_index,i])
                # errors.append(
                #     f"Mismatch in {pf_path.name} group {group_index} row {i}: "
                #     f"prevSeqId ({prev_seq_id}) != previous row's seqId ({seq_id_prev_row})"
                # )
    return errors

# Wrapper function for multiprocessing that unpacks arguments
def worker(args):
    return check_seqId_worker(*args)

def worker_raw(file_path):
    return check_seqId_worker_raw(file_path)

def main():
    parser = argparse.ArgumentParser(description="Check sequence IDs in parquet or 7z files using multiprocessing.")
    parser.add_argument("--dir", type=str, required=True, nargs='+',
                        help="Directory or directories containing the files. Multiple directories can be specified.")
    parser.add_argument("--num-processes", type=int, default=os.cpu_count(),
                        help=f"Number of processes to use. Defaults to the number of CPU cores ({os.cpu_count()}).")
    parser.add_argument("--raw", action="store_true", help="Treat input files as 7z files containing JSON.")
    parser.add_argument("--output", type=str, default="seqId_gaps.csv", help="Output CSV file for gaps (only for --raw mode).")
    parser.add_argument("--show_seg", type=str, help="Output CSV file for all segments (only for --raw mode).")
    args = parser.parse_args()

    # Handle multiple directories
    data_dirs = [Path(d) for d in args.dir]
    for data_dir in data_dirs:
        if not data_dir.is_dir():
            print(f"Error: {data_dir} is not a valid directory.")
            exit(1)

    if args.raw:
        # Collect files from all directories
        files = []
        for data_dir in data_dirs:
            dir_files = sorted(list(data_dir.rglob("*.7z")))
            files.extend(dir_files)
            print(f"Found {len(dir_files)} 7z files in {data_dir} and its subdirectories.")
        
        if not files:
            print(f"No 7z files found in the specified directories.")
            exit(1)
        print(f"Total: Found {len(files)} 7z files across all directories.")
        
        results = []
        with multiprocessing.Pool(processes=args.num_processes) as pool:
            with tqdm(total=len(files), desc="Processing 7z files") as pbar:
                for res in pool.imap_unordered(worker_raw, files):
                    results.append(res)
                    pbar.update(1)
        
        # Collect all segments and errors
        all_segments = []
        all_errors = []
        
        for res in results:
            if res['status'] == 'error':
                print(f"Error processing {res['file']}: {res['msg']}")
                continue
            
            if 'segments' in res:
                all_segments.extend(res['segments'])
            
            if 'errors' in res:
                for err in res['errors']:
                    all_errors.append([res['file'], err[0], err[1], err[2], err[3], err[4]])

        # Global merge of segments
        if all_segments:
            # Sort by start_prevSeqId
            all_segments.sort(key=lambda x: x[0])
            
            final_segments = merge_pairs(all_segments)
            
            if args.show_seg:
                seg_csv_path = Path(args.show_seg)
                with open(seg_csv_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['prevSeqId', 'seqId', 'start_file', 'end_file', 'start_timestamp', 'start_datetime', 'end_timestamp', 'end_datetime'])
                    for seg in final_segments:
                        # seg: (p, s, file_name, file_index, start_ts, end_ts, end_file)
                        if len(seg) == 7:
                            start_ts = seg[4]
                            end_ts = seg[5]
                            end_file = seg[6]
                        else:
                            start_ts = None
                            end_ts = None
                            end_file = seg[4]

                        writer.writerow([
                            seg[0],
                            seg[1],
                            seg[2], # start_file
                            end_file,
                            start_ts,
                            timestamp_to_datetime(start_ts),
                            end_ts,
                            timestamp_to_datetime(end_ts)
                        ])
                print(f"All segments exported to {seg_csv_path}")

            # if there still pairs that not start with -1, that means there are gaps
            gaps = [seg for seg in final_segments if seg[0] != -1]
            if gaps:
                print(f"Found {len(gaps)} gaps in the merged segments.")
                # Dump gaps to csv
                gaps_csv_path = Path(args.output)
                with open(gaps_csv_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['prevSeqId', 'seqId', 'file_name', 'file_index', 'start_timestamp', 'start_datetime', 'end_timestamp', 'end_datetime'])
                    for gap in gaps:
                        if len(gap) == 7:
                            start_ts = gap[4]
                            end_ts = gap[5]
                        else:
                            start_ts = None
                            end_ts = None
                            
                        writer.writerow([
                            gap[0],
                            gap[1],
                            gap[2],
                            gap[3],
                            start_ts,
                            timestamp_to_datetime(start_ts),
                            end_ts,
                            timestamp_to_datetime(end_ts)
                        ])
                print(f"Gaps exported to {gaps_csv_path}")


    else:
        # Collect parquet files from all directories
        pfs = []
        for data_dir in data_dirs:
            dir_pfs = sorted(list(data_dir.rglob("*.parquet")))
            pfs.extend(dir_pfs)
            print(f"Found {len(dir_pfs)} parquet files in {data_dir} and its subdirectories.")
        
        if not pfs:
            print(f"No parquet files found in the specified directories.")
            exit(1)
        print(f"Total: Found {len(pfs)} parquet files across all directories.")

        # Create a list of tasks (file_path, row_group_index, file_list)
        tasks = []
        print("Scanning files to create tasks...")
        for pf_path in tqdm(pfs, desc="Scanning files"):
            try:
                pf = pq.ParquetFile(pf_path)
                for i in range(pf.num_row_groups):
                    tasks.append((pf_path, i, pfs))
            except Exception as e:
                print(f"Warning: Could not read metadata from {pf_path.name}: {e}")

        if not tasks:
            print("No row groups to process. Exiting.")
            exit(0)

        total_errors = 0
        with multiprocessing.Pool(processes=args.num_processes) as pool:
            with tqdm(total=len(tasks), desc="Processing row groups") as pbar:
                # imap_unordered will give results as they are ready
                for error_list in pool.imap_unordered(worker, tasks):
                    if error_list:
                        for error in error_list:
                            if "Warning:" not in error:
                                total_errors += 1
                            print(error)
                    pbar.update(1)

        print("-" * 20)
        if total_errors == 0:
            print("Check finished. No errors found.")
        else:
            print(f"Check finished. Found {total_errors} errors in total.")
        
        exit(0 if total_errors == 0 else 1)

if __name__ == "__main__":
    main()
