import argparse
import json
from pathlib import Path
import pyarrow.parquet as pq
import multiprocessing
import os
from typing import List
from tqdm import tqdm

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

def main():
    parser = argparse.ArgumentParser(description="Check sequence IDs in parquet files using multiprocessing.")
    parser.add_argument("--dir", type=str, required=True, help="Directory containing the parquet files.")
    parser.add_argument("--num-processes", type=int, default=os.cpu_count(),
                        help=f"Number of processes to use. Defaults to the number of CPU cores ({os.cpu_count()}).")
    args = parser.parse_args()

    data_dir = Path(args.dir)
    if not data_dir.is_dir():
        print(f"Error: {data_dir} is not a valid directory.")
        exit(1)

    pfs = sorted(list(data_dir.glob("*.parquet")))
    if not pfs:
        print(f"No parquet files found in {data_dir}.")
        exit(1)
    else:
        print(f"Found {len(pfs)} parquet files in {data_dir}.")

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
