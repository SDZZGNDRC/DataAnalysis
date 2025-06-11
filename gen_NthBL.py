import argparse
import numpy as np
from typing import Dict, List
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm
import multiprocessing
import os

# from pybacktest.bookcore import BookCore
from cbookcore import BookCore

def gen_nth_bl_for_group(pf_path: Path, group_index: int, N: int) -> List[Dict]:
    """
    Process a single row group from a parquet file to generate Nth book levels.
    """
    try:
        pf = pq.ParquetFile(pf_path)
        df = pf.read_row_group(group_index).to_pandas()
    except Exception as e:
        print(f"Warning: Failed to read row group {group_index} from {pf_path.name}: {e}")
        return []

    if df.empty:
        return []
    
    # The first row of a processing unit must be a snapshot
    if df.iloc[0]['action'] != 'snapshot':
        print(f"Warning: {pf_path.name}'s group {group_index} does not start with a snapshot, skipping...")
        return []
        
    instId = df.iloc[0]['arg']['instId']
    bc = BookCore(instId)
    results = []
    
    for row in df.itertuples():
        bc.set_datapoint(row._asdict())
        
        record = {'ts': row.ts}
        
        asks = bc.asks[:N]
        bids = bc.bids[:N]
        
        for i in range(N):
            # Process asks
            if i < len(asks):
                record[f'asks_{i}_price'] = asks[i].price
                record[f'asks_{i}_amount'] = asks[i].amount
                record[f'asks_{i}_count'] = asks[i].count
            else:
                record[f'asks_{i}_price'] = None
                record[f'asks_{i}_amount'] = None
                record[f'asks_{i}_count'] = None
            
            # Process bids
            if i < len(bids):
                record[f'bids_{i}_price'] = bids[i].price
                record[f'bids_{i}_amount'] = bids[i].amount
                record[f'bids_{i}_count'] = bids[i].count
            else:
                record[f'bids_{i}_price'] = None
                record[f'bids_{i}_amount'] = None
                record[f'bids_{i}_count'] = None
        
        results.append(record)
        
    return results

# Wrapper function for multiprocessing that unpacks arguments
def worker(args):
    return gen_nth_bl_for_group(*args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Nth BookLevel features from parquet files.")
    parser.add_argument("--dir", type=str, required=True, help="Directory containing the source parquet files.")
    parser.add_argument("--output", type=str, required=True, help="Output directory for the generated parquet file.")
    parser.add_argument("--N", type=int, required=True, help="Number of book levels to generate for asks and bids.")
    parser.add_argument("--num-processes", type=int, default=os.cpu_count(),
                        help=f"Number of processes to use. Defaults to the number of CPU cores ({os.cpu_count()}).")
    parser.add_argument("--row-group-size", type=int, default=100_000, help="Row group size for the output parquet file.")
    
    args = parser.parse_args()
    data_dir = Path(args.dir)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not data_dir.is_dir():
        print(f"Error: {data_dir} is not a valid directory.")
        exit(1)
        
    pfs = sorted(list(data_dir.glob("*.parquet")))[:3]
    if not pfs:
        print(f"No parquet files found in {data_dir}.")
        exit(1)
    else:
        print(f"Found {len(pfs)} parquet files in {data_dir}.")

    # Create a list of tasks (file_path, row_group_index, N)
    tasks = []
    print("Scanning files to create tasks...")
    for pf_path in tqdm(pfs, desc="Scanning files"):
        try:
            pf = pq.ParquetFile(pf_path)
            for i in range(pf.num_row_groups):
                tasks.append((pf_path, i, args.N))
        except Exception as e:
            print(f"Warning: Could not read metadata from {pf_path.name}: {e}")
    
    if not tasks:
        print("No row groups to process. Exiting.")
        exit(0)

    # Process tasks in parallel
    all_results = []
    with multiprocessing.Pool(processes=args.num_processes) as pool:
        with tqdm(total=len(tasks), desc="Processing row groups") as pbar:
            for result in pool.imap_unordered(worker, tasks):
                if result:
                    all_results.extend(result)
                pbar.update(1)

    if not all_results:
        print("No data was generated. Exiting.")
        exit(0)
        
    # Sort by timestamp
    all_results.sort(key=lambda x: x['ts'])
    
    # ensure the number of row is equal
    total_row = 0
    for i, p in enumerate(pfs):
        pf = pq.ParquetFile(p)
        # 第一个parquet文件的第一个`row_group`不计入在内。
        if i == 0:
            df = pf.read_row_group(0).to_pandas()
            if df.iloc[0].action == 'snapshot':
                total_row += pf.metadata.num_rows
            else:
                for i in range(1, pf.num_row_groups):
                    total_row += pf.metadata.row_group(i).num_rows
        else:
            total_row += pf.metadata.num_rows
    if total_row != len(all_results):
        raise Exception(f'the number({len(all_results)}) of generated `blcsi` is not equal to origin parquet files ({total_row})')
    
    # Convert to pyarrow table
    bl_table = pa.Table.from_pylist(all_results)
    
    # Dump to parquet
    start_ts = all_results[0]['ts']
    end_ts = all_results[-1]['ts']
    prefix = f"OKX-BL{args.N}-{start_ts}-{end_ts}.parquet"
    output_file = output_dir / prefix
    pq.write_table(
        bl_table,
        output_file,
        row_group_size=args.row_group_size,
        compression='ZSTD',
        compression_level=2
    )
    print(f"Successfully generated {len(all_results)} rows.")
    print(f"NthBL data saved to {output_file}")


