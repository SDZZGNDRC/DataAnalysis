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
        
    pfs = sorted(list(data_dir.glob("*.parquet")))
    if not pfs:
        print(f"No parquet files found in {data_dir}.")
        exit(1)
    else:
        print(f"Found {len(pfs)} parquet files in {data_dir}.")

    # Process each parquet file individually to avoid high memory usage
    for pf_path in tqdm(pfs, desc="Processing files"):
        try:
            pf = pq.ParquetFile(pf_path)
            if pf.num_row_groups == 0:
                print(f"Warning: {pf_path.name} has no row groups, skipping.")
                continue
        except Exception as e:
            print(f"Warning: Could not read metadata from {pf_path.name}: {e}, skipping.")
            continue

        # Create a list of tasks for the current file
        tasks = [(pf_path, i, args.N) for i in range(pf.num_row_groups)]
        
        if not tasks:
            print(f"No row groups to process in {pf_path.name}. Skipping.")
            continue

        # Process tasks in parallel for the current file
        file_results = []
        with multiprocessing.Pool(processes=args.num_processes) as pool:
            desc = f"Processing {pf_path.name}"
            with tqdm(total=len(tasks), desc=desc, leave=False) as pbar:
                for result in pool.imap_unordered(worker, tasks):
                    if result:
                        file_results.extend(result)
                    pbar.update(1)

        if not file_results:
            print(f"No data was generated for {pf_path.name}. Skipping.")
            continue
            
        # Sort by timestamp
        file_results.sort(key=lambda x: x['ts'])
        
        # ensure the number of row is equal
        expected_rows = 0
        df_first_rg = pf.read_row_group(0).to_pandas()
        if not df_first_rg.empty and df_first_rg.iloc[0]['action'] == 'snapshot':
            expected_rows = pf.metadata.num_rows
        else:
            for i in range(1, pf.num_row_groups):
                expected_rows += pf.metadata.row_group(i).num_rows
        
        if expected_rows != len(file_results):
            print(f'Warning: For {pf_path.name}, the number of generated rows ({len(file_results)}) is not equal to the expected number of rows ({expected_rows}). Skipping file.')
            continue

        # Convert to pyarrow table
        try:
            bl_table = pa.Table.from_pylist(file_results)
        except Exception as e:
            print(f"Failed to create pyarrow table for {pf_path.name}: {e}")
            continue
        
        # Dump to parquet
        start_ts = file_results[0]['ts']
        end_ts = file_results[-1]['ts']
        # FIXME: 在名称中添加`instId`字段
        prefix = f"OKX-BL{args.N}-{start_ts}-{end_ts}.parquet"
        output_file = output_dir / prefix
        pq.write_table(
            bl_table,
            output_file,
            row_group_size=args.row_group_size,
            compression='ZSTD',
            compression_level=2
        )
        print(f"Successfully generated {len(file_results)} rows for {pf_path.name}.")
        print(f"NthBL data saved to {output_file}")

    print("All files processed.")

