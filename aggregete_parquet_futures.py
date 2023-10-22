import aggregate_parquet
from typing import List, Dict
import glob
import os
from multiprocessing.pool import Pool


files = glob.glob(r'e:\temp\parquet\OKX-Books-*.parquet')
grouped_files: Dict[str, List[str]] = {}
for file in files:
    items = file.split('-')
    dsID = '-'.join(items[2:-2])

    if dsID in grouped_files:
        grouped_files[dsID].append(file)
    else:
        grouped_files[dsID] = [file]
        
instIds = list(grouped_files.keys())

futures_instId = list(filter(lambda x: x.split('-')[2].isdigit() and len(x.split('-'))==4, instIds))
grouped_files_futures = {i: grouped_files[i] for i in futures_instId}
root_path = r'E:\out3'
tasks = []

for dsID, files in grouped_files_futures.items():
    out_path = os.path.join(root_path, dsID)
    tasks.append((out_path, files))

with Pool() as p:
    p.map(aggregate_parquet.gen, tasks[12:24])
