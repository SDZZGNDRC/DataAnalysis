from pathlib import Path
from typing import List, Tuple, Optional
import pyarrow.parquet as pq

_pfs = []
_key = None
_cache = []

def ensure_cache(pfs: List[Path], key='ts'):
    global _pfs, _key, _cache
    if _cache == [] or _pfs != pfs or _key != key:
        # Start to (re)build the cache.
        _pfs = pfs
        _key = key
        _cache = []
        
        for pf_idx, p in enumerate(pfs):
            pf = pq.ParquetFile(p)
            metadata = pf.metadata
            schema = pf.schema
            col_idx = None
            for i in range(len(schema)):
                if schema.column(i).name == key:
                    col_idx = i
                    break
            if col_idx is None:
                raise(f"ts2rg: column {key} not found in parquet file {pf}")
            
            for rg_idx in range(metadata.num_row_groups):
                row_group = metadata.row_group(rg_idx)
                col = row_group.column(col_idx)
                
                if col.statistics:
                    _cache.append([col.statistics.min,col.statistics.max,pf_idx,rg_idx])
                else:
                    raise Exception(f"ts2rg: no statistics available for column '{key}' in row group {rg_idx} of {pf}")


def ts2rg(pfs: List[Path], ts, key='ts') -> Optional[Tuple[int, int]]:
    """given a list of parquet files and a timestamp, find out the index of the correct row group which `key` column contain the ts.

    Returns:
        Tuple[int, int]: the index of the correct row group, the first element is the index of the parquet file, the second element is the index of the row group in that parquet file.
    """
    ensure_cache(pfs, key)

    # find out the correct row group
    for rg in _cache:
        if rg[0] <= ts <= rg[1]:
            return (rg[2], rg[3])
    else:
        return None

def ts2lastRg(pfs: List[Path], ts, key='ts') -> Optional[Tuple[int, int]]:
    ensure_cache(pfs, key)
    
    for idx in _cache[1:]:
        if _cache[idx][0] <= ts <= _cache[idx][1]:
            return (_cache[idx-1][2], _cache[idx-1][3])
    else:
        return None
    

def asop_ts2rg(pfs: List[Path], min_ts, max_ts, key='ts') -> List[Tuple[int, int]]:
    ensure_cache(pfs, key)
    
    start_rg_cache_idx = -1
    end_rg_cache_idx = -1

    # Find the index of the starting and ending row groups in the _cache
    for i, rg in enumerate(_cache):
        # The first RG we care about is the one that contains min_ts, or the one just before it.
        # To be safe for a 'backward' join, we find the first row group whose max timestamp is >= min_ts,
        # and then we also include the row group immediately preceding it (if one exists).
        if start_rg_cache_idx == -1 and rg[1] >= min_ts:
            start_rg_cache_idx = max(0, i - 1)

        # The last RG is the one that contains max_ts.
        if rg[1] >= max_ts:
            end_rg_cache_idx = i
            break
    
    # If we found a valid range
    if start_rg_cache_idx != -1 and end_rg_cache_idx != -1:
        # Return the slice of row group infos from the cache
        return [(rg[2], rg[3]) for rg in _cache[start_rg_cache_idx : end_rg_cache_idx + 1]]
    
    # Handle cases where the range is not found in the cache
    return []