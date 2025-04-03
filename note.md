## aggregate_books

### 1. 不同原始json文件见的data.ts可能会overlapped

根据`aggregate_books.py`的报错：
```psh
Error: Timestamp range overlap detected between files tmp\OKX-Books-1INCH-EUR-400-1740592268524-1740594337309.json and tmp\OKX-Books-1INCH-EUR-400-1740594341600-1740595478164.json
File tmp\OKX-Books-1INCH-EUR-400-1740592268524-1740594337309.json has max_ts=1740594337006, File tmp\OKX-Books-1INCH-EUR-400-1740594341600-1740595478164.json has min_ts=1740594336806
PS D:\Project\DataAnalysis> 
```
说明两个原始json文件间的ts可能会overlapped。

### 2. 同样是根据`aggregate_books.py`, 同一个原始json文件间的ts不一定按照升序排序.

```python
        # Check if timestamps are sorted in ascending order
        if ts_values != sorted(ts_values):
            print(f"Warning: Timestamps not in ascending order in file {paths[i]}")
```

``psh
PS D:\Project\DataAnalysis> python process_books.py E:\datapool\2025-02-27\ D:\tmp.json OKX-Books-1INCH-EUR-400 .\tmp
Unzipping 27 files...
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████| 27/27 [00:00<00:00, 60.90it/s]

Processing 27 JSON files...
Validating paths: 100%|█████████████████████████████████████████████████████████████████████████████████| 27/27 [00:00<00:00, 12545.28it/s]
Validating data names: 100%|███████████████████████████████████████████████████████████████████████████| 27/27 [00:00<00:00, 212469.43it/s]
Reading files: 100%|███████████████████████████████████████████████████████████████████████████████████████| 27/27 [00:00<00:00, 35.04it/s]
Processing files:   0%|                                                                                             | 0/27 [00:00<?, ?it/s]Warning: Timestamps not in ascending order in file tmp\OKX-Books-1INCH-EUR-400-1740583748649-1740586559885.json
Processing files:   4%|███▏                                                                                 | 1/27 [00:00<00:03,  8.31it/s]Warning: Timestamps not in ascending order in file tmp\OKX-Books-1INCH-EUR-400-1740586563882-1740589398472.json
                                                                                                                                           Warning: Timestamps not in ascending order in file tmp\OKX-Books-1INCH-EUR-400-1740589405460-1740592259423.json
```



