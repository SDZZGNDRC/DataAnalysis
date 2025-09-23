# 数据分析工具库

这个仓库包含了数据分析相关的工具，例如从原始数据生成 parquet 格式的数据。

## 脚本说明

- `j2p.py`: 将原始 JSON 数据转换为 parquet 格式
- `aggregate_parquet.py`: 将多个小 parquet 文件聚合成较少但更大的 parquet 文件
- `test_aggregate_parquet.py`: 确保聚合后的 parquet 文件正确无误
- `random_books_generator.py`: 生成随机的订单簿数据集
- `unzip.py`: 将文件夹中的所有 zip 文件解压到指定文件夹

> **处理流程**: 压缩的原始数据 (7z 文件) -> `unzip.py` -> 原始数据 (json 文件) -> `j2p.py` -> parquet 文件 -> `aggregate_parquet.py` -> 聚合后的 parquet 文件 (数据集)  

(Optional) 然后我们应该使用 `indicator` 来处理数据集并将数据导入到 InfluxDB 中。  

## InfluxDB 数据模式

### BLCSI

- **Measurement**: BLCSI
- **Tag key**: exchange, instId, instType, level, side
- **Field key**: val


### ABP

- **Measurement**: ABP
- **Tag key**: exchange, instId, instType, level, side
- **Field key**: val


### AAP

- **Measurement**: AAP
- **Tag key**: exchange, instId, instType, level, side
- **Field key**: val


### TA

- **Measurement**: TA
- **Tag key**: exchange, instId, instType, level, side
- **Field key**: val


### TV

- **Measurement**: TV
- **Tag key**: exchange, instId, instType, level, side
- **Field key**: val

