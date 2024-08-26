# DataAnalysis

This repo contains the utils for data analysis, for example, generating the parquet format data from the raw data.

## Scripts

- `j2p.py`: convert raw json data to parquet
- `aggregate_parquet.py`: aggregate multiple small parquet files into less but larger parquet
files
- `test_aggregate_parquet.py`: make sure that the aggregated parquet files are correct
- `random_books_generator.py`: generate random books dataset
- `unzip.py`: unzip all zip files in a folder into a folder

> **Process flow**: zipped raw data (7z file) -> `unzip.py` -> raw data (json file) -> `j2p.py` -> parquet files -> `aggregate_parquet.py` -> aggregated parquet files (data set)

Then we should use `indicator` to process the data set and dump the data into InfluxDB.

## InfluxDB schema

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

