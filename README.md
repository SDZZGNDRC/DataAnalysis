# DataAnalysis

This repo contains the utils for data analysis, for example, generating the parquet format data from the raw data.

## Scripts

- `j2p.py`: convert raw json data to parquet
- `aggregate_parquet.py`: aggregate multiple small parquet files into less but larger parquet
files
- `test_aggregate_parquet.py`: make sure that the aggregated parquet files are correct



## InfluxDB schema

### BLCSI

| Measurement | Tag key | Tag key | Field key |
| :---------: | :------: | :------: | :-------: |
| BLCSI | exchange | instId | value |


