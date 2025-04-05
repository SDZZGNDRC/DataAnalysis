from json2parquet import convert_json

# Infer Schema (requires reading dataset for column names)
convert_json(
    r'D:\Project\Crypto-DataLayer\out3\OrderBooks\2025-04-05\OKX-Books-BIO-USDT-SWAP-400--1743782921607607-1743784981666515.json',
    r'D:\result.parquet'
)
