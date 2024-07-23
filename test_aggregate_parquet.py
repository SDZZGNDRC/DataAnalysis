from glob import glob
import pandas as pd
from pathlib import Path
from .aggregate_parquet import gen
# price size numOrders side timestamp, action
# file_1
data_1 = [[2.0, 0.1, 10, 'bid', 1, 'snapshot'],
[2.1, 1.1, 11, 'bid', 1, 'snapshot'],
[2.2, 2.1, 12, 'bid', 1, 'snapshot'],
[2.3, 3.1, 13, 'bid', 1, 'snapshot'],
[2.4, 4.1, 14, 'bid', 1, 'snapshot'],
[2.5, 5.1, 15, 'ask', 1, 'snapshot'],
[2.6, 6.1, 16, 'ask', 1, 'snapshot'],
[2.7, 7.1, 17, 'ask', 1, 'snapshot'],
[2.8, 8.1, 18, 'ask', 1, 'snapshot'],
[2.9, 9.1, 19, 'ask', 1, 'snapshot'],
[2.3, 3.6, 7, 'bid', 2, 'update'],
[2.56, 6.51, 16, 'ask', 2, 'update'],
[2.1, 0, 0, 'bid', 2, 'update']]

# file_2
data_2 = [[2.3, 0, 0, 'bid', 3, 'update'],
[2.4, 9.51, 14, 'bid', 3, 'update'],
[2.5, 1.36, 4, 'ask', 3, 'update'],
[2.6, 3.64, 7, 'ask', 3, 'update'],
[2.7, 6.1, 6, 'ask', 4, 'update'],
[2.8, 1.1, 1, 'ask', 4, 'update'],
[2.9, 0, 0, 'ask', 4, 'update'],
[2.2, 3.3, 7, 'bid', 5, 'update'],
[2.7, 7.6, 2, 'ask', 5, 'update']]

# file_3
data_3 = [[2.0, 0, 0, 'bid', 6, 'update'],
[2.8, 4.9, 2, 'ask', 6, 'update'],
[2.2, 1.1, 1, 'bid', 7, 'snapshot'],
[2.7, 0, 0, 'ask', 7, 'update']]

# correct answer
# file_1
answer_1 = [
[2.0, 0.1, 10, 'bid', 'snapshot', 1],
[2.1, 1.1, 11, 'bid', 'snapshot', 1],
[2.2, 2.1, 12, 'bid', 'snapshot', 1],
[2.3, 3.1, 13, 'bid', 'snapshot', 1],
[2.4, 4.1, 14, 'bid', 'snapshot', 1],
[2.5, 5.1, 15, 'ask', 'snapshot', 1],
[2.6, 6.1, 16, 'ask', 'snapshot', 1],
[2.7, 7.1, 17, 'ask', 'snapshot', 1],
[2.8, 8.1, 18, 'ask', 'snapshot', 1],
[2.9, 9.1, 19, 'ask', 'snapshot', 1],
[2.3, 3.6, 7, 'bid', 'update', 2],
[2.56, 6.51, 16, 'ask', 'update', 2],
[2.1, 0, 0, 'bid', 'update', 2]]
# file_2
answer_2 = [
[2.0, 0.1, 10, 'bid', 'snapshot', 3],
[2.2, 2.1, 12, 'bid', 'snapshot', 3],
[2.4, 9.51, 14, 'bid', 'snapshot', 3],
[2.5, 1.36,  4, 'ask', 'snapshot', 3],
[2.56, 6.51, 16, 'ask', 'snapshot', 3],
[2.6, 3.64, 7, 'ask', 'snapshot', 3],
[2.7, 7.1, 17, 'ask', 'snapshot', 3],
[2.8, 8.1, 18, 'ask', 'snapshot', 3],
[2.9, 9.1, 19, 'ask', 'snapshot', 3],
[2.7, 6.1, 6, 'ask', 'update', 4],
[2.8, 1.1, 1, 'ask', 'update', 4],
[2.9, 0, 0, 'ask', 'update', 4]]
# file_3
answer_3 = [
[2.0, 0.1, 10, 'bid', 'snapshot', 5],
[2.2, 3.3, 7, 'bid', 'snapshot', 5],
[2.4, 9.51, 14, 'bid', 'snapshot', 5],
[2.5, 1.36,  4, 'ask', 'snapshot', 5],
[2.56, 6.51, 16, 'ask', 'snapshot', 5],
[2.6, 3.64, 7, 'ask', 'snapshot', 5],
[2.7, 7.6, 2, 'ask', 'snapshot', 5],
[2.8, 1.1, 1, 'ask', 'snapshot', 5],
[2.0, 0, 0, 'bid', 'update', 6],
[2.8, 4.9, 2, 'ask', 'update', 6]]
# file_4
answer_4 = [
[2.2, 1.1, 1, 'bid', 7, 'snapshot'],
[2.4, 9.51, 14, 'bid', 7, 'snapshot'],
[2.5, 1.36,  4, 'ask', 7, 'snapshot'],
[2.56, 6.51, 16, 'ask', 7, 'snapshot'],
[2.6, 3.64, 7, 'ask', 7, 'snapshot'],
[2.8, 4.9, 2, 'ask', 7, 'snapshot']]

def test_case(tmp_path: Path) -> None:
    gen((str(tmp_path/'TEST'), glob(r'./test/*.parquet')), chunk_size=2)
    
    # Verify
    df_1 = pd.read_parquet(tmp_path/'TEST'/'part-0-1-2.parquet').drop('instId', axis=1)
    df_2 = pd.read_parquet(tmp_path/'TEST'/'part-1-3-4.parquet').drop('instId', axis=1)
    df_3 = pd.read_parquet(tmp_path/'TEST'/'part-2-5-6.parquet').drop('instId', axis=1)
    # df_4 = pd.read_parquet(tmp_path/'TEST'/'part-3-7-7.parquet')

    print(sorted(df_1.to_dict(orient='tight')['data'], key=lambda x: x[0]))
    print('===============')
    print(sorted(answer_1, key=lambda x: x[0]))
    assert sorted(df_1.to_dict(orient='tight')['data'], key=lambda x: x[0]) == sorted(answer_1, key=lambda x: x[0])
    assert sorted(df_2.to_dict(orient='tight')['data'], key=lambda x: x[0]) == sorted(answer_2, key=lambda x: x[0])
    assert sorted(df_3.to_dict(orient='tight')['data'], key=lambda x: x[0]) == sorted(answer_3, key=lambda x: x[0])