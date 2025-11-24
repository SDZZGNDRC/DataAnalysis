import numpy as np
import json
import argparse
from multiprocessing import shared_memory
from collections import namedtuple
import pickle
import base64
import pandas as pd # 确保引入 pandas

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest, Recorder
from hftbacktest.stats import LinearAssetRecord
from backtest_rejection_strategy import backtest_rejection_strategy

# NamedTuple 和 run_single_backtest 函数都保持不变
StrategyParams = namedtuple(
    'StrategyParams',
    [
        'CANDLE_INTERVAL_NS',
        'REJECTION_RATIO',
        'MIN_BODY_RATIO',
        'TAKE_PROFIT_FACTOR',
        'STOP_LOSS_FACTOR',
        'elapsed_interval'
    ]
)

def run_single_backtest(params_dict, shm_name, shape, dtype):
    existing_shm = shared_memory.SharedMemory(name=shm_name)
    hft_data = np.ndarray(shape, dtype=dtype, buffer=existing_shm.buf)
    asset = (
        BacktestAsset()
            .data([hft_data])
            .linear_asset(1.0)
            .constant_order_latency(10_000_000, 10_000_000)
            .risk_adverse_queue_model()
            .no_partial_fill_exchange()
            .trading_value_fee_model(0.0008, 0.0010)
            .tick_size(0.1)
            .lot_size(0.001)
            .last_trades_capacity(1_000_000)
    )
    hbt = HashMapMarketDepthBacktest([asset])
    recorder = Recorder(1, 5_000_000)
    params = StrategyParams(**params_dict)
    backtest_rejection_strategy(
        hbt,
        recorder.recorder,
        params
    )
    _ = hbt.close()
    stats = LinearAssetRecord(recorder.get(0)).stats(book_size=10_000)
    summary = stats.summary()
    existing_shm.close()
    return summary # 返回 DataFrame

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--params", type=str, required=True)
    parser.add_argument("--shm_name", type=str, required=True)
    parser.add_argument("--shape", type=int, nargs='+', required=True)
    parser.add_argument("--dtype_b64", type=str, required=True)
    parser.add_argument("--output_file", type=str, required=True)
    args = parser.parse_args()
    
    params_dict = json.loads(args.params)
    shape_tuple = tuple(args.shape)
    
    pickled_dtype = base64.b64decode(args.dtype_b64)
    dtype_obj = pickle.loads(pickled_dtype)
    
    try:
        # 接收返回的 DataFrame
        result_summary_df = run_single_backtest(params_dict, args.shm_name, shape_tuple, dtype_obj)
        
        # --- 这里是确保您拥有正确代码的关键修改 ---
        summary_dict = {}
        if not result_summary_df.is_empty():
            # 将 DataFrame 的第一行转换为字典
            summary_dict = result_summary_df.to_pandas().iloc[0].to_dict()
            print(summary_dict)
            # 转换时间戳为字符串
            for key, value in summary_dict.items():
                if isinstance(value, (pd.Timestamp, np.datetime64)):
                    summary_dict[key] = pd.to_datetime(value).isoformat()
        
        output_data = {
            'params': params_dict,
            'summary': summary_dict # 使用转换后的纯净字典
        }
        
        with open(args.output_file, 'w') as f:
            # 这个 JSON 编码器现在只处理 NumPy 数字类型
            class NpEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, (np.integer, np.int_)):
                        return int(obj)
                    if isinstance(obj, (np.floating, np.float_)):
                        return float(obj)
                    if isinstance(obj, np.ndarray):
                        return obj.tolist()
                    return super(NpEncoder, self).default(obj)
            
            json.dump(output_data, f, cls=NpEncoder, indent=4)

    except Exception as e:
        error_data = {'params': params_dict, 'error': str(e)}
        import traceback
        error_data['traceback'] = traceback.format_exc()
        with open(args.output_file, 'w') as f:
            json.dump(error_data, f, indent=4)