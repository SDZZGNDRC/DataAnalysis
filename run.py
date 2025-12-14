import numpy as np
import multiprocessing as mp
from multiprocessing import shared_memory
from itertools import product
import json
import os
import subprocess
from tqdm import tqdm
import pickle
import base64
from pathlib import Path

def run_worker_script(cmd):
    """一个简单的函数，用于在子进程中执行命令行指令。"""
    try:
        # 使用 subprocess.run 来执行脚本，隐藏输出以保持主日志干净
        # 如果需要调试，可以移除 stdout 和 stderr
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        # 如果子脚本出错，这里可以捕获到，但我们依赖于 worker 自己记录错误
        # print(f"A worker process failed: {e}")
        pass
    return True

if __name__ == '__main__':
    npz_files = [
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-09.npz",
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-10.npz",
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-11.npz",
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-12.npz",
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-13.npz",
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-14.npz",
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-15.npz",
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-16.npz",
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-17.npz",
        r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-18.npz",
        # r"D:\Project\DataAnalysis\YOUR-OTHER-FILE.npz",
    ]

    base_results_dir = 'backtest_results'
    os.makedirs(base_results_dir, exist_ok=True)

    # --- 2. 定义参数网格 (无变化) ---
    param_grid = {
        'CANDLE_INTERVAL_NS': [1 * 60 * 1_000_000_000],
        'REJECTION_RATIO': [0.5, 0.7, 0.9],
        'MIN_BODY_RATIO': [0.01, 0.03, 0.05],
        'TAKE_PROFIT_FACTOR': [1.005, 1.010, 1.015],
        'STOP_LOSS_FACTOR': [0.990, 0.995, 1.000],
        'elapsed_interval': [100_000_000]
        # 'CANDLE_INTERVAL_NS': [1 * 60 * 1_000_000_000],
        # 'REJECTION_RATIO': [0.5],
        # 'MIN_BODY_RATIO': [0.01],
        # 'TAKE_PROFIT_FACTOR': [1.005],
        # 'STOP_LOSS_FACTOR': [0.990],
        # 'elapsed_interval': [100_000_000]
    }
    param_combinations = list(product(*param_grid.values()))
    param_keys = list(param_grid.keys())
    param_list = [dict(zip(param_keys, combo)) for combo in param_combinations]

    returns_accumulator = {}
    params_cache = {}
    num_processes = mp.cpu_count()

    for file_index, npz_path in enumerate(npz_files):
        print(f"\nLoading data from {npz_path} ...")
        if not os.path.isfile(npz_path):
            print(f"File '{npz_path}' not found. Skipping.")
            continue

        hft_data = np.load(npz_path)['data']
        shm = shared_memory.SharedMemory(create=True, size=hft_data.nbytes)
        shared_array = np.ndarray(hft_data.shape, dtype=hft_data.dtype, buffer=shm.buf)
        np.copyto(shared_array, hft_data)
        del hft_data
        print(f"Data loaded into shared memory block '{shm.name}'.")

        pickled_dtype = pickle.dumps(shared_array.dtype)
        dtype_b64_str = base64.b64encode(pickled_dtype).decode('ascii')
        shape_str = ' '.join(map(str, shared_array.shape))

        results_dir = os.path.join(
            base_results_dir, f'file_{file_index}_{Path(npz_path).stem}'
        )
        os.makedirs(results_dir, exist_ok=True)

        commands = []
        for i, params in enumerate(param_list):
            params_str = json.dumps(params)
            output_file = os.path.join(results_dir, f'result_{i}.json')
            if os.path.exists(output_file):
                os.remove(output_file)
            cmd = [
                'python',
                'worker.py',
                '--params', params_str,
                '--shm_name', shm.name,
                '--shape', shape_str,
                '--dtype_b64', dtype_b64_str,
                '--output_file', output_file
            ]
            commands.append(cmd)

        try:
            print(
                f"Starting {len(commands)} backtests for {npz_path} "
                f"using a pool of {num_processes} workers..."
            )
            with mp.Pool(processes=num_processes) as pool:
                list(tqdm(pool.imap_unordered(run_worker_script, commands), total=len(commands)))
            print("All backtest processes have completed.")

            print("Collecting results from files...")
            all_results = []
            for i in range(len(param_list)):
                result_file = os.path.join(results_dir, f'result_{i}.json')
                if os.path.exists(result_file):
                    try:
                        with open(result_file, 'r') as f:
                            data = json.load(f)
                            if 'summary' in data:
                                all_results.append(data)
                            elif 'error' in data and not globals().get('error_printed', False):
                                print(f"\n--- An error occurred in a worker ---")
                                print(f"Params: {data['params']}")
                                print(f"Error: {data['error']}")
                                if 'traceback' in data:
                                    print(f"Traceback:\n{data['traceback']}")
                                globals()['error_printed'] = True
                    except json.JSONDecodeError as e:
                        print(f"Could not parse result file {result_file}: {e}")

            for result in all_results:
                params = result['params']
                params_key = tuple(params[key] for key in param_keys)
                returns_accumulator.setdefault(params_key, []).append(result['summary']['Return'])
                params_cache[params_key] = params

        finally:
            print("Cleaning up shared memory...")
            shm.close()
            shm.unlink()
            print("Cleanup complete.")

    if returns_accumulator:
        best_key, returns = max(
            returns_accumulator.items(),
            key=lambda item: sum(item[1]) / len(item[1])
        )
        best_params = params_cache[best_key]
        avg_return = sum(returns) / len(returns)
        print("\n跨数据集平均Return最佳参数:", best_params)
        print(f"平均Return: {avg_return}")
    else:
        print("没有有效的回测结果可供分析。")