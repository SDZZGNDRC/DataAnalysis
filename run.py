import numpy as np
import multiprocessing as mp
from multiprocessing import shared_memory
from itertools import product
import json
import os
import subprocess
from tqdm import tqdm
import pickle  # <-- 1. 引入 pickle
import base64  # <-- 1. 引入 base64

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
    # --- 1. 加载数据并创建共享内存 (无变化) ---
    print("Loading data in the main process...")
    hft_data = np.load(r"D:\Project\DataAnalysis\OKX-HFT-BTC-USDT-2025-03-14.npz")['data']
    shm = shared_memory.SharedMemory(create=True, size=hft_data.nbytes)
    shared_array = np.ndarray(hft_data.shape, dtype=hft_data.dtype, buffer=shm.buf)
    np.copyto(shared_array, hft_data)
    del hft_data
    print(f"Data loaded into shared memory block '{shm.name}'.")

    # --- 2. 定义参数网格 (无变化) ---
    param_grid = {
        'CANDLE_INTERVAL_NS': [1 * 60 * 1_000_000_000],
        'REJECTION_RATIO': [0.5, 0.7, 0.9],
        'MIN_BODY_RATIO': [0.01, 0.03, 0.05],
        'TAKE_PROFIT_FACTOR': [1.005, 1.010, 1.015],
        'STOP_LOSS_FACTOR': [0.990, 0.995, 1.000],
        'elapsed_interval': [100_000_000]
    }
    # param_grid = {
    #     'CANDLE_INTERVAL_NS': [1 * 60 * 1_000_000_000],
    #     'REJECTION_RATIO': [0.5],
    #     'MIN_BODY_RATIO': [0.01],
    #     'TAKE_PROFIT_FACTOR': [1.005],
    #     'STOP_LOSS_FACTOR': [0.990],
    #     'elapsed_interval': [100_000_000]
    # }
    param_combinations = list(product(*param_grid.values()))
    param_keys = list(param_grid.keys())
    param_list = [dict(zip(param_keys, combo)) for combo in param_combinations]

    results_dir = 'backtest_results'
    os.makedirs(results_dir, exist_ok=True)
    
    # --- 3. 为每个参数组合生成命令行指令 (核心修改) ---
    commands = []
    
    # <-- 2. 对 dtype 对象进行序列化和编码
    # 先用 pickle 序列化为字节串
    pickled_dtype = pickle.dumps(shared_array.dtype)
    # 再用 base64 编码为 ASCII 字符串，以便安全地在命令行传递
    dtype_b64_str = base64.b64encode(pickled_dtype).decode('ascii')
    
    for i, params in enumerate(param_list):
        params_str = json.dumps(params)
        output_file = os.path.join(results_dir, f'result_{i}.json')
        shape_str = ' '.join(map(str, shared_array.shape))
        
        cmd = [
            'python',
            'worker.py',
            '--params', params_str,
            '--shm_name', shm.name,
            '--shape', shape_str,
            # <-- 3. 传递编码后的 dtype 字符串
            '--dtype_b64', dtype_b64_str,
            '--output_file', output_file
        ]
        commands.append(cmd)

    # --- 4. 并行执行脚本 (无变化) ---
    num_processes = mp.cpu_count()
    try:
        print(f"Starting {len(commands)} backtests as separate processes using a pool of {num_processes} workers...")
        with mp.Pool(processes=num_processes) as pool:
            list(tqdm(pool.imap_unordered(run_worker_script, commands), total=len(commands)))
        print("All backtest processes have completed.")

        # --- 5. 收集和分析结果 (无变化) ---
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
                        elif 'error' in data:
                            # 打印第一个遇到的错误，以帮助调试
                            if not any('error_printed' in globals() for _ in [1]):
                                print(f"\n--- An error occurred in a worker ---")
                                print(f"Params: {data['params']}")
                                print(f"Error: {data['error']}")
                                if 'traceback' in data:
                                    print(f"Traceback:\n{data['traceback']}")
                                globals()['error_printed'] = True
                except (json.JSONDecodeError) as e:
                    print(f"Could not parse result file {result_file}: {e}")

        if all_results:
            best_run = max(all_results, key=lambda item: item['summary']['Return'])
            print("\n最佳参数:", best_run['params'])
            print("最佳结果:", best_run['summary'])
        else:
            print("没有有效的回测结果可供分析。")

    finally:
        # --- 清理 (无变化) ---
        print("Cleaning up shared memory...")
        shm.close()
        shm.unlink()
        print("Cleanup complete.")