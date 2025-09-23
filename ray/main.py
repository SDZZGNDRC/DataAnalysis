# 导入所需的库
import ray
import itertools
import pandas as pd
from tqdm.notebook import tqdm
import re

# --- Ray 并行化改造 ---

# 1. 初始化 Ray。ignore_reinit_error=True 在 notebook 中很有用，可以避免重复执行时出错。
# 您可以根据需要配置 num_cpus，默认会使用所有可用的 CPU 核心。
ray.init(ignore_reinit_error=True, num_cpus=8)

# 2. 定义一个远程函数，该函数封装了单次回测的全部逻辑。
# @ray.remote 装饰器使其可以被 Ray 并行调度。
@ray.remote
def run_backtest_task(params, df_ref, fee_rate):
    """
    一个 Ray 任务，用于执行单个参数组合的回测。
    
    :param params: 包含策略参数的字典。
    :param df_ref: 共享在 Ray 对象存储中的数据帧的引用 (ObjectRef)。
    :param fee_rate: 交易费率。
    :return: 包含参数和回测结果的字典。
    """
    # OrderFlowImbalanceStrategy 和 backtest 函数是在之前的单元格中定义的，
    # Ray 会将它们的定义传递给工作进程。
    strategy = OrderFlowImbalanceStrategy(
        volume_threshold=params['volume_threshold'],
        take_profit_pct=params['take_profit_pct'],
        stop_loss_pct=params['stop_loss_pct'],
        fee_rate=fee_rate
    )
    
    # 直接在任务中从引用获取数据
    df = ray.get(df_ref)
    
    # 运行回测
    summary, _ = backtest(df, strategy)
    
    # 收集结果，与原代码逻辑相同
    result_row = params.copy()
    if isinstance(summary, dict):  # 检查是否有交易发生
        result_row.update(summary)
    else:  # 如果没有交易，则填充默认值
        result_row.update({
            "Win Rate": "N/A",
            "Total Net PnL (%)": "0.0%",
            "Average Net PnL per Trade (%)": "0.0%"
        })
    return result_row


# --- 参数设置 (与原代码相同) ---

# 定义要遍历的参数网格
param_grid = {
    'volume_threshold': [i/10000 for i in range(1, 11, 1)],
    'take_profit_pct': [i/100 for i in range(5, 91, 5)],
    'stop_loss_pct': [i/1000 for i in range(10, 301, 5)]
}

# 固定的手续费率
fee_rate = 0.0008

# 生成所有参数组合
keys, values = zip(*param_grid.items())
param_combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]


# --- 执行并行回测 ---

# 3. 将大型数据 df0 放入 Ray 的共享对象存储，返回一个引用。
# 这可以极大地提高效率，避免在每个任务中重复序列化和传输数据。
df0_ref = ray.put(df0)

print(f"开始使用 Ray 进行参数网格搜索，共 {len(param_combinations)} 种组合...")

# 4. 并行启动所有回测任务。
# .remote() 会立即返回一个 ObjectRef (对未来结果的引用)，而不会阻塞等待。
result_futures = [run_backtest_task.remote(params, df0_ref, fee_rate) for params in param_combinations]

# 5. 使用 tqdm 跟踪任务完成进度，并用 ray.get() 获取结果。
# 此循环会按顺序等待每个任务完成并获取其结果。
all_results = [ray.get(future) for future in tqdm(result_futures, desc="Grid Search Progress")]


# --- 资源清理与结果处理 ---

# 6. 完成计算后关闭 Ray
ray.shutdown()

# 7. 将结果转换为DataFrame并进行处理和排序 (与原代码相同)
if all_results:
    results_df = pd.DataFrame(all_results)
    
    # 将字符串格式的PnL转换为浮点数以便排序
    results_df['pnl_numeric'] = results_df['Total Net PnL (%)'].apply(
        lambda x: float(re.findall(r"[-+]?\\d*\\.\\d+|\\d+", str(x))[0]) if re.findall(r"[-+]?\\d*\\.\\d+|\\d+", str(x)) else 0.0
    )
    
    # 按总净收益率降序排序
    sorted_results_df = results_df.sort_values(by='pnl_numeric', ascending=False).drop(columns=['pnl_numeric'])
    
    # 8. 打印最终排序后的结果
    print("\n--- 参数网格搜索结果 (按总净PnL降序排列) ---")
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
        print(sorted_results_df)
else:
    print("网格搜索完成，但没有记录到任何回测结果。")