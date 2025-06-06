import numpy as np
import pandas as pd
import yfinance as yf
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
import matplotlib.pyplot as plt

# 获取BTC和ETH历史价格数据
def fetch_data(start_date='2025-01-01', end_date='2025-04-30'):
    import time
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            btc = yf.download('BTC-USD', start=start_date, end=end_date, interval='1d')['Close']
            eth = yf.download('ETH-USD', start=start_date, end=end_date, interval='1d')['Close']
            data = pd.concat([btc, eth], axis=1).dropna()
            data.columns = ['BTC', 'ETH']
            return data
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to fetch data after {max_retries} attempts. Please try again later.")

# 协整检验
def coint_test(series1, series2):
    score, p_value, _ = coint(series1, series2)
    return p_value

# 计算价差的z-score
def calculate_zscore(spread, window=30):
    mean = spread.rolling(window=window).mean()
    std = spread.rolling(window=window).std()
    zscore = (spread - mean) / std
    return zscore

# 统计套利策略
def stat_arb_strategy(data, window=30, threshold=2.0):
    btc, eth = data['BTC'], data['ETH']
    
    # 协整检验
    p_value = coint_test(btc, eth)
    print(f"Co-integration p-value: {p_value:.4f}")
    if p_value > 0.05:
        print("Warning: BTC and ETH may not be co-integrated.")
    
    # 计算对数价格比（或其他价差形式）
    spread = np.log(btc) - np.log(eth)
    
    # 计算z-score
    zscore = calculate_zscore(spread, window)
    
    # 生成交易信号
    signals = pd.DataFrame(index=data.index)
    signals['zscore'] = zscore
    signals['BTC'] = 0
    signals['ETH'] = 0
    
    # 交易逻辑：z-score > threshold 做空BTC，买入ETH；z-score < -threshold 买入BTC，做空ETH
    signals.loc[zscore > threshold, 'BTC'] = -1  # 做空BTC
    signals.loc[zscore > threshold, 'ETH'] = 1   # 买入ETH
    signals.loc[zscore < -threshold, 'BTC'] = 1  # 买入BTC
    signals.loc[zscore < -threshold, 'ETH'] = -1 # 做空ETH
    
    return signals, spread

# 可视化
def plot_results(data, signals, spread):
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(12, 16), sharex=True)
    
    # 价格变化率图
    ax1.plot(data['BTC'].pct_change(), label='BTC Price Change', color='blue')
    ax1.plot(data['ETH'].pct_change(), label='ETH Price Change', color='orange')
    ax1.set_title('BTC and ETH Daily Price Changes')
    ax1.legend()
    
    # z-score和交易信号
    ax2.plot(signals['zscore'], label='Z-Score', color='purple')
    ax2.axhline(2.0, color='red', linestyle='--')
    ax2.axhline(-2.0, color='red', linestyle='--')
    ax2.axhline(0, color='black', linestyle='-')
    ax2.plot(signals.index[signals['BTC'] == 1], signals['zscore'][signals['BTC'] == 1], '^', markersize=10, color='green', label='Buy BTC')
    ax2.plot(signals.index[signals['BTC'] == -1], signals['zscore'][signals['BTC'] == -1], 'v', markersize=10, color='red', label='Sell BTC')
    ax2.set_title('Z-Score and Trading Signals')
    ax2.legend()
    
    # ETH/BTC价格比
    price_ratio = (data['ETH'] / data['BTC']) * 100
    ax3.plot(price_ratio, label='ETH/BTC Price Ratio (%)', color='green')
    ax3.set_title('ETH/BTC Price Ratio')
    ax3.legend()
    
    # 策略收益率
    returns = (signals['BTC'].shift(1) * data['BTC'].pct_change() +
              signals['ETH'].shift(1) * data['ETH'].pct_change())
    cumulative_returns = (1 + returns).cumprod() - 1
    ax4.plot(cumulative_returns, label='Cumulative Returns', color='blue')
    ax4.set_title('Strategy Cumulative Returns')
    ax4.legend()
    
    plt.tight_layout()
    plt.savefig('stat_arb_btc_eth.png')

# 主函数
def main():
    # 获取数据
    data = fetch_data()
    
    # 执行策略
    signals, spread = stat_arb_strategy(data)
    
    # 可视化结果
    plot_results(data, signals, spread)
    print("Trading signals and plot saved.")

if __name__ == "__main__":
    main()